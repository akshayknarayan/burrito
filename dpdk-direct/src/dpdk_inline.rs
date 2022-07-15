use ahash::AHashMap as HashMap;
use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{ensure, eyre, Report, WrapErr};
use dpdk_wrapper::{
    bindings::*,
    mbuf_slice,
    utils::{parse_cfg, AddressInfo, HeaderInfo, TOTAL_HEADER_SIZE},
    wrapper::*,
};
use eui48::MacAddress;
use flume::{Receiver, Sender};
use futures_util::future::ready;
use futures_util::Stream;
use futures_util::{future::Ready, stream::Once};
use std::cell::RefCell;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tracing::{debug, error, trace, warn};

std::thread_local! {
    static DPDK_STATE: RefCell<Option<DpdkState>> = RefCell::new(None);
}

/// Chunnel implementation for DPDK inline.
///
/// The strategy here is to store the DPDK mempool state in a `thread_local`. On chunnel init, we
/// initialize the requested number of DPDK mempools and store them in this struct. When we make a
/// connection, we pop a mempool and affinitize that thread (because using mempools cross-thread is
/// unsafe).
///
/// Later, if tokio decides to move the task corresponding to this connection to another
/// thread, during the actual send/receive it will query `thread_local` to get the local DPDK
/// mempool to issue sends/receives against. If there is not one, it will try to initialize that
/// thread. If there are no mempools left, this will fail, but in theory we should not have more
/// threads than mempools.
pub struct DpdkInlineChunnel {
    // for initialization only. once we start sending/receiving on some threads, we will take these
    // out of this Vec and move them into
    initialized_mempools: Arc<Mutex<Vec<DpdkState>>>,
    // for connect-side connections, ephemeral source ports.
    ephemeral_ports: Arc<Mutex<Vec<u16>>>,
}

impl DpdkInlineChunnel {
    /// Initialize DPDK mempools for use in connections.
    ///
    /// Should make sure `num_dpdk_threads` is >= the number of tokio workers (threads).
    /// Otherwise we might run out of mempools.
    pub fn new(config_path: PathBuf, num_dpdk_threads: usize) -> Result<Self, Report> {
        let dpdks = DpdkState::new(config_path, num_dpdk_threads)?;
        Ok(DpdkInlineChunnel {
            initialized_mempools: Arc::new(Mutex::new(dpdks)),
            ephemeral_ports: Arc::new(Mutex::new((1024..60_000).collect())),
        })
    }
}

fn try_init_thread(init_pool: &Mutex<Vec<DpdkState>>) -> Result<(), Report> {
    DPDK_STATE
        .try_with(|dpdk| {
            let mut dpdk = dpdk.borrow_mut();
            if dpdk.is_none() {
                *dpdk = Some(
                    init_pool
                        .lock()
                        .unwrap()
                        .pop()
                        .ok_or(eyre!("No remaining initialized dpdk thread mempools"))?,
                );
            }
            Ok::<_, Report>(())
        })
        .wrap_err(eyre!("Error accessing dpdk state thread_local"))??;

    // affinitize
    affinitize_thread(get_lcore_id() as _).wrap_err(eyre!("affinitizing thread"))?;
    Ok(())
}

impl ChunnelListener for DpdkInlineChunnel {
    type Addr = SocketAddr;
    type Connection = DpdkInlineCn;
    type Future = Ready<Result<Self::Stream, Report>>;
    type Stream = Once<Ready<Result<Self::Connection, Self::Error>>>;
    type Error = Report;

    fn listen(&mut self, addr: Self::Addr) -> Self::Future {
        use SocketAddr::*;
        ready(match addr {
            V4(a) => Ok(futures_util::stream::once(ready(
                try_init_thread(self.initialized_mempools.as_ref()).and_then(|_| {
                    let mut ports = self.ephemeral_ports.lock().unwrap();
                    let mut found = false;
                    for i in 0..ports.len() {
                        if ports[i] == a.port() {
                            ports.remove(i);
                            found = true;
                            break;
                        }
                    }

                    ensure!(
                        found,
                        "Tried to listen on port that is already in use: {:?}",
                        a.port()
                    );

                    DPDK_STATE.with(|dpdk_cell| {
                        let this_lcore = get_lcore_id();
                        let mut dpdk_opt = dpdk_cell.borrow_mut();
                        let dpdk = dpdk_opt
                            .as_mut()
                            .ok_or(eyre!("dpdk not initialized on core {:?}", this_lcore))?;

                        dpdk.rx_packets_for_ports.push((
                            (
                                a.port(),
                                SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, 0),
                            ),
                            Vec::with_capacity(16),
                        ));

                        Ok::<_, Report>(())
                    })?;

                    Ok(DpdkInlineCn::new(
                        a.port(),
                        None,
                        None,
                        Arc::clone(&self.initialized_mempools),
                        Some(Arc::clone(&self.ephemeral_ports)),
                    ))
                }),
            )) as _),
            V6(a) => Err(eyre!("Only IPv4 is supported: {:?}", a)),
        })
    }
}

impl ChunnelConnector for DpdkInlineChunnel {
    type Addr = ();
    type Connection = DpdkInlineCn;
    type Future = futures_util::future::Ready<Result<Self::Connection, Report>>;
    type Error = Report;

    fn connect(&mut self, _a: Self::Addr) -> Self::Future {
        ready(
            try_init_thread(self.initialized_mempools.as_ref()).and_then(|_| {
                let port = self
                    .ephemeral_ports
                    .lock()
                    .unwrap()
                    .pop()
                    .ok_or(eyre!("No available ports"))?;
                DPDK_STATE.with(|dpdk_cell| {
                    let this_lcore = get_lcore_id();
                    let mut dpdk_opt = dpdk_cell.borrow_mut();
                    let dpdk = dpdk_opt
                        .as_mut()
                        .ok_or(eyre!("dpdk not initialized on core {:?}", this_lcore))?;

                    dpdk.rx_packets_for_ports.push((
                        (port, SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, 0)),
                        Vec::with_capacity(16),
                    ));

                    Ok::<_, Report>(())
                })?;

                Ok(DpdkInlineCn::new(
                    port,
                    None,
                    None,
                    Arc::clone(&self.initialized_mempools),
                    Some(Arc::clone(&self.ephemeral_ports)),
                ))
            }),
        )
    }
}

/// Is this connection type Send? If filter_port is on, and NIC RSS is on, then ports will only
/// arrive on one thread, so moving to any other thread will always be a bad idea because packets
/// will just pile up on the other thread's thread_local. This is still "safe", just really bad for
/// performance.
pub struct DpdkInlineCn {
    local_port: u16,
    remote_addr: Option<SocketAddrV4>,
    new_conns: Option<Sender<SocketAddrV4>>,
    port_pool: Option<Arc<Mutex<Vec<u16>>>>,
    _mempools: Arc<Mutex<Vec<DpdkState>>>,
}

impl DpdkInlineCn {
    fn new(
        local_port: u16,
        remote_addr: Option<SocketAddrV4>,
        new_conns: Option<Sender<SocketAddrV4>>,
        mempools: Arc<Mutex<Vec<DpdkState>>>,
        port_pool: Option<Arc<Mutex<Vec<u16>>>>,
    ) -> Self {
        Self {
            local_port,
            remote_addr,
            new_conns,
            port_pool,
            _mempools: mempools,
        }
    }
}

impl std::fmt::Debug for DpdkInlineCn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DpdkInlineCn")
            .field("local_port", &self.local_port)
            .field("remote_addr", &self.remote_addr)
            .finish()
    }
}

impl Drop for DpdkInlineCn {
    fn drop(&mut self) {
        // drop any stashed packets for this port.
        if let Err(err) = DPDK_STATE.try_with(|dpdk_cell| {
            let mut dpdk_opt = dpdk_cell.borrow_mut();
            if let Some(dpdk) = dpdk_opt.as_mut() {
                for i in 0..dpdk.rx_packets_for_ports.len() {
                    let ((local_port_slot, remote_addr_slot), ref mut stash) =
                        &mut dpdk.rx_packets_for_ports[i];
                    if let Some(remote_addr) = self.remote_addr {
                        if *remote_addr_slot == remote_addr && *local_port_slot == self.local_port {
                            // remove this stash from the list of stashes, so that a new matching
                            // packet triggers new connection logic.
                            dpdk.rx_packets_for_ports.swap_remove(i);
                        }
                    } else {
                        if *local_port_slot == self.local_port {
                            stash.clear();
                            break;
                        }
                    }
                }
            }
        }) {
            warn!(?err, "DPDK_STATE thread local access error");
        }

        // put our claimed port back
        if let Some(pool) = &self.port_pool {
            pool.lock().unwrap().push(self.local_port);
        }
    }
}

impl ChunnelConnection for DpdkInlineCn {
    type Data = (SocketAddr, Vec<u8>);

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        let this_lcore = get_lcore_id();
        Box::pin(ready({
            DPDK_STATE
                .try_with(|dpdk_cell| {
                    let mut dpdk_opt = dpdk_cell.borrow_mut();
                    let dpdk = dpdk_opt
                        .as_mut()
                        .ok_or(eyre!("dpdk not initialized on core {:?}", this_lcore))?;

                    dpdk.send_burst(burst.into_iter().map(|(to_addr, buf)| {
                        use SocketAddr::*;
                        let to_addr = match to_addr {
                            V4(a) => a,
                            V6(addr) => {
                                error!(?addr, "Only IPv4 is supported");
                                panic!("Only IPv4 is supported: {:?}", addr);
                            }
                        };
                        SendMsg {
                            src_port: self.local_port,
                            to_addr,
                            buf_ptr: buf.as_ptr(),
                            buf_len: buf.len(),
                        }
                    }))?;
                    Ok(())
                })
                .map_err(Into::into)
                .and_then(|x| x)
        }))
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        let this_lcore = get_lcore_id();
        Box::pin(async move {
            loop {
                let ret = DPDK_STATE
                    .try_with(|dpdk_cell| {
                        let mut dpdk_opt = dpdk_cell.borrow_mut();
                        let dpdk = dpdk_opt
                            .as_mut()
                            .ok_or(eyre!("dpdk not initialized on core {:?}", this_lcore))?;
                        let msgs = dpdk.try_recv_burst(
                            Some((self.local_port, self.remote_addr)),
                            self.new_conns.as_ref(),
                        )?;

                        let mut slot_idx = 0;
                        for msg in msgs.iter_mut().map_while(Option::take) {
                            ensure!(msg.port == self.local_port, "Port mismatch");
                            if let Some((ref mut addr, ref mut payload)) = msgs_buf[slot_idx] {
                                *addr = SocketAddr::V4(msg.addr);
                                payload.copy_from_slice(msg.get_buf());
                            } else {
                                msgs_buf[slot_idx] =
                                    Some((SocketAddr::V4(msg.addr), msg.get_buf().to_vec()));
                            }

                            slot_idx += 1;
                        }

                        Ok(slot_idx)
                    })
                    .map_err(Into::into)
                    .and_then(|x| x)?;
                if ret > 0 {
                    return Ok(&mut msgs_buf[..ret]);
                } else {
                    tokio::task::yield_now().await;
                }
            }
        })
    }
}

/// Implement separating
pub struct DpdkInlineReqChunnel(DpdkInlineChunnel);

impl From<DpdkInlineChunnel> for DpdkInlineReqChunnel {
    fn from(i: DpdkInlineChunnel) -> Self {
        Self(i)
    }
}

impl ChunnelListener for DpdkInlineReqChunnel {
    type Addr = SocketAddr;
    type Connection = DpdkInlineCn;
    type Future = futures_util::future::Ready<Result<Self::Stream, Report>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn listen(&mut self, addr: Self::Addr) -> Self::Future {
        use SocketAddr::*;
        ready((|| {
            match addr {
                V4(a) => {
                    try_init_thread(self.0.initialized_mempools.as_ref())?;

                    let (s, r) = flume::bounded(16);

                    // we have to first listen for a packet ourselves, since at the beginning there are no
                    // connections calling recv.

                    struct StreamState {
                        got_first: bool,
                        listen_addr: SocketAddrV4,
                        sender: Sender<SocketAddrV4>,
                        receiver: Receiver<SocketAddrV4>,
                        initialized_mempools: Arc<Mutex<Vec<DpdkState>>>,
                    }

                    let state = StreamState {
                        got_first: false,
                        listen_addr: a,
                        sender: s,
                        receiver: r,
                        initialized_mempools: Arc::clone(&self.0.initialized_mempools),
                    };

                    // we just initialized this, so `.with` is fine instead of `.try_with`.
                    Ok(Box::pin(futures_util::stream::try_unfold(
                        state,
                        |mut state| async move {
                            if !state.got_first {
                                debug!("DpdkInlineReqChunnel listen stream poll for first packet");
                                loop {
                                    DPDK_STATE.with(|dpdk_cell| {
                                        let this_lcore = get_lcore_id();
                                        let mut dpdk_opt = dpdk_cell.borrow_mut();
                                        let dpdk = dpdk_opt.as_mut().ok_or(eyre!(
                                            "dpdk not initialized on core {:?}",
                                            this_lcore
                                        ))?;

                                        dpdk.try_recv_burst_stash_only(Some(&state.sender))?;
                                        Ok::<_, Report>(())
                                    })?;

                                    match state.receiver.try_recv() {
                                        Ok(addr) => {
                                            debug!(
                                                ?addr,
                                                "DpdkInlineReqChunnel found first connection"
                                            );
                                            let cn = DpdkInlineCn::new(
                                                state.listen_addr.port(),
                                                Some(addr),
                                                Some(state.sender.clone()),
                                                Arc::clone(&state.initialized_mempools),
                                                None,
                                            );

                                            state.got_first = true;
                                            return Ok(Some((cn, state)));
                                        }
                                        Err(flume::TryRecvError::Empty) => (),
                                        Err(flume::TryRecvError::Disconnected) => {
                                            error!(addr = ?state.listen_addr, "New connection recevier closed without any connections");
                                            panic!("New connection recevier closed without any connections");
                                        }
                                    }
                                }
                            } else {
                                let addr = state.receiver.recv_async().await?;
                                debug!(?addr, "new connection");
                                let cn = DpdkInlineCn::new(
                                    state.listen_addr.port(),
                                    Some(addr),
                                    Some(state.sender.clone()),
                                    Arc::clone(&state.initialized_mempools),
                                    None,
                                );
                                return Ok(Some((cn, state)));
                            }
                        },
                    )) as _)
                }
                V6(a) => Err(eyre!("Only IPv4 is supported: {:?}", a)),
            }
        })())
    }
}

/// A message from DPDK.
#[derive(Debug)]
struct Msg {
    /// The local port.
    pub port: u16,
    /// The remote address.
    pub addr: SocketAddrV4,
    mbuf: *mut rte_mbuf,
    payload_length: usize,
}

impl Msg {
    pub fn get_buf(&self) -> &[u8] {
        unsafe { mbuf_slice!(self.mbuf, TOTAL_HEADER_SIZE, self.payload_length) }
    }
}

impl Drop for Msg {
    fn drop(&mut self) {
        unsafe {
            rte_pktmbuf_free(self.mbuf);
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct SendMsg {
    to_addr: SocketAddrV4,
    src_port: u16,
    buf_ptr: *const u8,
    buf_len: usize,
}

/// DPDK state with which packets can be sent/received.
///
/// It is responsible for actually sending and receiving packets, and doing bookkeeping (mux/demux)
/// associated with tracking sockets.
pub struct DpdkState {
    eth_addr: MacAddress,
    eth_addr_raw: rte_ether_addr,
    ip_addr: Ipv4Addr,
    ip_addr_raw: u32,
    port: u16,
    mbuf_pool: *mut rte_mempool,
    arp_table: HashMap<Ipv4Addr, MacAddress>,

    rx_queue_id: usize,
    rx_bufs: [*mut rte_mbuf; RECEIVE_BURST_SIZE as usize],
    rx_packets_for_ports: Vec<((u16, SocketAddrV4), Vec<Msg>)>,

    rx_recv_buf: Vec<Option<Msg>>,

    tx_bufs: [*mut rte_mbuf; RECEIVE_BURST_SIZE as usize],
    ip_id: u16,
}

// SAFETY: rte_mempools should be ok to pass between threads.
unsafe impl Send for DpdkState {}

impl DpdkState {
    /// Do global initialization.
    ///
    /// `config_path` should be a TOML files with:
    /// - "dpdk" table with "eal_init" key. "eal_init" should be a string array of DPDK init args.
    /// - "net" table with "ip" key and "arp" list-of-tables.
    ///   - "arp" entries should have "ip" and "mac" keys.
    ///
    /// # Example Config
    /// ```toml
    /// [dpdk]
    /// eal_init = ["-n", "4", "-l", "0-4", "--allow", "0000:08:00.0", "--proc-type=auto"]
    ///
    /// [net]
    /// ip = "1.2.3.4"
    ///
    ///   [[net.arp]]
    ///   ip = "1.2.3.4"
    ///   mac = "00:01:02:03:04:05"
    ///
    ///   [[net.arp]]
    ///   ip = "4.3.2.1"
    ///   mac = "05:04:03:02:01:00"
    /// ```
    fn new(config_path: std::path::PathBuf, num_dpdk_threads: usize) -> Result<Vec<Self>, Report> {
        let (dpdk_config, ip_addr, arp_table) = parse_cfg(config_path.as_path())?;
        let (mbuf_pools, nb_ports) = dpdk_init(dpdk_config, num_dpdk_threads)?;
        let port = nb_ports - 1;

        // what is my ethernet address (rte_ether_addr struct)
        let my_eth = get_my_macaddr(port)?;
        let eth_addr = MacAddress::from_bytes(&my_eth.addr_bytes).wrap_err("Parse mac address")?;
        let eth_addr_raw = rte_ether_addr {
            addr_bytes: eth_addr.to_array(),
        };

        let octets = ip_addr.octets();
        let ip_addr_raw: u32 = unsafe { make_ip(octets[0], octets[1], octets[2], octets[3]) };

        Ok(mbuf_pools
            .into_iter()
            .enumerate()
            .map(|(qid, mbuf_pool)| Self {
                eth_addr,
                eth_addr_raw,
                ip_addr,
                ip_addr_raw,
                port,
                mbuf_pool,
                arp_table: arp_table.clone(),
                rx_queue_id: qid,
                rx_bufs: [std::ptr::null_mut(); RECEIVE_BURST_SIZE as usize],
                rx_packets_for_ports: Vec::with_capacity(16),
                rx_recv_buf: (0..RECEIVE_BURST_SIZE).map(|_| None).collect(),
                tx_bufs: [std::ptr::null_mut(); RECEIVE_BURST_SIZE as usize],
                ip_id: 0,
            })
            .collect())
    }

    fn try_recv_burst<'cn>(
        &'cn mut self,
        port_filter: Option<(u16, Option<SocketAddrV4>)>,
        new_conns: Option<&Sender<SocketAddrV4>>,
    ) -> Result<&'cn mut [Option<Msg>], Report> {
        match port_filter {
            None => (),
            // if dst_port filter is enable or both dst_port and src_addr filter are enabled, try popping off some packets from stashed packets.
            Some((wanted_dst_port, maybe_wanted_src_addr)) => {
                for ((cand_dst_port, cand_src_addr), ref mut stash) in
                    &mut self.rx_packets_for_ports
                {
                    if *cand_dst_port == wanted_dst_port
                        && (maybe_wanted_src_addr.is_none()
                            || *cand_src_addr == maybe_wanted_src_addr.unwrap())
                        && !stash.is_empty()
                    {
                        let mut num_returned = 0;
                        while num_returned < self.rx_recv_buf.len() && !stash.is_empty() {
                            self.rx_recv_buf[num_returned] = Some(stash.pop().unwrap());
                            num_returned += 1;
                        }

                        return Ok(&mut self.rx_recv_buf[..num_returned]);
                    }
                }
            }
        }

        ensure!(
            self.rx_recv_buf.len() >= RECEIVE_BURST_SIZE as usize,
            "Received messages slice not large enough"
        );
        let num_received = unsafe {
            rte_eth_rx_burst(
                self.port,
                self.rx_queue_id as _,
                self.rx_bufs.as_mut_ptr(),
                RECEIVE_BURST_SIZE as u16,
            )
        } as usize;
        let mut num_valid = 0;
        let mut num_invalid = 0;
        'per_pkt: for i in 0..num_received {
            // first: parse if valid packet, and what the payload size is
            let (is_valid, src_ether, src_ip, src_port, dst_port, payload_length) =
                unsafe { parse_packet(self.rx_bufs[i], &self.eth_addr_raw as _, self.ip_addr_raw) };
            if !is_valid {
                unsafe { rte_pktmbuf_free(self.rx_bufs[i]) };
                num_invalid += 1;
                continue;
            }

            let [oct1, oct2, oct3, oct4] = src_ip.to_be_bytes();
            let pkt_src_ip = Ipv4Addr::new(oct1, oct2, oct3, oct4);

            // opportunistically update arp
            self.arp_table
                .entry(pkt_src_ip)
                .or_insert_with(|| MacAddress::from_bytes(&src_ether.addr_bytes).unwrap());
            let pkt_src_addr = SocketAddrV4::new(pkt_src_ip, src_port);

            let msg = Msg {
                port: dst_port,
                addr: pkt_src_addr,
                mbuf: self.rx_bufs[i],
                payload_length,
            };

            // Msg::drop will free the mbuf. So, after this point we should not call free
            // ourselves, and instead let Msg handle it for us.

            // this call wants packets for the flow on `call_port`, but we might have gotten
            // packets for other flows. So we stash those packets for future calls.
            match port_filter {
                Some((wanted_dst_port, None)) if wanted_dst_port != dst_port => {
                    for ((p, _), ref mut stash) in &mut self.rx_packets_for_ports {
                        if *p == dst_port {
                            stash.push(msg);
                            debug!(stash_size = ?stash.len(), ?dst_port, "Stashed packet");
                            break;
                        }
                    }

                    // the packet didn't match any ports. can drop.
                }
                Some((ref wanted_dst_port, Some(ref wanted_src_addr)))
                    if *wanted_dst_port != dst_port && *wanted_src_addr != pkt_src_addr =>
                {
                    let mut found_dst_port = false;
                    for ((cand_dst_port, cand_src_addr), ref mut stash) in
                        &mut self.rx_packets_for_ports
                    {
                        if *cand_dst_port == dst_port {
                            found_dst_port = true;
                            if *cand_src_addr == pkt_src_addr {
                                stash.push(msg);
                                debug!(stash_size = ?stash.len(), ?dst_port, ?pkt_src_addr, "Stashed packet");
                                continue 'per_pkt;
                            }
                        }
                    }

                    // if found_dst_port but we reached this point, then we've found a new flow.
                    // allocate a new stash and save `msg` in it.
                    if found_dst_port {
                        let mut new_stash = Vec::with_capacity(16);
                        new_stash.push(msg);
                        self.rx_packets_for_ports
                            .push(((dst_port, pkt_src_addr), new_stash));
                        trace!(?dst_port, ?pkt_src_addr, "created new stash for connection");
                        // signal new connection.
                        if let Some(nc) = new_conns {
                            nc.send(pkt_src_addr)
                                .wrap_err("New connection channel send failed")?;
                        }
                    }
                    // else !found_dst_port, which means no one was listening and we can drop.
                }
                _ => {
                    // either there is no filtering, or there is and it matched. either way, we can
                    // return this packet now.
                    self.rx_recv_buf[num_valid] = Some(msg);
                    num_valid += 1;
                }
            }
        }

        if num_valid > 0 {
            trace!(?num_valid, "Received valid packets");
        }

        if num_invalid > 0 {
            debug!(?num_invalid, "Discarded invalid packets");
        }

        Ok(&mut self.rx_recv_buf[..num_valid])
    }

    fn try_recv_burst_stash_only<'cn>(
        &'cn mut self,
        new_conns: Option<&Sender<SocketAddrV4>>,
    ) -> Result<(), Report> {
        ensure!(
            self.rx_recv_buf.len() >= RECEIVE_BURST_SIZE as usize,
            "Received messages slice not large enough"
        );
        let num_received = unsafe {
            rte_eth_rx_burst(
                self.port,
                self.rx_queue_id as _,
                self.rx_bufs.as_mut_ptr(),
                RECEIVE_BURST_SIZE as u16,
            )
        } as usize;

        let mut num_valid = 0;
        'per_pkt: for i in 0..num_received {
            // first: parse if valid packet, and what the payload size is
            let (is_valid, src_ether, src_ip, src_port, dst_port, payload_length) =
                unsafe { parse_packet(self.rx_bufs[i], &self.eth_addr_raw as _, self.ip_addr_raw) };
            if !is_valid {
                unsafe { rte_pktmbuf_free(self.rx_bufs[i]) };
                continue;
            }

            let [oct1, oct2, oct3, oct4] = src_ip.to_be_bytes();
            let pkt_src_ip = Ipv4Addr::new(oct1, oct2, oct3, oct4);

            // opportunistically update arp
            self.arp_table
                .entry(pkt_src_ip)
                .or_insert_with(|| MacAddress::from_bytes(&src_ether.addr_bytes).unwrap());
            let pkt_src_addr = SocketAddrV4::new(pkt_src_ip, src_port);

            num_valid += 1;
            let msg = Msg {
                port: dst_port,
                addr: pkt_src_addr,
                mbuf: self.rx_bufs[i],
                payload_length,
            };

            // Msg::drop will free the mbuf. So, after this point we should not call free
            // ourselves, and instead let Msg handle it for us.

            // we immediately stash this packet for later retrieval.
            for ((cand_dst_port, cand_src_addr), ref mut stash) in &mut self.rx_packets_for_ports {
                if *cand_dst_port == dst_port {
                    // packet for existing flow. we should not re-notify.
                    if *cand_src_addr == pkt_src_addr {
                        stash.push(msg);
                        continue 'per_pkt;
                    }
                }
            }

            // if found_dst_port but we reached this point, then we've found a new flow.
            // allocate a new stash and save `msg` in it.
            let mut new_stash = Vec::with_capacity(16);
            new_stash.push(msg);
            self.rx_packets_for_ports
                .push(((dst_port, pkt_src_addr), new_stash));
            trace!(?dst_port, ?pkt_src_addr, "created new stash for connection");
            // signal new connection.
            if let Some(nc) = new_conns {
                nc.try_send(pkt_src_addr)
                    .wrap_err("New connection channel send failed")?;
            }
        }

        if num_valid > 0 {
            trace!(?num_valid, "stashed packets");
        }

        Ok(())
    }

    fn send_burst(&mut self, msgs: impl Iterator<Item = SendMsg>) -> Result<(), Report> {
        let mut i = 0;
        for SendMsg {
            to_addr,
            src_port,
            buf_ptr,
            buf_len,
        } in msgs
        {
            let to_ip = to_addr.ip();
            let to_port = to_addr.port();
            unsafe {
                let dst_ether_addr = match self.arp_table.get(to_ip) {
                    Some(eth) => eth,
                    None => {
                        warn!(?to_ip, "Could not find IP in ARP table");
                        continue;
                    }
                };

                self.tx_bufs[i] = alloc_mbuf(self.mbuf_pool).unwrap();

                let src_info = AddressInfo {
                    udp_port: src_port,
                    ipv4_addr: self.ip_addr,
                    ether_addr: self.eth_addr,
                };

                let dst_info = AddressInfo {
                    udp_port: to_port,
                    ipv4_addr: *to_ip,
                    ether_addr: *dst_ether_addr,
                };

                // fill header
                let hdr_size = match fill_in_header(
                    self.tx_bufs[i],
                    &HeaderInfo { src_info, dst_info },
                    buf_len,
                    self.ip_id,
                ) {
                    Ok(s) => {
                        self.ip_id += 1;
                        self.ip_id %= 0xffff;
                        s
                    }
                    Err(err) => {
                        debug!(?err, "Error writing header");
                        rte_pktmbuf_free(self.tx_bufs[i]);
                        continue;
                    }
                };

                // write payload
                let payload_slice = mbuf_slice!(self.tx_bufs[i], hdr_size, buf_len);
                rte_memcpy_wrapper(payload_slice.as_mut_ptr() as _, buf_ptr as _, buf_len);

                (*self.tx_bufs[i]).pkt_len = (hdr_size + buf_len) as u32;
                (*self.tx_bufs[i]).data_len = (hdr_size + buf_len) as u16;

                i += 1;
                if i >= (RECEIVE_BURST_SIZE as _) {
                    break;
                }
            }
        }

        if i > 0 {
            if let Err(err) = unsafe {
                tx_burst(
                    self.port,
                    self.rx_queue_id as _,
                    self.tx_bufs.as_mut_ptr(),
                    i as u16,
                )
            } {
                warn!(?err, "tx_burst error");
            }
        }

        Ok(())
    }
}
