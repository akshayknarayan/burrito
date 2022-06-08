use ahash::AHashMap as HashMap;
use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{bail, ensure, eyre, Report, WrapErr};
use dpdk_wrapper::{
    bindings::*,
    mbuf_slice,
    utils::{parse_cfg, AddressInfo, HeaderInfo, TOTAL_HEADER_SIZE},
    wrapper::*,
};
use eui48::MacAddress;
use futures_util::future::ready;
use futures_util::Stream;
use std::cell::RefCell;
use std::future::Future;
use std::mem::zeroed;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, error, info, trace, warn};

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
    pub fn new(config_path: std::path::PathBuf, num_dpdk_threads: usize) -> Result<Self, Report> {
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
    type Future = futures_util::future::Ready<Result<Self::Stream, Report>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn listen(&mut self, addr: Self::Addr) -> Self::Future {
        use SocketAddr::*;
        ready(match addr {
            V4(a) => Ok(Box::pin(futures_util::stream::once(ready({
                try_init_thread(self.initialized_mempools.as_ref()).and_then(|_| {
                    Ok(DpdkInlineCn::new(
                        a.port(),
                        Arc::clone(&self.initialized_mempools),
                    ))
                })
            }))) as _),
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
        ready({
            try_init_thread(self.initialized_mempools.as_ref()).and_then(|_| {
                Ok(DpdkInlineCn::new(
                    self.ephemeral_ports
                        .lock()
                        .unwrap()
                        .pop()
                        .ok_or(eyre!("No available ports"))?,
                    Arc::clone(&self.initialized_mempools),
                ))
            })
        })
    }
}

pub struct DpdkInlineCn {
    src_port: u16,
    mempools: Arc<Mutex<Vec<DpdkState>>>,
}

impl DpdkInlineCn {
    fn new(src_port: u16, mempools: Arc<Mutex<Vec<DpdkState>>>) -> Self {
        Self { src_port, mempools }
    }
}

impl std::fmt::Debug for DpdkInlineCn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DpdkInlineCn").finish()
    }
}

impl ChunnelConnection for DpdkInlineCn {
    type Data = (SocketAddr, Vec<u8>);

    fn send(
        &self,
        (addr, buf): Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        let this_lcore = get_lcore_id();
        Box::pin(ready({
            if let SocketAddr::V4(addr) = addr {
                DPDK_STATE
                    .try_with(|dpdk_cell| {
                        let mut dpdk_opt = dpdk_cell.borrow_mut();
                        let dpdk = dpdk_opt
                            .as_mut()
                            .ok_or(eyre!("dpdk not initialized on core {:?}", this_lcore))?;
                        dpdk.send(addr, self.src_port, &buf)?;
                        Ok(())
                    })
                    .map_err(Into::into)
                    .and_then(|x| x)
            } else {
                Err(eyre!("Need ipv4 addr: {:?}", addr))
            }
        }))
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let this_lcore = get_lcore_id();
        Box::pin(ready({
            DPDK_STATE.try_with(|dpdk_cell| {
                let mut dpdk_opt = dpdk_cell.borrow_mut();
                let dpdk = dpdk_opt
                    .as_mut()
                    .ok_or(eyre!("dpdk not initialized on core {:?}", this_lcore))?;
                let mut recv_msg_buf: [Option<Msg>; RECEIVE_BURST_SIZE as usize] =
                    Default::default();
                let msgs = dpdk.try_recv(&mut recv_msg_buf[..])?;
                // what to do with these msgs??
                todo!()
            })
        }))
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
    listen_ports: Option<Vec<u16>>,

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
                rx_bufs: unsafe { zeroed() },
                tx_bufs: unsafe { zeroed() },
                ip_id: 0,
                listen_ports: None,
            })
            .collect())
    }

    /// This doesn't do anything other than filter incoming ports.
    fn listen(&mut self, port: u16) {
        if let Some(ref mut lp) = self.listen_ports {
            lp.push(port);
        } else {
            self.listen_ports = Some(vec![port]);
        }
    }

    fn try_recv<'buf>(
        &mut self,
        rcvd_msgs: &'buf mut [Option<Msg>],
    ) -> Result<&'buf mut [Option<Msg>], Report> {
        ensure!(
            rcvd_msgs.len() >= RECEIVE_BURST_SIZE as usize,
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
        for i in 0..num_received {
            // first: parse if valid packet, and what the payload size is
            let (is_valid, src_ether, src_ip, src_port, dst_port, payload_length) =
                unsafe { parse_packet(self.rx_bufs[i], &self.eth_addr_raw as _, self.ip_addr_raw) };
            if !is_valid {
                unsafe { rte_pktmbuf_free(self.rx_bufs[i]) };
                continue;
            }

            // if there are defined ports to listen on, enforce that only packets with those
            // dst_ports are received.
            if let Some(ref mut lp) = self.listen_ports {
                if !lp.iter().any(|p| *p == dst_port) {
                    unsafe { rte_pktmbuf_free(self.rx_bufs[i]) };
                    continue;
                }
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

            rcvd_msgs[num_valid] = Some(msg);
            num_valid += 1;
        }

        if num_valid > 0 {
            trace!(?num_valid, "Received valid packets");
        }

        Ok(&mut rcvd_msgs[..num_valid])
    }

    fn send<'a>(
        &mut self,
        to_addr: SocketAddrV4,
        src_port: u16,
        buf: &'a [u8],
    ) -> Result<(), Report> {
        let to_ip = to_addr.ip();
        let to_port = to_addr.port();
        unsafe {
            let dst_ether_addr = match self.arp_table.get(to_ip) {
                Some(eth) => eth,
                None => {
                    bail!("Could not find IP {:?} in ARP table", to_ip);
                }
            };

            self.tx_bufs[0] = alloc_mbuf(self.mbuf_pool).unwrap();

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
                self.tx_bufs[0],
                &HeaderInfo { src_info, dst_info },
                buf.len(),
                self.ip_id,
            ) {
                Ok(s) => {
                    self.ip_id += 1;
                    self.ip_id %= 0xffff;
                    s
                }
                Err(err) => {
                    debug!(?err, "Error writing header");
                    rte_pktmbuf_free(self.tx_bufs[0]);
                    bail!("Error writing header: {:?}", err);
                }
            };

            // write payload
            let payload_slice = mbuf_slice!(self.tx_bufs[0], hdr_size, buf.len());
            rte_memcpy_wrapper(
                payload_slice.as_mut_ptr() as _,
                buf.as_ptr() as _,
                buf.len(),
            );

            (*self.tx_bufs[0]).pkt_len = (hdr_size + buf.len()) as u32;
            (*self.tx_bufs[0]).data_len = (hdr_size + buf.len()) as u16;
        }

        if let Err(err) = unsafe {
            tx_burst(
                self.port,
                self.rx_queue_id as _,
                self.tx_bufs.as_mut_ptr(),
                1 as u16,
            )
        } {
            warn!(?err, "tx_burst error");
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
