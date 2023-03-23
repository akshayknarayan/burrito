use ahash::AHashMap as HashMap;
use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{bail, ensure, eyre, Report, WrapErr};
use dpdk_wrapper::{
    bindings::{get_lcore_id, get_lcore_map},
    wrapper::{affinitize_thread, FlowSteeringHandle},
};
use flume::{Receiver, Sender};
use futures_util::future::ready;
use futures_util::Stream;
use futures_util::{future::Ready, stream::Once};
use std::future::Future;
use std::net::{SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::{cell::RefCell, fmt::Debug};
use tracing::{debug, debug_span, error, info, trace, trace_span, warn};
use tracing_futures::Instrument;

mod dpdk_state;
pub use dpdk_state::{DpdkState, Msg, SendMsg};

std::thread_local! {
    pub static DPDK_STATE: RefCell<Option<DpdkState>> = RefCell::new(None);
}

#[derive(Default)]
struct FlowSteering(HashMap<u16, (Option<FlowSteeringHandle>, Vec<u16>)>);

impl FlowSteering {
    pub fn add_flow(&mut self, dpdk: &mut DpdkState, port: u16) -> Result<(), Report> {
        let (ref mut port_handle, ref mut queues_on_port) = self.0.entry(port).or_default();
        // 1. if port_handle is Some, it's always getting dropped and replaced
        //    here. we need to drop first to clear the old rule.
        std::mem::drop(port_handle.take());
        // 2. add to queues_on_port and make the new rule.
        queues_on_port.push(dpdk.rx_queue_id() as _);
        queues_on_port.sort();
        let flow_handle = dpdk.register_flow_steering(port, &queues_on_port[..])?;

        // 3. save the flow_handle.
        *port_handle = Some(flow_handle);

        Ok(())
    }

    pub fn remove_flow(&mut self, dpdk: &mut DpdkState, port: u16) -> Result<(), Report> {
        let queue_id = dpdk.rx_queue_id() as u16;
        let (ref mut port_handle, ref mut queues_on_port) = self.0.entry(port).or_default();
        debug!(?port, "removing flow steering rule");
        std::mem::drop(port_handle.take());

        // 1. remove `queue` from `queues_on_port`
        let mut found = false;
        for i in 0..queues_on_port.len() {
            if queues_on_port[i] == queue_id {
                queues_on_port.swap_remove(i);
                found = true;
                break;
            }
        }

        ensure!(found, "queue not registered on port");
        if queues_on_port.is_empty() {
            return Ok(());
        }

        // 2. now make a new rule for the current number of flows.
        queues_on_port.sort();
        let flow_handle = dpdk.register_flow_steering(port, &queues_on_port[..])?;

        *port_handle = Some(flow_handle);
        Ok(())
    }
}

/// Chunnel implementation for DPDK inline.
///
/// The strategy here is to store the DPDK mempool state in a `thread_local`. On chunnel init, we
/// initialize the requested number of DPDK mempools and store them in this struct. When we make a
/// connection, we pop a mempool and affinitize that thread (because using mempools cross-thread is
/// unsafe).
///
/// Later, if the task corresponding to this connection moves to another thread, during the actual
/// send/receive the connection will query the `thread_local` to get the local DPDK mempool to
/// issue sends/receives against. If there is not one, it will try to initialize that thread. If
/// there are no mempools left, this will fail, but in theory we should not have more threads than
/// mempools.
///
/// Calling `listen()` causes a flow rule to be created which steers packets to that thread. If
/// another flow calls `listen()` on the same address on another thread, this flow rule will be
/// deleted and replaced with another one that does RSS between the two queues.
///
/// The effect of this is that we should avoid moving `DpdkInlineCn`s between threads, even though
/// doing so is thread-safe and memory-safe.
#[derive(Clone)]
pub struct DpdkInlineChunnel {
    // for initialization only. once we start sending/receiving on some threads, we will take these
    // out of this Vec and move them into the `DPDK_STATE` thread local variable.
    initialization_state: Arc<Mutex<DpdkInitState>>,
    // for connect-side connections, ephemeral source ports.
    ephemeral_ports: Arc<Mutex<Vec<u16>>>,
    // map local dest ports to: (1) a handle to an `rte_flow` and (2) a list of queues that are
    // listening on that port.
    //
    // we need to manage single-queue vs rss flow steering rules based on how many flows are
    // listening on the port.
    flow_steering: Arc<Mutex<FlowSteering>>,
}

struct DpdkInitState {
    mempools: Vec<DpdkState>,
    lcore_map: Vec<u32>,
}

impl Debug for DpdkInlineChunnel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DpdkInlineChunnel").finish()
    }
}

impl DpdkInlineChunnel {
    /// Initialize DPDK mempools for use in connections.
    ///
    /// Should make sure `num_dpdk_threads` is >= the number of tokio workers (threads).
    /// Otherwise we might run out of mempools.
    pub fn new(config_path: PathBuf, num_dpdk_threads: usize) -> Result<Self, Report> {
        let dpdks = DpdkState::new(config_path, num_dpdk_threads)?;
        let lcore_map = get_lcore_map().wrap_err("Could not fetch DPDK lcore map")?;
        debug!(?lcore_map, "got lcore map");
        ensure!(
            lcore_map.len() >= num_dpdk_threads,
            "Not enough DPDK lcores ({:?}) for number of requested threads ({:?})",
            lcore_map.len(),
            num_dpdk_threads
        );
        Ok(DpdkInlineChunnel {
            initialization_state: Arc::new(Mutex::new(DpdkInitState {
                mempools: dpdks,
                lcore_map,
            })),
            ephemeral_ports: Arc::new(Mutex::new((4096..16_384).collect())),
            flow_steering: Default::default(),
        })
    }
}

fn try_init_thread(init_state: &Mutex<DpdkInitState>) -> Result<(), Report> {
    DPDK_STATE
        .try_with(|dpdk| {
            let mut dpdk = dpdk.borrow_mut();
            if dpdk.is_none() {
                // get a core assignment.
                let mut init_state_g = init_state.lock().unwrap();
                let core_id = init_state_g
                    .lcore_map
                    .pop()
                    .ok_or(eyre!("No remaining lcores"))?;

                // affinitize
                affinitize_thread(core_id as _)
                    .wrap_err(eyre!("affinitizing thread to lcore {:?}", core_id))?;

                //let remaining_qids: Vec<_> = init_state_g
                //    .mempools
                //    .iter()
                //    .map(DpdkState::rx_queue_id)
                //    .collect();
                //debug!(?remaining_qids, "pulling dpdk queue");
                let dpdk_state = init_state_g
                    .mempools
                    .pop()
                    .ok_or(eyre!("No remaining initialized dpdk thread mempools"))?;
                info!(?core_id, qid = ?dpdk_state.rx_queue_id(), "taking initialized DpdkState");
                *dpdk = Some(dpdk_state);
            }

            Ok::<_, Report>(())
        })
        .wrap_err(eyre!("Error accessing dpdk state thread_local"))?
        .wrap_err(eyre!("Error initializing thread-local dpdk state"))
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
                try_init_thread(self.initialization_state.as_ref()).and_then(|_| {
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

                        if let Err(err) = {
                            let mut steering_g = self.flow_steering.lock().unwrap();
                            steering_g.add_flow(dpdk, a.port())
                        } {
                            warn!(?err, "Error setting flow steering. This could be ok, as long as the last one works.");
                        }

                        dpdk.register_flow_buffer(
                            a.port(),
                            SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, 0),
                        );

                        Ok(DpdkInlineCn::new(
                            a.port(),
                            None,
                            None,
                            Arc::clone(&self.initialization_state),
                            Some(Arc::clone(&self.ephemeral_ports)),
                        )
                        .with_flow_steering(Arc::clone(&self.flow_steering)))
                    })
                }),
            )) as _),
            V6(a) => Err(eyre!("Only IPv4 is supported: {:?}", a)),
        })
    }
}

impl ChunnelConnector for DpdkInlineChunnel {
    type Addr = SocketAddr;
    type Connection = DpdkInlineCn;
    type Future = futures_util::future::Ready<Result<Self::Connection, Report>>;
    type Error = Report;

    fn connect(&mut self, _addr: Self::Addr) -> Self::Future {
        ready((|| {
            try_init_thread(self.initialization_state.as_ref()).and_then(|_| {
                DPDK_STATE.with(|dpdk_cell| {
                    let this_lcore = get_lcore_id();
                    let mut dpdk_opt = dpdk_cell.borrow_mut();
                    let dpdk = dpdk_opt
                        .as_mut()
                        .ok_or(eyre!("dpdk not initialized on core {:?}", this_lcore))?;

                    let port = {
                        let mut free_ports_g = self.ephemeral_ports.lock().unwrap();
                        if let Some(p) = free_ports_g.pop() {
                            p
                        } else {
                            bail!("Could not find appropriate src port to use");
                        }
                    };

                    if let Err(err) = {
                        let mut steering_g = self.flow_steering.lock().unwrap();
                        steering_g.add_flow(dpdk, port)
                    } {
                        warn!(?err, "Error setting flow steering. This could be ok, as long as the last one works.");
                    }

                    dpdk.register_flow_buffer(
                        port,
                        SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, 0),
                    );

                    Ok(
                        DpdkInlineCn::new(
                            port,
                            None, //Some(remote_addr),
                            None,
                            Arc::clone(&self.initialization_state),
                            Some(Arc::clone(&self.ephemeral_ports)),
                        ).with_flow_steering(Arc::clone(&self.flow_steering))
                    )
                })
            })
        })())
    }
}

/// Is this connection type Send? We use flow steering, so moving to any other thread will always
/// be a bad idea because packets will just pile up on the other thread's thread_local (so they
/// won't be received). This is still memory-safe, just bad for performance.
pub struct DpdkInlineCn {
    local_port: u16,
    remote_addr: Option<SocketAddrV4>,
    new_conns: Option<Sender<SocketAddrV4>>,
    port_pool: Option<Arc<Mutex<Vec<u16>>>>,
    flow_steering: Option<Arc<Mutex<FlowSteering>>>,
    closed: Option<Sender<()>>,
    _init_state: Arc<Mutex<DpdkInitState>>,
}

impl Debug for DpdkInlineCn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(remote) = self.remote_addr {
            f.debug_struct("DpdkInlineCn")
                .field("local_port", &self.local_port)
                .field("remote_addr", &remote)
                .finish()
        } else {
            f.debug_struct("DpdkInlineCn")
                .field("local_port", &self.local_port)
                .finish()
        }
    }
}

impl Drop for DpdkInlineCn {
    fn drop(&mut self) {
        // drop any stashed packets for this port.
        if let Err(err) = DPDK_STATE.try_with(|dpdk_cell| {
            let mut dpdk_opt = dpdk_cell.borrow_mut();
            if let Some(dpdk) = dpdk_opt.as_mut() {
                dpdk.deregister_flow_buffer(self.local_port, self.remote_addr);

                if let Some(fs) = self.flow_steering.take() {
                    let mut steering_g = fs.lock().unwrap();
                    if let Err(err) = steering_g.remove_flow(dpdk, self.local_port) {
                        warn!(?err, "Error editing steering rules on flow drop");
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

        if let Some(s) = &self.closed {
            if let Err(err) = s.send(()) {
                warn!(?err, "Error sending close notification to accept stream");
            }
        }
    }
}

impl DpdkInlineCn {
    fn new(
        local_port: u16,
        remote_addr: Option<SocketAddrV4>,
        new_conns: Option<Sender<SocketAddrV4>>,
        init_state: Arc<Mutex<DpdkInitState>>,
        port_pool: Option<Arc<Mutex<Vec<u16>>>>,
    ) -> Self {
        Self {
            local_port,
            remote_addr,
            new_conns,
            port_pool,
            flow_steering: None,
            closed: None,
            _init_state: init_state,
        }
    }

    fn with_flow_steering(mut self, flow_steering: Arc<Mutex<FlowSteering>>) -> Self {
        self.flow_steering = Some(flow_steering);
        self
    }

    fn with_closed_notification(mut self, closed: Sender<()>) -> Self {
        self.closed = Some(closed);
        self
    }

    pub fn local_port(&self) -> u16 {
        self.local_port
    }

    pub fn remote_addr(&self) -> Option<SocketAddrV4> {
        self.remote_addr
    }

    pub fn new_conn_signaller(&self) -> Option<&Sender<SocketAddrV4>> {
        self.new_conns.as_ref()
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
                            buf,
                        }
                    }))?;

                    // If a stream returns a connection which then only sends things without
                    // calling recv(), no new connections would be discovered. To prevent this, add
                    // a call to try_recv_burst_stash_only here.
                    dpdk.try_recv_burst_stash_only(self.new_conns.as_ref())
                        .wrap_err("try_recv_burst_stash_only after send")?;
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
        let local_port = self.local_port;
        let remote_addr = self.remote_addr;
        let new_conns = self.new_conns.as_ref();
        let clk = quanta::Clock::new();
        let mut start = clk.now();
        Box::pin(
            async move {
                trace!("start");
                loop {
                    let ret = DPDK_STATE
                        .try_with(|dpdk_cell| {
                            let mut dpdk_opt = dpdk_cell.borrow_mut();
                            let dpdk = dpdk_opt
                                .as_mut()
                                .ok_or(eyre!("dpdk not initialized on core {:?}", this_lcore))?;
                            let msgs =
                                dpdk.try_recv_burst(Some((local_port, remote_addr)), new_conns, Some(msgs_buf.len()))?;

                            let mut slot_idx = 0;
                            for msg in msgs.iter_mut().map_while(Option::take) {
                                ensure!(msg.port == local_port, "Port mismatch");
                                if let Some((ref mut addr, ref mut payload)) = msgs_buf[slot_idx] {
                                    *addr = SocketAddr::V4(msg.addr);
                                    payload.clear();
                                    payload.extend_from_slice(msg.get_buf());
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
                        trace!(num_returned = ?ret, "done");
                        return Ok(&mut msgs_buf[..ret]);
                    } else {
                        if clk.now().duration_since(start) > std::time::Duration::from_secs(5) {
                            if let Err(err) = DPDK_STATE.try_with(|dpdk_cell| {
                                let mut dpdk_opt = dpdk_cell.borrow_mut();
                                let dpdk = dpdk_opt.as_mut().ok_or(eyre!(
                                    "dpdk not initialized on core {:?}",
                                    this_lcore
                                ))?;
                                let stats =  dpdk.eth_stats()?;
                                debug!(in_packets = ?stats.ipackets, in_errrors = ?stats.ierrors, "eth_dev stats");
                                Ok::<_, Report>(())
                            }) {
                                debug!(?err, "error getting eth_dev stats");
                            }
                            start = clk.now();
                        }

                        tokio::task::yield_now().await;
                    }
                }
            }
            .instrument(trace_span!("DpdkInlineCn::recv")),
        )
    }
}

/// Demultiplex an incoming packet stream into connections by port.
///
/// To do this, we rely on `DpdkInlineCn`'s stashing implementation. We listen for the first
/// connection in stash-only mode (i.e., don't return packets, only buffer them). Once we find a
/// connection, we yield a connection from the stream. After this, calling `recv()` on that
/// connection will send on a channel when new connections are found, so we no longer need to poll
/// independently.
#[derive(Debug, Clone)]
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
                    let this_lcore = get_lcore_id();
                    try_init_thread(self.0.initialization_state.as_ref())
                        .wrap_err("try_init_thread failed")?;

                    // we don't return `FlowSteeringHandle`s to connections here. Instead we
                    // register once here. If this stream is ever dropped/cancelled, the state will
                    // get cleaned up then.
                    //
                    // we just initialized dpdk on this thread, so `.with` is fine instead of
                    // `.try_with`.
                    {
                        let mut steering_g = self.0.flow_steering.lock().unwrap();
                        DPDK_STATE.with(|dpdk_cell| {
                            let mut dpdk_opt = dpdk_cell.borrow_mut();
                            let dpdk = dpdk_opt
                                .as_mut()
                                .ok_or(eyre!("dpdk not initialized on core {:?}", this_lcore))?;
                            if let Err(err) = steering_g.add_flow(dpdk, a.port()) {
                                warn!(?err, "Error setting flow steering. This could be ok, as long as the last one works.");
                            }

                            Ok::<_, Report>(())
                        })?;
                    }

                    let (s, r) = flume::bounded(16);
                    let (conn_closed_notifier, conn_closed_listener) = flume::bounded(16);
                    let state = StreamState {
                        listen_addr: a,
                        sender: s,
                        receiver: r,
                        conn_closed_notifier,
                        conn_closed_listener,
                        initialization_state: Arc::clone(&self.0.initialization_state),
                        flow_steering: Arc::clone(&self.0.flow_steering),
                        conn_count: 0,
                    };

                    Ok(Box::pin(futures_util::stream::try_unfold(state, |state| {
                        let listen_addr = state.listen_addr;
                        state
                            .get_next()
                            .instrument(debug_span!("connection_listen", ?listen_addr))
                    })) as _)
                }
                V6(a) => Err(eyre!("Only IPv4 is supported: {:?}", a)),
            }
        })())
    }
}

struct StreamState {
    listen_addr: SocketAddrV4,
    sender: Sender<SocketAddrV4>,
    receiver: Receiver<SocketAddrV4>,
    conn_closed_notifier: Sender<()>,
    conn_closed_listener: Receiver<()>,
    initialization_state: Arc<Mutex<DpdkInitState>>,
    flow_steering: Arc<Mutex<FlowSteering>>,
    conn_count: usize,
}

impl Drop for StreamState {
    fn drop(&mut self) {
        let this_lcore = get_lcore_id();
        let mut steering_g = self.flow_steering.lock().unwrap();
        if let Err(err) = DPDK_STATE.with(|dpdk_cell| {
            let mut dpdk_opt = dpdk_cell.borrow_mut();
            let dpdk = dpdk_opt
                .as_mut()
                .ok_or(eyre!("dpdk not initialized on core {:?}", this_lcore))?;
            steering_g.remove_flow(dpdk, self.listen_addr.port())?;
            Ok::<_, Report>(())
        }) {
            warn!(?err, "Error updating flow steering on stream close");
        }
    }
}

impl StreamState {
    async fn get_next(mut self) -> Result<Option<(DpdkInlineCn, Self)>, Report> {
        loop {
            if self.conn_count == 0 {
                debug!(addr = ?self.listen_addr, "DpdkInlineReqChunnel listen stream poll for first packet");
                loop {
                    // we initialized dpdk on this thread, so `.with` is fine instead of `.try_with`.
                    DPDK_STATE.with(|dpdk_cell| {
                        let this_lcore = get_lcore_id();
                        let mut dpdk_opt = dpdk_cell.borrow_mut();
                        let dpdk = dpdk_opt
                            .as_mut()
                            .ok_or(eyre!("dpdk not initialized on core {:?}", this_lcore))?;

                        dpdk.try_recv_burst_stash_only(Some(&self.sender))?;
                        Ok::<_, Report>(())
                    })?;

                    match self.receiver.try_recv() {
                        Ok(addr) => {
                            debug!(?addr, "DpdkInlineReqChunnel found first connection");
                            let cn = DpdkInlineCn::new(
                                self.listen_addr.port(),
                                Some(addr),
                                Some(self.sender.clone()),
                                Arc::clone(&self.initialization_state),
                                None,
                            )
                            .with_closed_notification(self.conn_closed_notifier.clone());
                            self.conn_count += 1;
                            return Ok(Some((cn, self)));
                        }
                        Err(flume::TryRecvError::Empty) => tokio::task::yield_now().await,
                        Err(flume::TryRecvError::Disconnected) => {
                            error!(addr = ?self.listen_addr, "New connection receiver closed without any connections");
                            unreachable!("New connection receiver is in disconnected state");
                        }
                    }
                }
            } else {
                debug!(cnt = ?self.conn_count, "DpdkInlineReqChunnel listen stream wait for connection signal");
                // recv will never have no senders since we keep one around locally. When a
                // connection closes, it lets us know so we can check whether it was the last one
                // (`Arc::strong_count` above), and thus we have to search for a connection
                // ourselves.
                use futures_util::future::Either;
                let ret = match futures_util::future::select(
                    self.receiver.recv_async(),
                    self.conn_closed_listener.recv_async(),
                )
                .await
                {
                    // An existing conection found a new flow.
                    Either::Left((Ok(addr), _)) => {
                        debug!(?addr, "DpdkInlineReqChunnel returning new connection");
                        self.conn_count += 1;
                        Some(
                            DpdkInlineCn::new(
                                self.listen_addr.port(),
                                Some(addr),
                                Some(self.sender.clone()),
                                Arc::clone(&self.initialization_state),
                                None,
                            )
                            .with_closed_notification(self.conn_closed_notifier.clone()),
                        )
                    }
                    // A flow exited. We have to check if we are the only one left.
                    Either::Right((Ok(_), _)) => {
                        self.conn_count -= 1;
                        None
                    }
                    Either::Left((Err(err), _)) | Either::Right((Err(err), _)) => {
                        // since this loop keeps a sender, it should not be possible for the
                        // receiver to disconnect.
                        error!(addr = ?self.listen_addr, ?err, "New connection receiver is in disconnected state");
                        unreachable!(
                            "New connection receiver is in disconnected state: {:?}",
                            err
                        );
                    }
                };

                if let Some(cn) = ret {
                    return Ok(Some((cn, self)));
                }
            }
        }
    }
}
