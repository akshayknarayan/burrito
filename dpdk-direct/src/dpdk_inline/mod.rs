use ahash::HashMap;
use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{bail, ensure, eyre, Report, WrapErr};
use dpdk_wrapper::{
    bindings::{get_lcore_id, get_lcore_map},
    wrapper::{affinitize_thread, FlowSteeringHandle},
};
use eui48::MacAddress;
use flume::{Receiver, Sender};
use futures_util::Stream;
use futures_util::{future::ready, stream::once};
use futures_util::{future::Ready, stream::Once};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::{cell::RefCell, fmt::Debug};
use std::{future::Future, net::Ipv4Addr};
use std::{
    net::{SocketAddr, SocketAddrV4},
    sync::atomic::AtomicBool,
};
use tracing::{debug, debug_span, error, info, trace, trace_span, warn};
use tracing_futures::Instrument;

mod dpdk_state;
use dpdk_state::{DpdkState, SendMsg};

use crate::switcher::{ActiveConnection, DatapathConnectionMigrator};

std::thread_local! {
    static DPDK_STATE: RefCell<Option<DpdkState>> = RefCell::new(None);
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
        if self.0.is_empty() {
            return Ok(());
        }

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
    // once set to true, `DpdkState`s will self-destruct.
    shutdown: Arc<AtomicBool>,
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

impl Drop for DpdkInlineChunnel {
    fn drop(&mut self) {
        self.initialization_state.lock().unwrap().mempools.clear();
    }
}

impl DpdkInlineChunnel {
    /// Initialize DPDK mempools for use in connections.
    ///
    /// Should make sure `num_dpdk_threads` is >= the number of tokio workers (threads).
    /// Otherwise we might run out of mempools.
    pub fn new(config_path: PathBuf, num_dpdk_threads: usize) -> Result<Self, Report> {
        let dpdks = DpdkState::new(config_path, num_dpdk_threads)?;
        Self::do_new(dpdks, num_dpdk_threads)
    }

    pub(crate) fn get_cfg(&self) -> (Ipv4Addr, HashMap<Ipv4Addr, MacAddress>) {
        let x = DPDK_STATE.try_with(|dpdk_cell| {
            let dpdk_opt = dpdk_cell.borrow_mut();
            let dpdk = dpdk_opt
                .as_ref()
                .ok_or(eyre!("dpdk not initialized on core"))?;
            Ok::<_, Report>(dpdk.get_cfg())
        });

        match x {
            Err(_) | Ok(Err(_)) => {
                let init_g = self.initialization_state.lock().unwrap();
                if !init_g.mempools.is_empty() {
                    init_g.mempools[0].get_cfg()
                } else {
                    panic!(
                        "Could not find either thread-local dpdk state or unclaimed dpdk state."
                    );
                }
            }
            Ok(Ok(x)) => x,
        }
    }

    pub fn new_preconfig(
        ip_addr: Ipv4Addr,
        arp_table: HashMap<Ipv4Addr, MacAddress>,
        num_dpdk_threads: usize,
    ) -> Result<Self, Report> {
        let dpdks = DpdkState::new_preconfig(ip_addr, arp_table, num_dpdk_threads)
            .wrap_err("Could not initialize DPDK state")?;
        Self::do_new(dpdks, num_dpdk_threads)
    }

    fn do_new(dpdks: Vec<DpdkState>, num_dpdk_threads: usize) -> Result<Self, Report> {
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
            shutdown: Default::default(),
        })
    }

    fn do_shutdown(&self) {
        self.flow_steering.lock().unwrap().0.clear();
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Initialize state for the given set of connection descriptors.
    ///
    /// `conns` should only be connections that should live on the current thread.
    fn load_conns(
        &mut self,
        conns: Vec<ActiveConnection>,
    ) -> Result<HashMap<ActiveConnection, DpdkInlineCn>, Report> {
        let descs: Vec<_> = conns
            .iter()
            .map(|c| match c {
                ActiveConnection::UnConnected { local_port } => {
                    (*local_port, SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))
                }
                ActiveConnection::Connected {
                    local_port,
                    remote_addr,
                } => (*local_port, *remote_addr),
            })
            .collect();

        fn init_flow_steering(this: &DpdkInlineChunnel, port: u16) -> Result<(), Report> {
            DPDK_STATE.with(|dpdk_cell| {
                let this_lcore = get_lcore_id();
                let mut dpdk_opt = dpdk_cell.borrow_mut();
                let dpdk = dpdk_opt
                    .as_mut()
                    .ok_or(eyre!("dpdk not initialized on core {:?}", this_lcore))?;
                if let Err(err) = {
                    let mut steering_g = this.flow_steering.lock().unwrap();
                    steering_g.add_flow(dpdk, port)
                } {
                    warn!(?err, "Error setting flow steering. This could be ok, as long as the last one works.");
                }

                Ok::<_, Report>(())
            })
        }

        fn iter(
            this: &DpdkInlineChunnel,
            acceptors: &mut HashMap<u16, StreamState>,
            c: ActiveConnection,
        ) -> Result<(ActiveConnection, DpdkInlineCn), Report> {
            let (port, remote_addr) = match c {
                ActiveConnection::Connected {
                    local_port,
                    remote_addr,
                } => (local_port, Some(remote_addr)),
                ActiveConnection::UnConnected { local_port } => (local_port, None),
            };

            // it's ok if the port is already taken out of ephemeral_ports as long as
            // `remote_addr` is Some(_), since there can be many connected addresses on an
            // accept() port.
            let mut ports = this.ephemeral_ports.lock().unwrap();
            let mut found = false;
            for i in 0..ports.len() {
                if ports[i] == port {
                    ports.swap_remove(i);
                    found = true;
                    break;
                }
            }

            if remote_addr.is_some() {
                ensure!(
                    !(found && acceptors.contains_key(&port)),
                    "Found stream state for an unclaimed port: {:?}",
                    c
                );

                let mut should_init_flow_steering = false;
                let StreamState {
                    new_conn_notifier,
                    conn_closed_notifier,
                    ..
                } = acceptors.entry(port).or_insert_with(|| {
                    should_init_flow_steering = true;

                    // this is a new acceptor, so we need to make a stream for it.
                    let (new_conn_notifier, new_conn_listener) = flume::bounded(16);
                    let (conn_closed_notifier, conn_closed_listener) = flume::bounded(16);
                    StreamState {
                        listen_addr: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port),
                        new_conn_notifier,
                        new_conn_listener,
                        conn_closed_notifier,
                        conn_closed_listener,
                        initialization_state: Arc::clone(&this.initialization_state),
                        flow_steering: Arc::clone(&this.flow_steering),
                        conn_count: 0,
                        shutdown: Arc::clone(&this.shutdown),
                    }
                });

                if should_init_flow_steering {
                    init_flow_steering(this, port)?;
                }

                let cn = DpdkInlineCn::new(
                    port,
                    remote_addr,
                    Some(new_conn_notifier.clone()),
                    Arc::clone(&this.initialization_state),
                    Some(Arc::clone(&this.ephemeral_ports)),
                    Arc::clone(&this.shutdown),
                )
                .with_closed_notification(conn_closed_notifier.clone());
                Ok((c, cn))
            } else if found {
                init_flow_steering(this, port)?;

                let cn = DpdkInlineCn::new(
                    port,
                    None,
                    None,
                    Arc::clone(&this.initialization_state),
                    Some(Arc::clone(&this.ephemeral_ports)),
                    Arc::clone(&this.shutdown),
                )
                .with_flow_steering(Arc::clone(&this.flow_steering));
                Ok((c, cn))
            } else {
                Err(eyre!("Connection uses a duplicated port: {:?}", c))
            }
        }

        try_init_thread(&self.initialization_state.as_ref()).and_then(|_| {
            DPDK_STATE.with(|dpdk_cell| {
                let this_lcore = get_lcore_id();
                let mut dpdk_opt = dpdk_cell.borrow_mut();
                let dpdk = dpdk_opt
                    .as_mut()
                    .ok_or(eyre!("dpdk not initialized on core {:?}", this_lcore))?;
                dpdk.init_accepted(descs)?; // register flow buffers
                Ok::<_, Report>(())
            })?;

            let mut acceptors = Default::default();
            conns
                .into_iter()
                .map(|c| iter(&self, &mut acceptors, c))
                .collect()
        })
    }

    pub fn do_listen_non_accept(&mut self, addr: SocketAddr) -> Result<DpdkInlineCn, Report> {
        let a = match addr {
            SocketAddr::V4(a) => a,
            _ => bail!("Only IPv4 is supported: {:?}", addr),
        };

        try_init_thread(self.initialization_state.as_ref()).and_then(|_| {
            let mut ports = self.ephemeral_ports.lock().unwrap();
            let mut found = false;
            for i in 0..ports.len() {
                if ports[i] == a.port() {
                    ports.swap_remove(i);
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
                    Arc::clone(&self.shutdown),
                )
                .with_flow_steering(Arc::clone(&self.flow_steering)))
            })
        })
    }
}

impl DatapathConnectionMigrator for DpdkInlineChunnel {
    type Conn = DpdkInlineCn;
    type Error = Report;

    fn shut_down(&mut self) -> Result<(), Report> {
        self.do_shutdown();
        Ok(())
    }

    /// Construct connection state (and connection objects) corresponding to the provided set of
    /// `ActiveConnection`s.
    fn load_connections(
        &mut self,
        conns: Vec<ActiveConnection>,
    ) -> Result<HashMap<ActiveConnection, Self::Conn>, Self::Error> {
        self.load_conns(conns)
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

fn deinit_thread() {
    DPDK_STATE
        .try_with(|dpdk_cell| {
            let mut dpdk_opt = dpdk_cell.borrow_mut();
            dpdk_opt.take();
        })
        // `try_with` might error if the thread has already been
        // destructed, in which case there's no cleanup to do anyway.
        .unwrap_or(());
}

impl ChunnelListener for DpdkInlineChunnel {
    type Addr = SocketAddr;
    type Connection = DpdkInlineCn;
    type Future = Ready<Result<Self::Stream, Report>>;
    type Stream = Once<Ready<Result<Self::Connection, Self::Error>>>;
    type Error = Report;

    fn listen(&mut self, addr: Self::Addr) -> Self::Future {
        let res = self.do_listen_non_accept(addr);
        match res {
            Ok(r) => ready(Ok(once(ready(Ok(r))))),
            Err(e) => ready(Err(e)),
        }
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
                            Arc::clone(&self.shutdown),
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
    shutdown: Arc<AtomicBool>,
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
        // check shutdown. if it is set, we should drop this thread's local `DpdkState`
        // (`DPDK_STATE`). if already dropped, do nothing. if not set, continue with the other
        // cleanup below.
        //
        // NOTE: why is it ok to check here and nowhere else? because there is no dpdk loop outside this
        // type's `send()`/`recv()` calls. So, if we drop `DpdkInlineCn`, checking in its drop
        // handler is enough to decide whether to clean up the thread's `DpdkState`.
        if self.shutdown.load(std::sync::atomic::Ordering::SeqCst) {
            deinit_thread();
            return;
        }

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

        debug!(port = ?self.local_port, remote = ?self.remote_addr, "dropped DpdkInlineCn");
    }
}

impl DpdkInlineCn {
    fn new(
        local_port: u16,
        remote_addr: Option<SocketAddrV4>,
        new_conns: Option<Sender<SocketAddrV4>>,
        init_state: Arc<Mutex<DpdkInitState>>,
        port_pool: Option<Arc<Mutex<Vec<u16>>>>,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            local_port,
            remote_addr,
            new_conns,
            port_pool,
            shutdown,
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
pub struct DpdkInlineReqChunnel(pub DpdkInlineChunnel);

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
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + Sync + 'static>>;
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

                    let (new_conn_notifier, new_conn_listener) = flume::bounded(16);
                    let (conn_closed_notifier, conn_closed_listener) = flume::bounded(16);
                    let state = StreamState {
                        listen_addr: a,
                        new_conn_notifier,
                        new_conn_listener,
                        conn_closed_notifier,
                        conn_closed_listener,
                        initialization_state: Arc::clone(&self.0.initialization_state),
                        flow_steering: Arc::clone(&self.0.flow_steering),
                        conn_count: 0,
                        shutdown: Arc::clone(&self.0.shutdown),
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

impl DatapathConnectionMigrator for DpdkInlineReqChunnel {
    type Conn = DpdkInlineCn;
    type Error = Report;

    fn shut_down(&mut self) -> Result<(), Report> {
        self.0.do_shutdown();
        Ok(())
    }

    /// Construct connection state (and connection objects) corresponding to the provided set of
    /// `ActiveConnection`s.
    fn load_connections(
        &mut self,
        conns: Vec<ActiveConnection>,
    ) -> Result<HashMap<ActiveConnection, Self::Conn>, Self::Error> {
        self.0.load_conns(conns)
    }
}

struct StreamState {
    listen_addr: SocketAddrV4,
    new_conn_notifier: Sender<SocketAddrV4>,
    new_conn_listener: Receiver<SocketAddrV4>,
    conn_closed_notifier: Sender<()>,
    conn_closed_listener: Receiver<()>,
    initialization_state: Arc<Mutex<DpdkInitState>>,
    flow_steering: Arc<Mutex<FlowSteering>>,
    conn_count: usize,
    shutdown: Arc<AtomicBool>,
}

impl Drop for StreamState {
    fn drop(&mut self) {
        if self.shutdown.load(std::sync::atomic::Ordering::SeqCst) {
            deinit_thread();
        } else {
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

        debug!(listen = ?self.listen_addr, "exiting dpdk-inline stream");
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

                        dpdk.try_recv_burst_stash_only(Some(&self.new_conn_notifier))?;
                        Ok::<_, Report>(())
                    })?;

                    match self.new_conn_listener.try_recv() {
                        Ok(addr) => {
                            debug!(?addr, "DpdkInlineReqChunnel found first connection");
                            self.conn_count += 1;
                            let cn = DpdkInlineCn::new(
                                self.listen_addr.port(),
                                Some(addr),
                                Some(self.new_conn_notifier.clone()),
                                Arc::clone(&self.initialization_state),
                                None,
                                Arc::clone(&self.shutdown),
                            )
                            .with_closed_notification(self.conn_closed_notifier.clone());
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
                debug!(
                    conn_count = ?self.conn_count,
                    "DpdkInlineReqChunnel listen stream wait for connection signal"
                );
                // When a connection closes, it lets us know so we can check whether it was the
                // last one, and thus we have to search for a connection ourselves.
                use futures_util::future::Either;
                let ret = match futures_util::future::select(
                    self.new_conn_listener.recv_async(),
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
                                Some(self.new_conn_notifier.clone()),
                                Arc::clone(&self.initialization_state),
                                None,
                                Arc::clone(&self.shutdown),
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
