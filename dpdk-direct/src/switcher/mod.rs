//! Dynamically switch between `dpdk_direct::DpdkUdpReqChunnel` and
//! `dpdk_direct::DpdkInlineReqChunnel`.
//!
//! kvserver::server::serve is expecting:
//! ```
//!    mut raw_listener: impl ChunnelListener<
//!            Addr = SocketAddr,
//!            Connection = impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
//!            Error = impl Into<Report> + Send + Sync + 'static,
//!        > + Clone
//!        + Send
//!        + 'static,
//! ```
//! we need to produce something that wraps both these functions:
//! ```
//! fn run_server_dpdk_singlethread(opt: Opt) -> Result<(), Report> {
//!     let s = dpdk_direct::DpdkUdpSkChunnel::new(&opt.cfg.unwrap())?;
//!     let l = dpdk_direct::DpdkUdpReqChunnel(s);
//!     serve( l, /* ... */)
//! }
//!
//! fn run_server_dpdk_multithread(opt: Opt) -> Result<(), Report> {
//!     let s = dpdk_direct::DpdkInlineChunnel::new(opt.cfg.unwrap(), (opt.num_shards + 1) as _)?;
//!     let l = dpdk_direct::DpdkInlineReqChunnel::from(s);
//!     serve( l, /* ... */)
//! }
//! ```
//! in a single implementation of that interface. It should also support dynamic switching between
//! the two via a handle.

use crate::{DpdkInlineChunnel, DpdkInlineReqChunnel, DpdkUdpReqChunnel, DpdkUdpSkChunnel};
use ahash::HashMap;
use bertha::{ChunnelConnector, ChunnelListener, Either};
use color_eyre::{
    eyre::{ensure, eyre},
    Report,
};
use eui48::MacAddress;
use flume::{Receiver, Sender};
use futures_util::{
    future::{ready, Ready},
    stream::{once, Once, Stream, TryStreamExt},
};
use std::{
    cell::{RefCell, UnsafeCell},
    fmt::Debug,
    net::{Ipv4Addr, SocketAddr},
    path::Path,
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    sync::{Arc, Barrier, Mutex, RwLock},
    task::{Context, Poll},
};

mod migrator;
pub use migrator::*;

mod connection;
pub use connection::*;

#[derive(Clone, Debug)]
enum DatapathInner {
    Thread(DpdkUdpSkChunnel),
    Inline(DpdkInlineChunnel),
}

impl DatapathInner {
    fn shut_down(&mut self) -> Result<(), Report> {
        match self {
            DatapathInner::Thread(t) => t.shut_down(),
            DatapathInner::Inline(t) => t.shut_down(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum DpdkDatapathChoice {
    Thread,
    Inline { num_threads: usize },
}

std::thread_local! {
    static THIS_THREAD_ACTIVE: RefCell<bool> = RefCell::new(false);
}

/// A base-level chunnel able to dynamically switch between dpdk-thread and dpdk-inline datapaths.
///
/// To trigger a switch, call `trigger_transition`. `DpdkDatapath` can be cloned to enable later calling
/// `trigger_transition`.
#[derive(Clone)]
pub struct DpdkDatapath {
    ip_addr: Ipv4Addr,
    arp_table: HashMap<Ipv4Addr, MacAddress>,

    curr_datapath: DatapathInner,
    // need to keep track of connections so we can send updated `DatapathCnInner`s.
    conns: HashMap<ActiveConnection, Sender<DatapathCnInner>>,
    wait_for_datapath_swap_now: Arc<AtomicBool>,
    barrier_count: Arc<AtomicUsize>,
    swap_barrier: Arc<RwLock<Barrier>>,
}

impl Debug for DpdkDatapath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DpdkDatapath")
            .field("addr", &self.ip_addr)
            .field("datapath", &self.curr_datapath)
            .field("swap_now", &self.wait_for_datapath_swap_now)
            .finish()
    }
}

impl DpdkDatapath {
    pub fn new(config_path: impl AsRef<Path>, init_dp: DpdkDatapathChoice) -> Result<Self, Report> {
        let (curr_datapath, ip_addr, arp_table) = match init_dp {
            DpdkDatapathChoice::Thread => {
                let ch = DpdkUdpSkChunnel::new(config_path)?;
                let (ip_addr, arp_table) = ch.get_cfg();
                (DatapathInner::Thread(ch), ip_addr, arp_table)
            }
            DpdkDatapathChoice::Inline { num_threads } => {
                let ch = DpdkInlineChunnel::new(config_path.as_ref().to_path_buf(), num_threads)?;
                let (ip_addr, arp_table) = ch.get_cfg();
                (DatapathInner::Inline(ch), ip_addr, arp_table)
            }
        };

        Ok(DpdkDatapath {
            ip_addr,
            arp_table,
            curr_datapath,
            conns: Default::default(),
            wait_for_datapath_swap_now: Default::default(),
            barrier_count: Arc::new(1usize.into()),
            swap_barrier: Arc::new(RwLock::new(Barrier::new(1))),
        })
    }

    /// 1. Get the active connections from OldDatapath
    /// (downtime starts)
    /// 2. Tear down OldDatapath.
    /// 3. Start up NewDatapath
    /// 4. Load connection state from (1) into NewDatapath
    /// (downtime ends)
    pub fn trigger_transition(&mut self, choice: DpdkDatapathChoice) -> Result<(), Report> {
        match (/* from */ &self.curr_datapath, /* to */ choice) {
            (
                /* from */ DatapathInner::Inline(_),
                /* to */ DpdkDatapathChoice::Thread,
            ) => (),
            (
                /* from */ DatapathInner::Thread(_),
                /* to */ DpdkDatapathChoice::Inline { .. },
            ) => (),
            _ => return Ok(()),
        };

        let conns = self.conns.keys().copied().collect();
        self.curr_datapath.shut_down()?;

        // we can ignore accept_streams here because we have not implemented accept() style
        // connections, so there will be no streams.
        match choice {
            DpdkDatapathChoice::Thread => {
                let mut new_ch =
                    DpdkUdpSkChunnel::new_preconfig(self.ip_addr, self.arp_table.clone())?;
                let LoadedConnections {
                    conns: mut new_conns,
                    ..
                } = new_ch.load_connections(conns)?;

                // now need to replace the active connections in our set of active connections.
                for (conn_desc, sender) in &self.conns {
                    let new_conn = new_conns
                        .remove(conn_desc)
                        .ok_or(eyre!("Did not find new connection for {:?}", conn_desc))?;
                    sender.send(DatapathCnInner::Thread(new_conn))?;
                }

                ensure!(
                    new_conns.is_empty(),
                    "Did not find existing connections for {:?}",
                    new_conns
                );
            }
            DpdkDatapathChoice::Inline { num_threads } => {
                let mut new_ch = DpdkInlineChunnel::new_preconfig(
                    self.ip_addr,
                    self.arp_table.clone(),
                    num_threads,
                )?;
                let LoadedConnections {
                    conns: mut new_conns,
                    ..
                } = new_ch.load_connections(conns)?;

                for (conn_desc, sender) in &self.conns {
                    let new_conn = new_conns
                        .remove(conn_desc)
                        .ok_or(eyre!("Did not find new connection for {:?}", conn_desc))?;
                    sender.send(DatapathCnInner::Inline(new_conn))?;
                }

                ensure!(
                    new_conns.is_empty(),
                    "Did not find existing connections for {:?}",
                    new_conns
                );
            }
        }

        // we have sent on all the senders. we can now set this `AtomicBool` to true, so that once we
        // return, all connections will check it (downtime starts at this point), receive on the
        // channels, replace their underlying connections, (downtime ends) then use them for
        // operations .
        self.wait_for_datapath_swap_now
            .store(true, Ordering::SeqCst);
        Ok(())
    }
}

impl ChunnelConnector for DpdkDatapath {
    type Addr = SocketAddr;
    type Connection = DatapathCn;
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Error = Report;

    fn connect(&mut self, a: Self::Addr) -> Self::Future {
        fn try_connect(this: &mut DpdkDatapath, a: SocketAddr) -> Result<DatapathCn, Report> {
            let inner = match &mut this.curr_datapath {
                DatapathInner::Thread(ref mut s) => {
                    DatapathCnInner::Thread(Either::Left(s.connect(a).into_inner()?))
                }
                DatapathInner::Inline(ref mut s) => {
                    DatapathCnInner::Inline(s.connect(a).into_inner()?)
                }
            };

            let (s, r) = flume::bounded(1);
            this.conns.insert(
                ActiveConnection::UnConnected {
                    local_port: inner.local_port(),
                },
                s,
            );

            Ok(DatapathCn {
                inner: UnsafeCell::new(inner),
                wait_for_datapath_swap_now: Arc::clone(&this.wait_for_datapath_swap_now),
                new_datapath: r,
                barrier_cnt: Arc::clone(&this.barrier_count),
                swap_barrier: Arc::clone(&this.swap_barrier),
            })
        }

        ready(try_connect(self, a))
    }
}

impl ChunnelListener for DpdkDatapath {
    type Future = Ready<Result<Self::Stream, Self::Error>>;
    type Addr = SocketAddr;
    type Connection = DatapathCn;
    type Error = Report;
    type Stream = Once<Ready<Result<Self::Connection, Self::Error>>>;

    fn listen(&mut self, addr: Self::Addr) -> Self::Future {
        fn try_listen(this: &mut DpdkDatapath, addr: SocketAddr) -> Result<DatapathCn, Report> {
            let inner = match &mut this.curr_datapath {
                DatapathInner::Thread(ref mut s) => {
                    DatapathCnInner::Thread(Either::Left(s.do_listen_non_accept(addr)?))
                }
                DatapathInner::Inline(ref mut s) => {
                    DatapathCnInner::Inline(s.do_listen_non_accept(addr)?)
                }
            };

            let (s, r) = flume::bounded(1);
            this.conns.insert(
                ActiveConnection::UnConnected {
                    local_port: inner.local_port(),
                },
                s,
            );

            Ok(DatapathCn {
                inner: UnsafeCell::new(inner),
                wait_for_datapath_swap_now: Arc::clone(&this.wait_for_datapath_swap_now),
                new_datapath: r,
                barrier_cnt: Arc::clone(&this.barrier_count),
                swap_barrier: Arc::clone(&this.swap_barrier),
            })
        }

        let res = try_listen(self, addr);
        match res {
            Ok(r) => ready(Ok(once(ready(Ok(r))))),
            Err(e) => ready(Err(e)),
        }
    }
}

type ReqDatapathStream =
    Pin<Box<dyn Stream<Item = Result<DatapathCn, Report>> + Send + Sync + 'static>>;

#[derive(Debug)]
pub struct DpdkReqDatapath {
    inner: DpdkDatapath,
    conns: Arc<Mutex<HashMap<ActiveConnection, Sender<DatapathCnInner>>>>,
    acceptors: HashMap<u16, Sender<ReqDatapathStream>>,
}

impl From<DpdkDatapath> for DpdkReqDatapath {
    fn from(inner: DpdkDatapath) -> Self {
        DpdkReqDatapath {
            inner,
            conns: Default::default(),
            acceptors: Default::default(),
        }
    }
}

impl DpdkReqDatapath {
    pub fn trigger_transition(&mut self, choice: DpdkDatapathChoice) -> Result<(), Report> {
        match (
            /* from */ &self.inner.curr_datapath,
            /* to */ choice,
        ) {
            (
                /* from */ DatapathInner::Inline(_),
                /* to */ DpdkDatapathChoice::Thread,
            ) => (),
            (
                /* from */ DatapathInner::Thread(_),
                /* to */ DpdkDatapathChoice::Inline { .. },
            ) => (),
            _ => return Ok(()),
        };

        let conns_g = self.conns.lock().unwrap();

        let conns = conns_g.keys().copied().collect();
        self.inner.curr_datapath.shut_down()?;

        match choice {
            DpdkDatapathChoice::Thread => {
                let mut new_ch = DpdkUdpReqChunnel(DpdkUdpSkChunnel::new_preconfig(
                    self.inner.ip_addr,
                    self.inner.arp_table.clone(),
                )?);
                let LoadedConnections {
                    conns: mut new_conns,
                    mut accept_streams,
                } = new_ch.load_connections(conns)?;

                // now need to replace the active connections in our set of active connections.
                for (conn_desc, sender) in conns_g.iter() {
                    let new_conn = new_conns
                        .remove(conn_desc)
                        .ok_or(eyre!("Did not find new connection for {:?}", conn_desc))?;
                    sender.send(DatapathCnInner::Thread(new_conn))?;
                }

                ensure!(
                    new_conns.is_empty(),
                    "Did not find existing connections for {:?}",
                    new_conns
                );

                std::mem::drop(conns_g);

                // accept_streams
                for (acc_port, stream) in accept_streams.drain() {
                    let st = Box::pin(stream.map_ok(DatapathCnInner::Thread)) as _;
                    let st = self.adapt_inner_stream(st);
                    self.acceptors
                        .get(&acc_port)
                        .ok_or(eyre!("Did not find accept stream for {:?}", acc_port))?
                        .send(st)?;
                }
            }
            DpdkDatapathChoice::Inline { num_threads } => {
                let mut new_ch = DpdkInlineChunnel::new_preconfig(
                    self.inner.ip_addr,
                    self.inner.arp_table.clone(),
                    num_threads,
                )?;
                let LoadedConnections {
                    conns: mut new_conns,
                    mut accept_streams,
                } = new_ch.load_connections(conns)?;

                for (conn_desc, sender) in conns_g.iter() {
                    let new_conn = new_conns
                        .remove(conn_desc)
                        .ok_or(eyre!("Did not find new connection for {:?}", conn_desc))?;
                    sender.send(DatapathCnInner::Inline(new_conn))?;
                }

                ensure!(
                    new_conns.is_empty(),
                    "Did not find existing connections for {:?}",
                    new_conns
                );

                std::mem::drop(conns_g);

                // accept_streams
                for (acc_port, stream) in accept_streams.drain() {
                    let st = Box::pin(stream.map_ok(DatapathCnInner::Inline)) as _;
                    let st = self.adapt_inner_stream(st);
                    self.acceptors
                        .get(&acc_port)
                        .ok_or(eyre!("Did not find accept stream for {:?}", acc_port))?
                        .send(st)?;
                }
            }
        }

        // we have sent on all the senders. we can now set this `AtomicBool` to true, so that once we
        // return, all connections will check it (downtime starts at this point), receive on the
        // channels, replace their underlying connections, (downtime ends) then use them for
        // operations .
        self.inner
            .wait_for_datapath_swap_now
            .store(true, Ordering::SeqCst);
        Ok(())
    }

    fn adapt_inner_stream(
        &mut self,
        st: Pin<Box<dyn Stream<Item = Result<DatapathCnInner, Report>> + Send + Sync + 'static>>,
    ) -> ReqDatapathStream {
        // per-item shared state
        let conns = Arc::clone(&self.conns);
        let wait_swap = Arc::clone(&self.inner.wait_for_datapath_swap_now);
        let swap_barrier = Arc::clone(&self.inner.swap_barrier);
        let barrier_cnt = Arc::clone(&self.inner.barrier_count);

        Box::pin(st.map_ok(move |cn| {
            let ws = Arc::clone(&wait_swap);
            let sb = Arc::clone(&swap_barrier);
            let bc = Arc::clone(&barrier_cnt);

            // conn updater
            let (s, r) = flume::bounded(1);
            conns.lock().unwrap().insert(
                ActiveConnection::Connected {
                    local_port: cn.local_port(),
                    remote_addr: cn
                        .remote_addr()
                        .expect("expected remote address for accept-style connection"),
                },
                s,
            );

            DatapathCn {
                inner: UnsafeCell::new(cn),
                wait_for_datapath_swap_now: ws,
                swap_barrier: sb,
                barrier_cnt: bc,
                new_datapath: r,
            }
        }))
    }
}

impl ChunnelListener for DpdkReqDatapath {
    type Future = Ready<Result<Self::Stream, Self::Error>>;
    type Addr = SocketAddr;
    type Connection = DatapathCn;
    type Error = Report;
    type Stream = UpgradeStream;

    fn listen(&mut self, addr: Self::Addr) -> Self::Future {
        fn try_listen(
            this: &mut DpdkReqDatapath,
            addr: SocketAddr,
        ) -> Result<UpgradeStream, Report> {
            let inner: Pin<
                Box<dyn Stream<Item = Result<DatapathCnInner, Report>> + Send + Sync + 'static>,
            > = match &mut this.inner.curr_datapath {
                DatapathInner::Thread(ref mut s) => Box::pin(
                    DpdkUdpReqChunnel(s.clone())
                        .listen(addr)
                        .into_inner()?
                        .map_ok(Either::Right)
                        .map_ok(DatapathCnInner::Thread),
                ) as _,
                DatapathInner::Inline(ref mut s) => Box::pin(
                    DpdkInlineReqChunnel::from(s.clone())
                        .listen(addr)
                        .into_inner()?
                        .map_ok(DatapathCnInner::Inline),
                ) as _,
            };

            let st = this.adapt_inner_stream(inner);

            // stream updater
            let (s, r) = flume::bounded(1);
            this.acceptors.insert(addr.port(), s);

            Ok(UpgradeStream {
                inner: st,
                updates: r,
                wait_for_datapath_swap_now: Arc::clone(&this.inner.wait_for_datapath_swap_now),
                swap_barrier: Arc::clone(&this.inner.swap_barrier),
            })
        }

        ready(try_listen(self, addr))
    }
}

pub struct UpgradeStream {
    inner: ReqDatapathStream,
    updates: Receiver<ReqDatapathStream>,
    wait_for_datapath_swap_now: Arc<AtomicBool>,
    swap_barrier: Arc<RwLock<Barrier>>,
}

impl UpgradeStream {
    fn maybe_swap_datapath(&mut self) {
        if self.wait_for_datapath_swap_now.load(Ordering::Relaxed) {
            let new_dp = self
                .updates
                .recv()
                .expect("datapath update sender disappeared");
            let res = self.swap_barrier.read().unwrap().wait();
            self.inner = new_dp;
            if res.is_leader() {
                self.wait_for_datapath_swap_now
                    .store(false, Ordering::Relaxed);
            }
        } else {
            if let Ok(new_dp) = self.updates.try_recv() {
                self.inner = new_dp;
            }
        }
    }
}

impl Stream for UpgradeStream {
    type Item = <ReqDatapathStream as Stream>::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.maybe_swap_datapath();
        Pin::new(&mut self.inner).poll_next(cx)
    }
}
