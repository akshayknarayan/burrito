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
    eyre::{ensure, eyre, WrapErr},
    Report,
};
use eui48::MacAddress;
use flume::Sender;
use futures_util::{
    future::{ready, Ready},
    stream::{once, Once, Stream, TryStreamExt},
};
use std::task::Waker;
use std::{
    cell::{RefCell, UnsafeCell},
    fmt::Debug,
    net::{Ipv4Addr, SocketAddr},
    path::Path,
    pin::Pin,
    str::FromStr,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    sync::{Arc, Barrier, Mutex, RwLock},
};
use tracing::debug;

mod migrator;
pub use migrator::*;

mod connection;
pub use connection::*;

mod stream;
use stream::*;

#[derive(Clone, Debug)]
pub(crate) enum DatapathInner {
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

impl FromStr for DpdkDatapathChoice {
    type Err = Report;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let l = s.to_lowercase();
        match l.chars().next().ok_or(eyre!("got empty string"))? {
            't' => Ok(DpdkDatapathChoice::Thread),
            'i' => {
                let parts: Vec<&str> = l.split(':').collect();
                if parts.len() == 1 {
                    Ok(DpdkDatapathChoice::Inline { num_threads: 0 })
                } else if parts.len() == 2 {
                    let num_threads = parts[1].parse()?;
                    Ok(DpdkDatapathChoice::Inline { num_threads })
                } else {
                    Err(eyre!("unknown specifier {:?}", s))
                }
            }
            x => Err(eyre!("unknown specifier {:?}", x)),
        }
    }
}

std::thread_local! {
    static THIS_THREAD_ID: RefCell<Option<usize>> = RefCell::new(None);
}

fn update_thread_idx_and_count(
    barrier_count: &Arc<AtomicUsize>,
    swap_barrier: &Arc<RwLock<Barrier>>,
) {
    THIS_THREAD_ID.with(|is_active| {
        let mut a = is_active.borrow_mut();
        if a.is_none() {
            let cnt = barrier_count.fetch_add(1, Ordering::SeqCst);
            *a = Some(cnt);
            let mut sb = swap_barrier.write().unwrap();
            *sb = Barrier::new(cnt);
        }
    });
}

fn try_get_thread_id() -> Option<usize> {
    THIS_THREAD_ID.with(|active| *active.borrow())
}

fn store_thread_waker(tw: &Arc<Mutex<Vec<Option<Waker>>>>, waker: Waker) {
    // store a waker so we can wake this task when we want to do a swap.
    let thread_idx = try_get_thread_id().expect("thread not initialized");
    let mut wakers_g = tw.lock().unwrap();
    while wakers_g.len() <= thread_idx {
        wakers_g.push(None);
    }

    wakers_g[thread_idx] = Some(waker);
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
    conns: Arc<Mutex<HashMap<ActiveConnection, Sender<DatapathCnInner>>>>,
    wait_for_datapath_swap_now: Arc<AtomicBool>,
    barrier_count: Arc<AtomicUsize>,
    swap_barrier: Arc<RwLock<Barrier>>,
    swap_wakers: Arc<Mutex<Vec<Option<Waker>>>>,
}

impl Debug for DpdkDatapath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DpdkDatapath")
            .field("addr", &self.ip_addr)
            .field("datapath", &self.curr_datapath)
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
            swap_wakers: Default::default(),
        })
    }

    /// Return `true` if we need to perform a transition and `false` otherwise.
    fn check_do_transition(&self, choice: DpdkDatapathChoice) -> bool {
        match (/* from */ &self.curr_datapath, /* to */ choice) {
            (
                /* from */ DatapathInner::Inline(_),
                /* to */ DpdkDatapathChoice::Thread,
            )
            | (
                /* from */ DatapathInner::Thread(_),
                /* to */ DpdkDatapathChoice::Inline { .. },
            ) => true,
            _ => false,
        }
    }

    /// Start the critical section.
    ///
    /// safety postcondition: any thread with send/recv operations is currently waiting in
    /// `maybe_swap_datapath` on a channel recv from us, and is thus *not* accessing DPDK state.
    /// we are about to tear down and replace that DPDK state, so it is important no one is using it.
    ///
    /// what about the *current thread*? It is not sending or receiving (since it is doing
    /// this), but on the next send/recv operation it needs to perform the swap we are about to
    /// send it via the channel.
    fn wait_trigger_synchronize(&self) {
        debug!("synchronizing now");
        // we must *immediately* set this AtomicBool so that any send/recv calls on other threads
        // that occur become synchronized on the setup happening on this thread.
        self.wait_for_datapath_swap_now
            .store(true, Ordering::SeqCst);
        for o_w in self.swap_wakers.lock().unwrap().drain(..) {
            if let Some(w) = o_w {
                w.wake();
            }
        }

        self.swap_barrier.read().unwrap().wait();
        debug!("synchronized");
    }

    fn transition_complete(&self) {
        self.wait_for_datapath_swap_now
            .store(false, Ordering::SeqCst);
        debug!("done");
    }

    pub fn trigger_transition(&mut self, choice: DpdkDatapathChoice) -> Result<(), Report> {
        if !self.check_do_transition(choice) {
            return Ok(());
        }

        let _transition_span_g = tracing::debug_span!("datapath_transition", ?choice).entered();
        self.wait_trigger_synchronize();
        // begin critical synchronized section. Other threads will exit the critical section as we
        // send() `new_ch`s to them.
        self.curr_datapath.shut_down()?;

        let conns_g = self.conns.lock().unwrap();
        let conns = conns_g.keys().copied().collect();
        debug!(?conns, "current connections");

        match choice {
            DpdkDatapathChoice::Thread => {
                let mut new_ch =
                    DpdkUdpSkChunnel::new_preconfig(self.ip_addr, self.arp_table.clone())
                        .wrap_err("Initializing dpdk-thread chunnel")?;
                let mut new_conns = new_ch.load_connections(conns)?;
                self.curr_datapath = DatapathInner::Thread(new_ch);

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
            }
            DpdkDatapathChoice::Inline { num_threads } => {
                let mut new_ch = DpdkInlineChunnel::new_preconfig(
                    self.ip_addr,
                    self.arp_table.clone(),
                    num_threads,
                )
                .wrap_err("initializing dpdk-inline chunnel")?;
                // TODO this is wrong. it will load all the connections on the local thread, but
                // what we want to do is distribute the new connections to the threads on which
                // they already live.
                let mut new_conns = new_ch.load_connections(conns)?;
                self.curr_datapath = DatapathInner::Inline(new_ch);

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
            }
        }

        // critical section over.
        self.transition_complete();
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
            this.conns.lock().unwrap().insert(
                ActiveConnection::UnConnected {
                    local_port: inner.local_port(),
                },
                s,
            );

            update_thread_idx_and_count(&this.barrier_count, &this.swap_barrier);
            Ok(DatapathCn {
                inner: UnsafeCell::new(inner),
                wait_for_datapath_swap_now: Arc::clone(&this.wait_for_datapath_swap_now),
                new_datapath: r,
                swap_barrier: Arc::clone(&this.swap_barrier),
                thread_wakers: Arc::clone(&this.swap_wakers),
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
            this.conns.lock().unwrap().insert(
                ActiveConnection::UnConnected {
                    local_port: inner.local_port(),
                },
                s,
            );

            update_thread_idx_and_count(&this.barrier_count, &this.swap_barrier);
            Ok(DatapathCn {
                inner: UnsafeCell::new(inner),
                wait_for_datapath_swap_now: Arc::clone(&this.wait_for_datapath_swap_now),
                new_datapath: r,
                swap_barrier: Arc::clone(&this.swap_barrier),
                thread_wakers: Arc::clone(&this.swap_wakers),
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

#[derive(Clone)]
pub struct DpdkReqDatapath {
    inner: DpdkDatapath,
    acceptors: Arc<Mutex<HashMap<u16, Vec<Sender<DatapathInner>>>>>,
}

impl Debug for DpdkReqDatapath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("DpdkReqDatapath").field(&self.inner).finish()
    }
}

impl From<DpdkDatapath> for DpdkReqDatapath {
    fn from(inner: DpdkDatapath) -> Self {
        DpdkReqDatapath {
            inner,
            acceptors: Default::default(),
        }
    }
}

impl DpdkReqDatapath {
    pub fn trigger_transition(&mut self, choice: DpdkDatapathChoice) -> Result<(), Report> {
        self.inner.trigger_transition(choice)?;
        for (_, senders) in self.acceptors.lock().unwrap().iter() {
            for s in senders {
                s.send(self.inner.curr_datapath.clone())?;
            }
        }

        Ok(())
    }

    fn adapt_inner_stream(
        st: Pin<Box<dyn Stream<Item = Result<DatapathCnInner, Report>> + Send + Sync + 'static>>,
        conns: Arc<Mutex<HashMap<ActiveConnection, Sender<DatapathCnInner>>>>,
        wait_for_datapath_swap_now: Arc<AtomicBool>,
        swap_barrier: Arc<RwLock<Barrier>>,
        thread_wakers: Arc<Mutex<Vec<Option<Waker>>>>,
    ) -> ReqDatapathStream {
        Box::pin(st.map_ok(move |cn| {
            let ws = Arc::clone(&wait_for_datapath_swap_now);
            let sb = Arc::clone(&swap_barrier);
            let tw = Arc::clone(&thread_wakers);

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
                new_datapath: r,
                thread_wakers: tw,
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
            > = match &this.inner.curr_datapath {
                DatapathInner::Thread(ref s) => Box::pin(
                    DpdkUdpReqChunnel(s.clone())
                        .listen(addr)
                        .into_inner()?
                        .map_ok(Either::Right)
                        .map_ok(DatapathCnInner::Thread),
                ) as _,
                DatapathInner::Inline(ref s) => Box::pin(
                    DpdkInlineReqChunnel::from(s.clone())
                        .listen(addr)
                        .into_inner()?
                        .map_ok(DatapathCnInner::Inline),
                ) as _,
            };

            let st = DpdkReqDatapath::adapt_inner_stream(
                inner,
                this.inner.conns.clone(),
                this.inner.wait_for_datapath_swap_now.clone(),
                this.inner.swap_barrier.clone(),
                this.inner.swap_wakers.clone(),
            );

            // stream updater
            let (s, r) = flume::bounded(1);
            this.acceptors
                .lock()
                .unwrap()
                .entry(addr.port())
                .or_default()
                .push(s);
            update_thread_idx_and_count(&this.inner.barrier_count, &this.inner.swap_barrier);
            Ok(UpgradeStream::new(
                addr.port(),
                st,
                r,
                this.inner.conns.clone(),
                Arc::clone(&this.inner.wait_for_datapath_swap_now),
                Arc::clone(&this.inner.swap_barrier),
                Arc::clone(&this.inner.swap_wakers),
            ))
        }

        ready(try_listen(self, addr))
    }
}
