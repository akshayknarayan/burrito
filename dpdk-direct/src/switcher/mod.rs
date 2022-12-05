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
    stream::{once, FuturesUnordered, Once, Stream, TryStreamExt},
    StreamExt,
};
use std::{
    fmt::Debug,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::Path,
    pin::Pin,
    str::FromStr,
    sync::{Arc, Mutex as StdMutex},
};
use tokio::sync::Mutex;
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
    conns: Arc<StdMutex<HashMap<ActiveConnection, (Arc<Mutex<DatapathCnInner>>, Sender<()>)>>>,
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

    pub async fn trigger_transition(&mut self, choice: DpdkDatapathChoice) -> Result<(), Report> {
        if !self.check_do_transition(choice) {
            return Ok(());
        }

        let _transition_span_g = tracing::debug_span!("datapath_transition", ?choice).entered();

        self.curr_datapath.shut_down()?;

        let conns_g = self.conns.lock().unwrap();
        let conns = conns_g.keys().copied().collect();
        debug!(?conns, "current connections");
        let conn_handles_futs: futures_util::stream::FuturesUnordered<_> = conns_g
            .iter()
            .map(|(c, (cn_mutex, preemptor))| async move {
                // TODO XXX race condition on lock vs re-lock after the send.
                // the re-lock case deadlocks.
                let (send_res, g) = futures_util::join!(preemptor.send_async(()), cn_mutex.lock());
                send_res.unwrap();
                (c, g)
            })
            .collect();
        let mut conn_handles: HashMap<_, _> = conn_handles_futs.collect().await;
        match choice {
            DpdkDatapathChoice::Thread => {
                let mut new_ch =
                    DpdkUdpSkChunnel::new_preconfig(self.ip_addr, self.arp_table.clone())
                        .wrap_err("Initializing dpdk-thread chunnel")?;
                let new_conns = new_ch.load_connections(conns)?;
                self.curr_datapath = DatapathInner::Thread(new_ch);

                // now need to replace the active connections in our set of active connections.
                for (conn_desc, new_conn) in new_conns {
                    let mut guard = conn_handles
                        .remove(&conn_desc)
                        .ok_or(eyre!("Did not find connection for {:?}", conn_desc))?;
                    *guard = DatapathCnInner::Thread(new_conn);
                }

                ensure!(
                    conn_handles.is_empty(),
                    "Did not find existing connections for {:?}",
                    conn_handles.keys().copied().collect::<Vec<_>>(),
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
                let new_conns = new_ch.load_connections(conns)?;
                self.curr_datapath = DatapathInner::Inline(new_ch);

                for (conn_desc, new_conn) in new_conns {
                    let mut guard = conn_handles
                        .remove(&conn_desc)
                        .ok_or(eyre!("Did not find connection for {:?}", conn_desc))?;
                    *guard = DatapathCnInner::Inline(new_conn);
                }

                ensure!(
                    conn_handles.is_empty(),
                    "Did not find existing connections for {:?}",
                    conn_handles.keys().copied().collect::<Vec<_>>(),
                );
            }
        }

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

            let local_port = inner.local_port();
            let remote_addr = inner.remote_addr();

            let inner = Arc::new(Mutex::new(inner));

            let (s, r) = flume::bounded(1);
            this.conns.lock().unwrap().insert(
                ActiveConnection::UnConnected { local_port },
                (inner.clone(), s),
            );
            Ok(DatapathCn {
                inner,
                new_datapath: r,
                local_port,
                remote_addr,
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

            let local_port = inner.local_port();
            let remote_addr = inner.remote_addr();
            let inner = Arc::new(Mutex::new(inner));

            let (s, r) = flume::bounded(1);
            this.conns.lock().unwrap().insert(
                ActiveConnection::UnConnected { local_port },
                (inner.clone(), s),
            );
            Ok(DatapathCn {
                inner,
                new_datapath: r,
                local_port,
                remote_addr,
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
    acceptors: Arc<StdMutex<HashMap<u16, Vec<(Sender<()>, Arc<Mutex<ReqDatapathStream>>)>>>>,
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
    pub async fn trigger_transition(&mut self, choice: DpdkDatapathChoice) -> Result<(), Report> {
        let acceptors_g = self.acceptors.lock().unwrap();
        let update_guards_futs = FuturesUnordered::new();
        for (p, senders) in acceptors_g.iter() {
            for (s, mux) in senders {
                update_guards_futs.push(async {
                    let (send_res, g) = futures_util::join!(s.send_async(()), mux.lock());
                    send_res.unwrap();
                    (*p, g)
                });
            }
        }
        debug!("locking streams");
        let update_guards: Vec<_> = update_guards_futs.collect().await;
        debug!("locked streams");
        self.inner.trigger_transition(choice).await?;
        for (port, mut g) in update_guards {
            debug!(?self.inner.curr_datapath, "performing datapath stream swap");
            // now we construct a new stream.
            let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port));
            let inner: Pin<
                Box<dyn Stream<Item = Result<DatapathCnInner, Report>> + Send + Sync + 'static>,
            > = match self.inner.curr_datapath {
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

            let st = DpdkReqDatapath::adapt_inner_stream(inner, self.inner.conns.clone());
            *g = st;
            debug!("datapath stream swap done");
        }

        Ok(())
    }

    fn adapt_inner_stream(
        st: Pin<Box<dyn Stream<Item = Result<DatapathCnInner, Report>> + Send + Sync + 'static>>,
        conns: Arc<StdMutex<HashMap<ActiveConnection, (Arc<Mutex<DatapathCnInner>>, Sender<()>)>>>,
    ) -> ReqDatapathStream {
        Box::pin(st.map_ok(move |cn| {
            let local_port = cn.local_port();
            let remote_addr = cn.remote_addr();

            let cn = Arc::new(Mutex::new(cn));
            // conn updater
            let (s, r) = flume::bounded(1);
            conns.lock().unwrap().insert(
                ActiveConnection::Connected {
                    local_port,
                    remote_addr: remote_addr
                        .expect("expected remote address for accept-style connection"),
                },
                (cn.clone(), s),
            );

            DatapathCn {
                inner: cn,
                new_datapath: r,
                local_port,
                remote_addr,
            }
        }))
    }
}

impl ChunnelListener for DpdkReqDatapath {
    type Future = Ready<Result<Self::Stream, Self::Error>>;
    type Addr = SocketAddr;
    type Connection = DatapathCn;
    type Error = Report;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn listen(&mut self, addr: Self::Addr) -> Self::Future {
        fn try_listen(
            this: &mut DpdkReqDatapath,
            addr: SocketAddr,
        ) -> Result<Pin<Box<dyn Stream<Item = Result<DatapathCn, Report>> + Send + 'static>>, Report>
        {
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

            let st = Arc::new(Mutex::new(DpdkReqDatapath::adapt_inner_stream(
                inner,
                this.inner.conns.clone(),
            )));

            // stream updater
            let (s, r) = flume::bounded(1);
            this.acceptors
                .lock()
                .unwrap()
                .entry(addr.port())
                .or_default()
                .push((s, st.clone()));
            let us = UpgradeStream::new(st, r);
            Ok(Box::pin(futures_util::stream::try_unfold(
                us,
                UpgradeStream::next,
            )))
        }

        ready(try_listen(self, addr))
    }
}
