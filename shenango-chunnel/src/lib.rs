//! Shenango-powered UDP chunnel.

use ahash::AHashMap as HashMap;
use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{eyre, Report, WrapErr};
use futures_util::{
    future::{ready, Ready},
    stream::Stream,
};
use std::future::Future;
use std::net::{SocketAddr, SocketAddrV4};
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};
use tracing::{debug, warn};

lazy_static::lazy_static! {
static ref SHENANGO_RUNTIME_SENDER: Arc<StdMutex<Option<flume::Sender<NewConn>>>> =
    Arc::new(StdMutex::new(None));
}

struct Msg {
    addr: SocketAddrV4,
    buf: Vec<u8>,
}

enum NewConn {
    Accept {
        addr: SocketAddrV4,
        outgoing_s: flume::Sender<Msg>,
        outgoing_r: flume::Receiver<Msg>,
        conns: flume::Sender<ShenangoUdpSk>,
    },
    Listen {
        addr: SocketAddrV4,
        incoming: flume::Sender<Msg>,
        outgoing: flume::Receiver<Msg>,
    },
    Dial {
        incoming: flume::Sender<Msg>,
        outgoing: flume::Receiver<Msg>,
    },
}
enum IncomingCh {
    Msg(flume::Sender<Msg>),
    Conn {
        outgoing: flume::Sender<Msg>,
        conns: flume::Sender<ShenangoUdpSk>,
    },
}

impl IncomingCh {
    fn recv_loop(self, laddr: SocketAddrV4, rsk: Arc<shenango::udp::UdpConnection>) {
        match self {
            Self::Msg(incoming) => {
                // receive
                let mut buf = [0u8; 2048];
                loop {
                    let (read_len, from_addr) = rsk
                        .read_from(&mut buf)
                        .wrap_err("shenango read_from")
                        .unwrap();
                    if incoming
                        .send(Msg {
                            addr: from_addr,
                            buf: buf[..read_len].to_vec(),
                        })
                        .is_err()
                    {
                        warn!(sk=?laddr, "Incoming channel dropped, recv thread exiting");
                        break;
                    }
                }
            }
            Self::Conn { outgoing, conns } => {
                let mut buf = [0u8; 2048];
                let mut remotes = HashMap::default();
                loop {
                    let (read_len, from_addr) = rsk
                        .read_from(&mut buf)
                        .wrap_err("shenango read_from")
                        .unwrap();

                    let mut new_conn_res = Ok(());
                    let msg_sender = remotes.entry(from_addr).or_insert_with(|| {
                        let (cn_s, cn_r) = flume::bounded(16);
                        new_conn_res = conns.send(ShenangoUdpSk {
                            outgoing: outgoing.clone(),
                            incoming: cn_r,
                        });
                        cn_s
                    });
                    new_conn_res.unwrap();

                    if let Err(send_err) = msg_sender.send(Msg {
                        addr: from_addr,
                        buf: buf[..read_len].to_vec(),
                    }) {
                        remotes.remove(&from_addr).unwrap();
                        debug!(sk=?laddr, ?from_addr, "Incoming channel dropped, resetting port");
                        let (cn_s, cn_r) = flume::bounded(16);
                        conns
                            .send(ShenangoUdpSk {
                                outgoing: outgoing.clone(),
                                incoming: cn_r,
                            })
                            .unwrap();
                        cn_s.send(send_err.into_inner()).unwrap(); // cannot fail since we just made cn_r
                        remotes.insert(from_addr, cn_s);
                    }
                }
            }
        }
    }
}

impl NewConn {
    fn start(self) {
        use NewConn::*;
        let (sk, incoming, outgoing) = match self {
            Accept {
                addr,
                conns,
                outgoing_s,
                outgoing_r,
            } => (
                Arc::new(
                    shenango::udp::UdpConnection::listen(addr)
                        .wrap_err("Failed to make shenango udp socket")
                        .expect("make udp conn"),
                ),
                IncomingCh::Conn {
                    outgoing: outgoing_s,
                    conns,
                },
                outgoing_r,
            ),
            Listen {
                addr,
                incoming,
                outgoing,
            } => (
                Arc::new(
                    shenango::udp::UdpConnection::listen(addr)
                        .wrap_err("Failed to make shenango udp socket")
                        .expect("make udp conn"),
                ),
                IncomingCh::Msg(incoming),
                outgoing,
            ),
            Dial { incoming, outgoing } => {
                let laddr = SocketAddrV4::new(std::net::Ipv4Addr::new(0, 0, 0, 0), 0);
                (
                    Arc::new(
                        shenango::udp::UdpConnection::listen(laddr)
                            .wrap_err("Failed to make shenango udp socket")
                            .expect("make udp conn"),
                    ),
                    IncomingCh::Msg(incoming),
                    outgoing,
                )
            }
        };

        let laddr = sk.local_addr();
        let rsk = Arc::clone(&sk);

        shenango::thread::spawn(move || {
            incoming.recv_loop(laddr, rsk);
        });

        // send
        shenango::thread::spawn(move || loop {
            match outgoing.try_recv() {
                Ok(Msg {
                    addr: a, buf: data, ..
                }) => {
                    sk.write_to(&data, a).wrap_err("shenango write_to").unwrap();
                }
                Err(flume::TryRecvError::Empty) => {
                    shenango::thread::thread_yield();
                }
                Err(flume::TryRecvError::Disconnected) => {
                    debug!(sk=?laddr, "send thread exiting");
                    break;
                }
            }
        });
    }
}

// everything here blocks.
// On success, this won't return while the ShenangoUdpSkChunnel still lives.
fn shenango_runtime_start(shenango_config: &Path) -> flume::Sender<NewConn> {
    let mut rt_guard = SHENANGO_RUNTIME_SENDER
        .lock()
        .expect("Shenango runtime lock acquisition failure is critical");
    if let Some(ref s) = *rt_guard {
        return s.clone();
    }

    let (s, conns) = flume::bounded(0);
    rt_guard.replace(s.clone());
    let cfg = shenango_config.to_str().unwrap().to_owned();
    std::thread::spawn(move || {
        shenango::runtime_init(cfg, move || loop {
            match conns.try_recv() {
                Ok(conn) => {
                    conn.start();
                }
                Err(flume::TryRecvError::Empty) => {
                    shenango::thread::thread_yield();
                }
                Err(flume::TryRecvError::Disconnected) => {
                    debug!("main thread exiting");
                    break;
                }
            }
        })
        .map_err(|i| eyre!("shenango runtime error: {:?}", i))
    });

    s
}

#[derive(Clone, Debug)]
pub struct ShenangoUdpSkChunnel {
    events: flume::Sender<NewConn>,
}

impl ShenangoUdpSkChunnel {
    pub fn new(config: impl AsRef<Path>) -> Self {
        let config = config.as_ref().to_path_buf();
        let s = shenango_runtime_start(&config);
        Self { events: s }
    }

    fn make_accept(&self, a: SocketAddrV4) -> Result<flume::Receiver<ShenangoUdpSk>, Report> {
        let (conns_s, conns_r) = flume::unbounded();
        let (outgoing_s, outgoing_r) = flume::unbounded();
        self.events.send(NewConn::Accept {
            addr: a,
            outgoing_r,
            outgoing_s,
            conns: conns_s,
        })?;
        Ok(conns_r)
    }

    fn make_listen(&self, a: SocketAddrV4) -> Result<ShenangoUdpSk, Report> {
        let (incoming_s, incoming_r) = flume::unbounded();
        let (outgoing_s, outgoing_r) = flume::unbounded();
        self.events.send(NewConn::Listen {
            addr: a,
            incoming: incoming_s,
            outgoing: outgoing_r,
        })?;
        Ok(ShenangoUdpSk::new(incoming_r, outgoing_s))
    }

    fn make_dial(&self) -> Result<ShenangoUdpSk, Report> {
        let (incoming_s, incoming_r) = flume::unbounded();
        let (outgoing_s, outgoing_r) = flume::unbounded();
        self.events.send(NewConn::Dial {
            incoming: incoming_s,
            outgoing: outgoing_r,
        })?;
        Ok(ShenangoUdpSk::new(incoming_r, outgoing_s))
    }
}

impl ChunnelListener for ShenangoUdpSkChunnel {
    type Addr = SocketAddr;
    type Connection = ShenangoUdpSk;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn listen(&mut self, addr: Self::Addr) -> Self::Future {
        use SocketAddr::*;
        match addr {
            V4(a) => {
                Box::pin(ready(self.make_listen(a).map(|sk| {
                    Box::pin(futures_util::stream::once(ready(Ok(sk)))) as _
                })))
            }
            V6(a) => Box::pin(ready(Err(eyre!("Only IPv4 is supported: {:?}", a)))),
        }
    }
}

impl ChunnelConnector for ShenangoUdpSkChunnel {
    type Addr = ();
    type Connection = ShenangoUdpSk;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn connect(&mut self, _a: Self::Addr) -> Self::Future {
        Box::pin(ready(self.make_dial()))
    }
}

#[derive(Clone)]
pub struct ShenangoUdpSk {
    outgoing: flume::Sender<Msg>,
    incoming: flume::Receiver<Msg>,
}

impl std::fmt::Debug for ShenangoUdpSk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShenangoUdpSk").finish()
    }
}

impl ShenangoUdpSk {
    fn new(incoming: flume::Receiver<Msg>, outgoing: flume::Sender<Msg>) -> Self {
        Self { outgoing, incoming }
    }
}

impl ChunnelConnection for ShenangoUdpSk {
    type Data = (SocketAddr, Vec<u8>);

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        use SocketAddr::*;
        for data in burst {
            match data {
                (V4(addr), d) => {
                    // unbounded channel will not block.
                    self.outgoing
                        .send(Msg { addr, buf: d })
                        .expect("shenango won't drop");
                }
                (V6(a), _) => {
                    return Box::pin(ready(Err(eyre!("Only IPv4 is supported: {:?}", a))))
                }
            }
        }

        Box::pin(ready(Ok(())))
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        Box::pin(async move {
            let Msg { addr: a, buf: d } = self
                .incoming
                .recv_async()
                .await
                .wrap_err(eyre!("All senders dropped"))?;
            msgs_buf[0] = Some((SocketAddr::V4(a), d));
            let mut slot_idx = 1;
            if slot_idx >= msgs_buf.len() {
                return Ok(msgs_buf);
            }

            while let Ok(Msg { addr: a, buf: d }) = self.incoming.try_recv() {
                msgs_buf[slot_idx] = Some((SocketAddr::V4(a), d));
                slot_idx += 1;

                if slot_idx >= msgs_buf.len() {
                    break;
                }
            }

            Ok(msgs_buf)
        })
    }
}

#[derive(Clone, Debug)]
pub struct ShenangoUdpReqChunnel(pub ShenangoUdpSkChunnel);

impl ChunnelListener for ShenangoUdpReqChunnel {
    type Addr = SocketAddr;
    type Connection = ShenangoUdpSk;
    type Future = Ready<Result<Self::Stream, Self::Error>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn listen(&mut self, addr: Self::Addr) -> Self::Future {
        use SocketAddr::*;
        match addr {
            V4(a) => {
                use futures_util::stream::StreamExt;
                let st = self.0.make_accept(a);
                ready(st.map(|s| Box::pin(s.into_stream().map(Ok)) as _))
            }
            V6(a) => ready(Err(eyre!("Only IPv4 is supported: {:?}", a))),
        }
    }
}
