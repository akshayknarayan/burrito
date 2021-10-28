//! Shenango-powered UDP chunnel.

use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{eyre, Report, WrapErr};
use futures_util::stream::Stream;
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

impl NewConn {
    fn start(self) {
        use NewConn::*;
        let (sk, incoming, outgoing) = match self {
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
                incoming,
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
                    incoming,
                    outgoing,
                )
            }
        };

        let laddr = sk.local_addr();

        let rsk = Arc::clone(&sk);
        // receive
        shenango::thread::spawn(move || {
            let mut buf = [0u8; 1024];
            loop {
                let (read_len, from_addr) = rsk
                    .read_from(&mut buf)
                    .wrap_err("shenango read_from")
                    .unwrap();
                if incoming.send(Msg {
                    addr: from_addr,
                    buf: buf[..read_len].to_vec(),
                }).is_err() {
                    warn!(sk=?laddr, "Incoming channel dropped, recv thread exiting");
                    break;
                }
            }
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
            V4(a) => Box::pin(futures_util::future::ready(self.make_listen(a).map(|sk| Box::pin(futures_util::stream::once(futures_util::future::ready(Ok(
                            sk,
                        )))) as _))),
            V6(a) => Box::pin(futures_util::future::ready(Err(eyre!(
                "Only IPv4 is supported: {:?}",
                a
            )))),
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
        Box::pin(futures_util::future::ready(self.make_dial()))
    }
}

#[derive(Clone)]
pub struct ShenangoUdpSk {
    outgoing: flume::Sender<Msg>,
    incoming: Arc<flume::Receiver<Msg>>,
}

impl std::fmt::Debug for ShenangoUdpSk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShenangoUdpSk").finish()
    }
}

impl ShenangoUdpSk {
    fn new(inc: flume::Receiver<Msg>, out: flume::Sender<Msg>) -> Self {
        Self {
            outgoing: out,
            incoming: Arc::new(inc),
        }
    }
}

impl ChunnelConnection for ShenangoUdpSk {
    type Data = (SocketAddr, Vec<u8>);

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        use SocketAddr::*;
        match data {
            (V4(addr), d) => {
                self.outgoing
                    .send(Msg { addr, buf: d })
                    .expect("shenango won't drop");
                Box::pin(futures_util::future::ready(Ok(())))
            }
            (V6(a), _) => Box::pin(futures_util::future::ready(Err(eyre!(
                "Only IPv4 is supported: {:?}",
                a
            )))),
        }
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let inc = Arc::clone(&self.incoming);
        Box::pin(async move {
            let Msg { addr: a, buf: d } = inc
                .recv_async()
                .await
                .expect("shenango side will never drop");
            Ok((SocketAddr::V4(a), d))
        })
    }
}

#[derive(Clone, Debug)]
pub struct ShenangoUdpReqChunnel(pub ShenangoUdpSkChunnel);

impl ChunnelListener for ShenangoUdpReqChunnel {
    type Addr = SocketAddr;
    type Connection = UdpConn;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        let sk = self.0.listen(a);
        Box::pin(async move {
            use futures_util::StreamExt;
            // .listen() gives a Once<Ready<...>>, so the top level might error but after that
            // unwraps are ok.
            let sk = bertha::util::AddrSteer::new(sk.await?.next().await.unwrap().unwrap());
            Ok(sk.steer(UdpConn::new))
        })
    }
}

#[derive(Debug, Clone)]
pub struct UdpConn {
    resp_addr: SocketAddr,
    recv: Arc<flume::Receiver<(SocketAddr, Vec<u8>)>>,
    send: ShenangoUdpSk,
}

impl UdpConn {
    fn new(
        resp_addr: SocketAddr,
        send: ShenangoUdpSk,
        recv: flume::Receiver<(SocketAddr, Vec<u8>)>,
    ) -> Self {
        UdpConn {
            resp_addr,
            recv: Arc::new(recv),
            send,
        }
    }
}

impl ChunnelConnection for UdpConn {
    type Data = (SocketAddr, Vec<u8>);

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        let sk = self.send.clone();
        let addr = self.resp_addr;
        let (_, data) = data;
        Box::pin(async move {
            sk.send((addr, data)).await?;
            Ok(())
        })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let recv = Arc::clone(&self.recv);
        Box::pin(async move {
            let (a, d) = recv
                .recv_async()
                .await
                .wrap_err(eyre!("Nothing more to receive"))?;
            Ok((a, d))
        }) as _
    }

    fn recv_batch<'cn>(
        &'cn self,
        batch_size: usize,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Self::Data>, Report>> + Send + 'cn>>
    where
        Self::Data: Send,
        Self: Sync,
    {
        Box::pin(async move {
            let mut this_batch = vec![];

            // try to pull off as many messages as we can, up to `batch_size`.
            loop {
                match self.recv.try_recv() {
                    Ok(d) => {
                        this_batch.push(d);
                        if this_batch.len() >= batch_size {
                            break;
                        }
                    }
                    Err(flume::TryRecvError::Empty) => {
                        break;
                    }
                    Err(flume::TryRecvError::Disconnected) => {
                        return Err(eyre!("Connection closed"));
                    }
                }
            }

            if this_batch.is_empty() {
                Ok(vec![self
                    .recv
                    .recv_async()
                    .await
                    .wrap_err(eyre!("Nothing more to receive"))?])
            } else {
                Ok(this_batch)
            }
        })
    }
}
