//! Shenango-powered UDP chunnel.

use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{eyre, Report, WrapErr};
use crossbeam::channel;
use futures_util::stream::Stream;
use std::future::Future;
use std::net::SocketAddrV4;
use std::path::Path;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::{mpsc, Mutex};

static SHENANGO_RUNTIME_INIT: AtomicBool = AtomicBool::new(false);

enum NewConn {
    Listen {
        addr: SocketAddrV4,
        incoming: mpsc::UnboundedSender<(SocketAddrV4, Vec<u8>)>,
        outgoing: channel::Receiver<(SocketAddrV4, Vec<u8>)>,
    },
    Dial {
        incoming: mpsc::UnboundedSender<(SocketAddrV4, Vec<u8>)>,
        outgoing: channel::Receiver<(SocketAddrV4, Vec<u8>)>,
    },
}

// everything here blocks.
// On success, this won't return while the ShenangoUdpSkChunnel still lives.
fn shenango_runtime_start(
    shenango_config: &Path,
    conns: channel::Receiver<NewConn>,
) -> Result<(), Report> {
    SHENANGO_RUNTIME_INIT
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .map_err(|_| eyre!("Can only start shenango runtime once"))?;
    shenango::runtime_init(shenango_config.to_str().unwrap().to_owned(), move || {
        while let Ok(conn) = conns.recv() {
            use NewConn::*;
            let (sk, incoming, outgoing) = match conn {
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

            let rsk = Arc::clone(&sk);
            // receive
            shenango::thread::spawn(move || {
                let mut buf = [0u8; 1024];
                loop {
                    let (read_len, from_addr) = rsk
                        .read_from(&mut buf)
                        .wrap_err("shenango read_from")
                        .unwrap();
                    incoming
                        .send((from_addr, buf[..read_len].to_vec()))
                        .unwrap();
                }
            });

            // send
            shenango::thread::spawn(move || loop {
                match outgoing.try_recv() {
                    Ok((a, data)) => {
                        sk.write_to(&data, a).wrap_err("shenango write_to").unwrap();
                    }
                    Err(crossbeam::channel::TryRecvError::Empty) => {
                        shenango::thread::thread_yield();
                    }
                    Err(crossbeam::channel::TryRecvError::Disconnected) => {
                        break;
                    }
                }
            });
        }
    })
    .map_err(|i| eyre!("shenango runtime error: {:?}", i))
}

#[derive(Clone, Debug)]
pub struct ShenangoUdpSkChunnel {
    events: channel::Sender<NewConn>,
}

impl ShenangoUdpSkChunnel {
    pub fn new(config: impl AsRef<Path>) -> Result<Self, Report> {
        let (s, r) = channel::bounded(0);
        let config = config.as_ref().to_path_buf();
        std::thread::spawn(move || shenango_runtime_start(&config, r));
        Ok(Self { events: s })
    }

    fn make_listen(&self, a: SocketAddrV4) -> Result<ShenangoUdpSk, Report> {
        let (incoming_s, incoming_r) = mpsc::unbounded_channel();
        let (outgoing_s, outgoing_r) = channel::unbounded();
        self.events.send(NewConn::Listen {
            addr: a,
            incoming: incoming_s,
            outgoing: outgoing_r,
        })?;
        Ok(ShenangoUdpSk::new(incoming_r, outgoing_s))
    }

    fn make_dial(&self) -> Result<ShenangoUdpSk, Report> {
        let (incoming_s, incoming_r) = mpsc::unbounded_channel();
        let (outgoing_s, outgoing_r) = channel::unbounded();
        self.events.send(NewConn::Dial {
            incoming: incoming_s,
            outgoing: outgoing_r,
        })?;
        Ok(ShenangoUdpSk::new(incoming_r, outgoing_s))
    }
}

impl ChunnelListener for ShenangoUdpSkChunnel {
    type Addr = SocketAddrV4;
    type Connection = ShenangoUdpSk;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        Box::pin(futures_util::future::ready(self.make_listen(a).and_then(
            |sk| {
                Ok(
                    Box::pin(futures_util::stream::once(futures_util::future::ready(Ok(
                        sk,
                    )))) as _,
                )
            },
        )))
    }
}

impl ChunnelConnector for ShenangoUdpSkChunnel {
    type Addr = ();
    type Connection = ShenangoUdpSk;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn connect(&mut self, _a: Self::Addr) -> Self::Future {
        // pass an arbitrary port, we're not going to use it
        Box::pin(futures_util::future::ready(self.make_dial()))
    }
}

#[derive(Debug, Clone)]
pub struct ShenangoUdpSk {
    outgoing: channel::Sender<(SocketAddrV4, Vec<u8>)>,
    incoming: Arc<Mutex<mpsc::UnboundedReceiver<(SocketAddrV4, Vec<u8>)>>>,
}

impl ShenangoUdpSk {
    fn new(
        inc: mpsc::UnboundedReceiver<(SocketAddrV4, Vec<u8>)>,
        out: channel::Sender<(SocketAddrV4, Vec<u8>)>,
    ) -> Self {
        Self {
            incoming: Arc::new(Mutex::new(inc)),
            outgoing: out,
        }
    }
}

impl ChunnelConnection for ShenangoUdpSk {
    type Data = (SocketAddrV4, Vec<u8>);

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        self.outgoing.send(data).expect("shenango won't drop");
        Box::pin(futures_util::future::ready(Ok(())))
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let inc = Arc::clone(&self.incoming);
        Box::pin(async move {
            Ok(inc
                .lock()
                .await
                .recv()
                .await
                .expect("shenango side will never drop"))
        })
    }
}
