//! Shenango-powered UDP chunnel.

use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{eyre, Report, WrapErr};
use crossbeam::channel;
use futures_util::stream::Stream;
use std::future::Future;
use std::net::{SocketAddr, SocketAddrV4};
use std::path::Path;
use std::pin::Pin;
use std::sync::{atomic::AtomicUsize, Arc, Mutex as StdMutex};
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, trace, warn};

lazy_static::lazy_static! {
static ref SHENANGO_RUNTIME_SENDER: Arc<StdMutex<Option<channel::Sender<NewConn>>>> =
    Arc::new(StdMutex::new(None));
}

enum NewConn {
    Listen {
        addr: SocketAddrV4,
        cn: Self::Dial,
    },
    Dial {
        incoming: mpsc::UnboundedSender<(SocketAddrV4, Vec<u8>)>,
        outgoing: channel::Receiver<(SocketAddrV4, Vec<u8>)>,
    },
}

impl NewConn {
    fn start(self) {
        use NewConn::*;
        let (sk, incoming, outgoing) = match self {
            Listen {
                addr,
                cn: Dial { incoming, outgoing },
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
        use std::convert::TryInto;
        use std::time::Instant;
        let times: Arc<dashmap::DashMap<u64, Instant>> = Default::default();
        let rtimes = Arc::clone(&times);

        let rsk = Arc::clone(&sk);
        // receive
        shenango::thread::spawn(move || {
            let mut buf = [0u8; 1024];
            let mut times_us = hdrhistogram::Histogram::<u64>::new_with_max(100_000, 3).unwrap();
            loop {
                let (read_len, from_addr) = rsk
                    .read_from(&mut buf)
                    .wrap_err("shenango read_from")
                    .unwrap();

                // TODO check times
                let msg_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
                if let Some((_, then)) = rtimes.remove(&msg_id) {
                    times_us.saturating_record(then.elapsed().as_micros() as _);
                }

                trace!(sk=?laddr, "recv");
                if let Err(_) = incoming.send((from_addr, buf[..read_len].to_vec())) {
                    warn!(sk=?laddr, "Incoming channel dropped, recv thread exiting");
                    break;
                }

                if times_us.len() % 1000 == 0 {
                    debug!(
                        p5 = times_us.value_at_quantile(0.05),
                        p25 = times_us.value_at_quantile(0.25),
                        p50 = times_us.value_at_quantile(0.5),
                        p75 = times_us.value_at_quantile(0.75),
                        p95 = times_us.value_at_quantile(0.95),
                        cnt = times_us.len(),
                        "shenango times (us)",
                    );
                }
            }

            debug!(
                p5 = times_us.value_at_quantile(0.05),
                p25 = times_us.value_at_quantile(0.25),
                p50 = times_us.value_at_quantile(0.5),
                p75 = times_us.value_at_quantile(0.75),
                p95 = times_us.value_at_quantile(0.95),
                cnt = times_us.len(),
                "shenango times (us)",
            );
        });

        // send
        shenango::thread::spawn(move || {
            let mut no_yield_ctr = 0;
            let mut no_yield_ctr_hist =
                hdrhistogram::Histogram::<u16>::new_with_max(1000, 3).unwrap();
            let mut yield_time_hist =
                hdrhistogram::Histogram::<u32>::new_with_max(10000, 4).unwrap();
            let mut write_time_hist =
                hdrhistogram::Histogram::<u32>::new_with_max(10000, 4).unwrap();
            let mut queue_time_hist =
                hdrhistogram::Histogram::<u32>::new_with_max(10_000, 4).unwrap();
            let clock = quanta::Clock::new();
            loop {
                match outgoing.try_recv() {
                    Ok((a, data)) => {
                        // TODO record msg id time
                        let msg_id = u64::from_le_bytes(data[0..8].try_into().unwrap());
                        times.insert(msg_id, Instant::now());

                        // msg id is secretly the msg creation time
                        queue_time_hist
                            .saturating_record(clock.delta(msg_id, clock.end()).as_micros() as _);

                        no_yield_ctr += 1;

                        trace!(sk=?laddr, "send");

                        let write_time_start = clock.start();
                        sk.write_to(&data, a).wrap_err("shenango write_to").unwrap();
                        let write_time_end = clock.end();
                        let write_time = clock.delta(write_time_start, write_time_end);
                        write_time_hist.saturating_record(write_time.as_nanos() as _);
                    }
                    Err(crossbeam::channel::TryRecvError::Empty) => {
                        if no_yield_ctr > 0 {
                            no_yield_ctr_hist.saturating_record(no_yield_ctr);
                        }

                        no_yield_ctr = 0;
                        let yield_time_start = clock.start();
                        shenango::thread::thread_yield();
                        let yield_time_end = clock.end();
                        let yield_time = clock.delta(yield_time_start, yield_time_end);
                        yield_time_hist.saturating_record(yield_time.as_nanos() as _);
                    }
                    Err(crossbeam::channel::TryRecvError::Disconnected) => {
                        debug!(sk=?laddr, "send thread exiting");
                        debug!(
                            p5 = no_yield_ctr_hist.value_at_quantile(0.05),
                            p25 = no_yield_ctr_hist.value_at_quantile(0.25),
                            p50 = no_yield_ctr_hist.value_at_quantile(0.5),
                            p75 = no_yield_ctr_hist.value_at_quantile(0.75),
                            p95 = no_yield_ctr_hist.value_at_quantile(0.95),
                            cnt = no_yield_ctr_hist.len(),
                            thread = ?laddr,
                            "shenango no yield ctr",
                        );
                        debug!(
                            p5 = yield_time_hist.value_at_quantile(0.05),
                            p25 = yield_time_hist.value_at_quantile(0.25),
                            p50 = yield_time_hist.value_at_quantile(0.5),
                            p75 = yield_time_hist.value_at_quantile(0.75),
                            p95 = yield_time_hist.value_at_quantile(0.95),
                            cnt = yield_time_hist.len(),
                            thread = ?laddr,
                            "shenango yield time (ns)",
                        );
                        debug!(
                            p5 = write_time_hist.value_at_quantile(0.05),
                            p25 = write_time_hist.value_at_quantile(0.25),
                            p50 = write_time_hist.value_at_quantile(0.5),
                            p75 = write_time_hist.value_at_quantile(0.75),
                            p95 = write_time_hist.value_at_quantile(0.95),
                            cnt = write_time_hist.len(),
                            thread = ?laddr,
                            "shenango write time (ns)",
                        );
                        debug!(
                            p5 =  queue_time_hist.value_at_quantile(0.05),
                            p25 = queue_time_hist.value_at_quantile(0.25),
                            p50 = queue_time_hist.value_at_quantile(0.5),
                            p75 = queue_time_hist.value_at_quantile(0.75),
                            p95 = queue_time_hist.value_at_quantile(0.95),
                            cnt = queue_time_hist.len(),
                            thread = ?laddr,
                            "shenango queue time (us)",
                        );
                        break;
                    }
                }
            }
        });
    }
}

// everything here blocks.
// On success, this won't return while the ShenangoUdpSkChunnel still lives.
fn shenango_runtime_start(shenango_config: &Path) -> channel::Sender<NewConn> {
    let mut rt_guard = SHENANGO_RUNTIME_SENDER
        .lock()
        .expect("Shenango runtime lock acquisition failure is critical");
    if let Some(ref s) = *rt_guard {
        return s.clone();
    }

    let (s, conns) = channel::bounded(0);
    rt_guard.replace(s.clone());
    let cfg = shenango_config.to_str().unwrap().to_owned();
    std::thread::spawn(move || {
        shenango::runtime_init(cfg, move || loop {
            match conns.try_recv() {
                Ok(conn) => {
                    conn.start();
                }
                Err(crossbeam::channel::TryRecvError::Empty) => {
                    shenango::thread::thread_yield();
                }
                Err(crossbeam::channel::TryRecvError::Disconnected) => {
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
    events: channel::Sender<NewConn>,
}

impl ShenangoUdpSkChunnel {
    pub fn new(config: impl AsRef<Path>) -> Self {
        let config = config.as_ref().to_path_buf();
        let s = shenango_runtime_start(&config);
        Self { events: s }
    }

    fn make_listen(&self, a: SocketAddrV4) -> Result<ShenangoUdpSk, Report> {
        let (incoming_s, incoming_r) = mpsc::unbounded_channel();
        let (outgoing_s, outgoing_r) = channel::unbounded();
        self.events.send(NewConn::Listen {
            addr: a,
            cn: NewConn::Dial {
                incoming: incoming_s,
                outgoing: outgoing_r,
            },
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
    type Addr = SocketAddr;
    type Connection = ShenangoUdpSk;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn listen(&mut self, addr: Self::Addr) -> Self::Future {
        use SocketAddr::*;
        match addr {
            V4(a) => Box::pin(futures_util::future::ready(self.make_listen(a).and_then(
                |sk| {
                    Ok(
                        Box::pin(futures_util::stream::once(futures_util::future::ready(Ok(
                            sk,
                        )))) as _,
                    )
                },
            ))),
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

#[derive(Debug, Clone)]
pub struct ShenangoUdpSk {
    outgoing: channel::Sender<(SocketAddrV4, Vec<u8>)>,
    incoming: Arc<Mutex<mpsc::UnboundedReceiver<(SocketAddrV4, Vec<u8>)>>>,
    outgoing_len_hist: Arc<StdMutex<hdrhistogram::Histogram<u32>>>,
}

impl Drop for ShenangoUdpSk {
    fn drop(&mut self) {
        let hist = self.outgoing_len_hist.lock().unwrap();
        debug!(
            p5 = hist.value_at_quantile(0.05),
            p25 = hist.value_at_quantile(0.25),
            p50 = hist.value_at_quantile(0.5),
            p75 = hist.value_at_quantile(0.75),
            p95 = hist.value_at_quantile(0.95),
            cnt = hist.len(),
            "shenango queue len (us)",
        );
    }
}

impl ShenangoUdpSk {
    fn new(
        inc: mpsc::UnboundedReceiver<(SocketAddrV4, Vec<u8>)>,
        out: channel::Sender<(SocketAddrV4, Vec<u8>)>,
    ) -> Self {
        Self {
            incoming: Arc::new(Mutex::new(inc)),
            outgoing: out,
            outgoing_len_hist: Arc::new(StdMutex::new(
                hdrhistogram::Histogram::<u32>::new_with_max(10000, 3).unwrap(),
            )),
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
                self.outgoing_len_hist
                    .lock()
                    .unwrap()
                    .saturating_record(self.outgoing.len() as _);
                self.outgoing.send((addr, d)).expect("shenango won't drop");
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
            let (a, d) = inc
                .lock()
                .await
                .recv()
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
    pending_ctr: Arc<AtomicUsize>,
    recv: Arc<Mutex<mpsc::UnboundedReceiver<(SocketAddr, Vec<u8>)>>>,
    send: ShenangoUdpSk,
}

impl UdpConn {
    fn new(
        resp_addr: SocketAddr,
        send: ShenangoUdpSk,
        recv: Arc<Mutex<mpsc::UnboundedReceiver<(SocketAddr, Vec<u8>)>>>,
        pending_ctr: Arc<AtomicUsize>,
    ) -> Self {
        UdpConn {
            resp_addr,
            pending_ctr,
            recv,
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
        let r = Arc::clone(&self.recv);
        let ctr = Arc::clone(&self.pending_ctr);
        Box::pin(async move {
            let d = r.lock().await.recv().await;
            ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            trace!(from = ?&d.as_ref().map(|x| x.0), "recv pkt");
            d.ok_or_else(|| eyre!("Nothing more to receive"))
        }) as _
    }
}
