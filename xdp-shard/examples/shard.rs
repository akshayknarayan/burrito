use color_eyre::eyre::{eyre, Report};
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use structopt::StructOpt;
use tracing::debug_span;

#[derive(Clone, PartialEq, Serialize, Deserialize)]
struct Pong {
    duration_us: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct PingParams {
    work: Work,
    amount: i64,
    padding: Vec<u8>,
}

#[derive(Clone, Debug)]
struct Server {
    req_cnt: Arc<AtomicUsize>,
}

impl Default for Server {
    fn default() -> Self {
        Self {
            req_cnt: Arc::new(0.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum Work {
    Immediate,
    Const,         // work as duration in us
    Poisson,       // work as mean duration in us
    BusyTimeConst, // work as busy-spin duration in us
    BusyWorkConst, // work as number of operations to perform
}

impl Server {
    fn get_counter(&self) -> Arc<AtomicUsize> {
        self.req_cnt.clone()
    }

    async fn do_ping(&self, ping_req: PingParams) -> Result<Pong, Report> {
        let span = debug_span!("ping()", req = ?ping_req);
        let _span = span.enter();
        let then = std::time::Instant::now();

        let w: Work = ping_req.work;

        self.req_cnt
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let amt = ping_req.amount.try_into().expect("u64 to i64 cast");

        match w {
            Work::Immediate => (),
            Work::Const => {
                let completion_time = then + std::time::Duration::from_micros(amt);
                tokio::time::sleep_until(completion_time.into()).await;
            }
            Work::Poisson => {
                let completion_time = then + gen_poisson_duration(amt as f64)?;
                tokio::time::sleep_until(completion_time.into()).await;
            }
            Work::BusyTimeConst => {
                let completion_time = then + std::time::Duration::from_micros(amt);
                while std::time::Instant::now() < completion_time {
                    // spin
                    tokio::task::yield_now().await;
                }
            }
            Work::BusyWorkConst => {
                // copy from shenango:
                // https://github.com/shenango/shenango/blob/master/apps/synthetic/src/fakework.rs#L54
                let k = 2350845.545;
                for i in 0..amt {
                    criterion::black_box(f64::sqrt(k * i as f64));
                }
            }
        }

        Ok(Pong {
            duration_us: then
                .elapsed()
                .as_micros()
                .try_into()
                .expect("u128 to i64 cast"),
        })
    }
}

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short = "i", long = "interface")]
    interface: String,

    #[structopt(short = "p", long = "port")]
    ports: Vec<u16>,
}

fn dump_ctrs(
    interval: std::time::Duration,
    first_done: tokio::sync::oneshot::Sender<()>,
    ctrs: Vec<(u16, Arc<AtomicUsize>)>,
) {
    loop {
        std::thread::sleep(interval);
        let counts: Vec<_> = ctrs
            .iter()
            .map(|(port, ctr)| (port, ctr.load(Ordering::Relaxed)))
            .collect();

        if counts[0].1 > 0 {
            break;
        }
    }

    // we got the first request.
    // now, start the timer on sharding so we can see the difference
    first_done.send(()).unwrap();

    loop {
        std::thread::sleep(interval);
        let counts: Vec<_> = ctrs
            .iter()
            .map(|(port, ctr)| (port, ctr.load(Ordering::Relaxed)))
            .collect();

        tracing::info!(counts = ?&counts, "request counts");
    }
}

async fn serve_udp(srv: Server, port: u16) -> Result<(), Report> {
    // udp server
    let mut buf = [0u8; 1024];
    let addr: std::net::SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
    let sk = tokio::net::UdpSocket::bind::<std::net::SocketAddr>(addr)
        .await
        .unwrap();
    loop {
        let (len, from_addr) = sk.recv_from(&mut buf).await?;
        let msg = &buf[..len];
        // deserialize
        let msg: PingParams = bincode::deserialize(msg)?;
        let resp: Pong = srv.do_ping(msg.into()).await?.into();
        let msg = bincode::serialize(&resp)?;
        sk.send_to(&msg, from_addr).await?;
    }
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    let opt = Opt::from_args();

    tracing_subscriber::fmt::init();
    color_eyre::install()?;

    if opt.ports.is_empty() {
        return Err(eyre!("Must supply at least one port"));
    }

    // listen on ports
    let ctrs: Vec<(u16, Arc<AtomicUsize>)> = opt
        .ports
        .clone()
        .into_iter()
        .map(|port| {
            let srv = Server::default();
            let ctr = srv.get_counter();
            tokio::spawn(serve_udp(srv, port));
            (port, ctr)
        })
        .collect();

    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || dump_ctrs(std::time::Duration::from_secs(1), tx, ctrs));

    let (start_sharding_tx, start_sharding_rx) = std::sync::mpsc::channel();
    tokio::spawn(async move {
        // after this, wait say 10 seconds and then enable sharding
        rx.await.unwrap();
        std::thread::sleep(std::time::Duration::from_secs(5));
        start_sharding_tx.send(()).unwrap();
    });

    let mut prog =
        xdp_shard::BpfHandles::<xdp_shard::Ingress>::load_on_interface_name(&opt.interface)?;
    let ifn = opt.interface;

    let stop: Arc<AtomicBool> = Arc::new(false.into());
    let s = stop.clone();
    ctrlc::set_handler(move || {
        tracing::warn!("stopping");
        s.store(true, Ordering::SeqCst);
    })
    .unwrap();

    // start with no sharding, then introduce sharding in a bit
    while !stop.load(std::sync::atomic::Ordering::SeqCst) {
        std::time::Duration::from_secs(1);
        if let Ok(_) = start_sharding_rx.try_recv() {
            // PingParams is { i32, i64 } and we want to shard on the first value.
            // so offset = 0, length = 4.
            prog.shard_ports(opt.ports[0], &opt.ports[1..], 0, 4)?;
            tracing::info!(interface = ?&ifn, from = opt.ports[0], to = ?&opt.ports[1..], "sharding activated");
        }

        let (stats, prev) = prog.get_stats()?;

        let mut rxqs = stats.get_rxq_cpu_port_count();
        let prev_rxqs = prev.get_rxq_cpu_port_count();
        xdp_shard::diff_maps(&mut rxqs, &prev_rxqs);
        for (rxq, cpus) in rxqs.iter().enumerate() {
            for (cpu, portcounts) in cpus.iter().enumerate() {
                for (port, count) in portcounts.iter() {
                    if *count > 0 {
                        tracing::info!(interface = ?&ifn, rxq, cpu, port, count, "");
                    }
                }
            }
        }
    }

    Ok(())
}

fn gen_poisson_duration(amt: f64) -> Result<std::time::Duration, Report> {
    use rand_distr::{Distribution, Poisson};

    let mut rng = rand::thread_rng();
    let pois = Poisson::new(amt as f64).map_err(|e| eyre!("Invalid amount {}: {:?}", amt, e))?;
    Ok(std::time::Duration::from_micros(pois.sample(&mut rng)))
}
