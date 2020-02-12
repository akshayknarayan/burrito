use std::time::Duration;
use structopt::StructOpt;
use tracing::{info, span, trace, Level};
use tracing_futures::Instrument;
use tracing_timing::{Builder, Histogram};

#[derive(Debug, StructOpt)]
#[structopt(name = "static_test")]
enum Opt {
    Server(Server),
    Client(Client),
}

#[derive(Debug, StructOpt)]
struct Server {
    #[structopt(short, long)]
    burrito_coordinator_addr: Option<std::path::PathBuf>,

    #[structopt(long)]
    resolve_to: std::net::SocketAddr,
}

#[derive(Debug, StructOpt)]
struct Client {
    #[structopt(short, long)]
    burrito_coordinator_addr: std::path::PathBuf,

    #[structopt(short, long)]
    iters: usize,
}

#[tokio::main]
async fn main() -> Result<(), failure::Error> {
    let opt = Opt::from_args();
    let subscriber = Builder::default()
        .no_span_recursion()
        .build(|| Histogram::new_with_max(10_000_000, 2).unwrap());
    let sid = subscriber.downcaster();
    let d = tracing::Dispatch::new(subscriber);

    //tracing_subscriber::fmt::init();
    tracing::dispatcher::set_global_default(d.clone()).expect("set tracing global default");

    match opt {
        Opt::Server(opt) => server_main(opt).await,
        Opt::Client(opt) => client_main(opt).await,
    }?;

    sid.downcast(&d).unwrap().force_synchronize();

    println!("trace timing");
    sid.downcast(&d).unwrap().with_histograms(|hs| {
        for (span_group, hs) in hs {
            for (event_group, h) in hs {
                println!(
                    "{}:{}: {} {} {} {} {}",
                    span_group,
                    event_group,
                    h.min(),
                    h.value_at_quantile(0.25),
                    h.value_at_quantile(0.5),
                    h.value_at_quantile(0.75),
                    h.max(),
                );
            }
        }
    });

    Ok(())
}

async fn server_main(opt: Server) -> Result<(), failure::Error> {
    std::fs::remove_dir_all("./tmp-test-sr/").unwrap_or_default();
    std::fs::create_dir_all("./tmp-test-sr/").unwrap();

    let burrito = burrito_ctl::StaticResolver::new(
        opt.burrito_coordinator_addr,
        &format!("{}", opt.resolve_to),
        "tcp".into(),
    );

    burrito
        .start()
        .instrument(span!(Level::DEBUG, "static server"))
        .await
}

#[tracing::instrument]
async fn client_main(opt: Client) -> Result<(), failure::Error> {
    let then = tokio::time::Instant::now();
    let mut sc = burrito_addr::staticnet::StaticClient::new(opt.burrito_coordinator_addr).await;
    let connect_dur = then.elapsed();

    info!("connected");

    let mut durs = vec![];
    for _ in 0..opt.iters {
        let then = tokio::time::Instant::now();
        let addr: hyper::Uri = burrito_addr::Uri::new("staticnet").into();
        trace!("resolving");
        sc.resolve(addr)
            .instrument(span!(Level::DEBUG, "client resolve"))
            .await?;
        trace!("resolved");

        let ping_dur = then.elapsed().as_micros() as f64;
        durs.push(ping_dur);
    }

    info!(connect_dur = ?connect_dur, "done");

    println!("ping");
    print_stats(durs);

    Ok(())
}

fn print_stats(durs: Vec<f64>) {
    if durs.len() < 10 {
        println!("{:?}", durs);
        return;
    }

    use criterion_stats::univariate as stats;

    let sample = stats::Sample::new(&durs[10..]);
    let min = sample.min();
    let mean = sample.mean();
    let dev = sample.std_dev(Some(mean));
    let max = sample.max();

    println!(
        "min {:?} -> avg {:?} +/- stddev {:?} -> max {:?}",
        Duration::from_micros(min as u64),
        Duration::from_micros(mean as u64),
        Duration::from_micros(dev as u64),
        Duration::from_micros(max as u64),
    );

    let p = sample.percentiles();
    let (p25, p50, p75) = p.quartiles();

    println!(
        "{:?}---|{:?} {:?} {:?}|---{:?}",
        Duration::from_micros(min as u64),
        Duration::from_micros(p25 as u64),
        Duration::from_micros(p50 as u64),
        Duration::from_micros(p75 as u64),
        Duration::from_micros(max as u64),
    );
}
