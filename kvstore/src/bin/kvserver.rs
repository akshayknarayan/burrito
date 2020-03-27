use slog::{debug, info};
use std::error::Error;
use structopt::StructOpt;
use tracing_timing::{Builder, Histogram};

type StdError = Box<dyn Error + Send + Sync + 'static>;

#[derive(Debug, StructOpt)]
#[structopt(name = "kvserver")]
struct Opt {
    #[structopt(short, long)]
    port: u16,

    #[structopt(short, long)]
    ip_addr: Option<std::net::IpAddr>,

    #[structopt(short, long)]
    num_shards: Option<usize>,

    #[structopt(long, default_value = "/tmp/burrito")]
    burrito_root: std::path::PathBuf,

    #[structopt(long)]
    no_shard_ctl: bool,

    #[structopt(short, long)]
    log: bool,
}

fn sk_shard_addrs(
    ip_addr: Option<std::net::IpAddr>,
    num_shards: usize,
    base_port: u16,
) -> Vec<std::net::SocketAddr> {
    let ip_addr = ip_addr.unwrap_or_else(|| std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
    (0..num_shards + 1)
        .map(|i| std::net::SocketAddr::new(ip_addr, base_port + i as u16))
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), StdError> {
    let log = burrito_util::logger();
    let opt = Opt::from_args();
    if opt.log {
        write_tracing(&log);
    }

    tracing_subscriber::fmt::init();

    let num_shards = match opt.num_shards {
        None | Some(1) => 0, // having 1 shard is pointless, same as 0, might as well avoid the extra channel sends.
        Some(x) => x,
    };
    let shard_fn = move |m: &kvstore::Msg| {
        // TODO update to match FNV function in xdp program
        use std::hash::{Hash, Hasher};
        let mut hasher = ahash::AHasher::default();
        m.key().hash(&mut hasher);
        hasher.finish() as usize % num_shards
    };

    info!(&log, "KV Server");

    let port = opt.port;
    let addrs = sk_shard_addrs(opt.ip_addr, num_shards, port);

    use burrito_shard_ctl::{
        proto::{self, ShardInfo},
        ShardCtlClient,
    };

    let si = ShardInfo {
        service_name: "kv".into(),
        canonical_addr: proto::Addr::Udp(addrs[0]),
        shard_addrs: if opt.no_shard_ctl {
            vec![]
        } else {
            addrs[1..].iter().map(|a| proto::Addr::Udp(*a)).collect()
        },
        shard_info: proto::SimpleShardPolicy {
            packet_data_offset: 18,
            packet_data_length: 4,
        },
    };

    debug!(&log, "Registering shard"; "si" => ?&si);

    // register the shards
    {
        let mut shardctl = ShardCtlClient::new(&opt.burrito_root).await?;
        shardctl.register(si).await?;
    } // drop shardctl connection

    info!(&log, "Registered shard-server"; "canonical_addr" => ?addrs[0], "shard_addrs" => ?&addrs[1..]);

    let ls: Result<Vec<_>, _> = futures_util::future::join_all(
        addrs
            .into_iter()
            .map(|addr| async move { tokio::net::UdpSocket::bind(addr).await }),
    )
    .await
    .into_iter()
    .collect();

    kvstore::shard_server_udp(ls?, shard_fn).await?;
    Ok(())
}

fn write_tracing(log: &slog::Logger) {
    let subscriber = Builder::default()
        .no_span_recursion()
        .build(|| Histogram::new_with_max(10_000_000, 2).unwrap());
    let sid = subscriber.downcaster();
    let d = tracing::Dispatch::new(subscriber);

    tracing::dispatcher::set_global_default(d.clone()).expect("set tracing global default");

    let log = log.clone();
    std::thread::spawn(move || loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
        sid.downcast(&d).unwrap().force_synchronize();
        sid.downcast(&d).unwrap().with_histograms(|hs| {
            for (span_group, hs) in hs {
                for (event_group, h) in hs {
                    let tag = format!("{}:{}", span_group, event_group);
                    slog::info!(&log, "tracing"; "event" => &tag,
                        "min" => h.min(),
                        "p25" => h.value_at_quantile(0.25),
                        "p50" => h.value_at_quantile(0.5),
                        "p75" => h.value_at_quantile(0.75),
                        "max" => h.max(),
                        "cnt" => h.len(),
                    );
                }
            }
        });
    });
}
