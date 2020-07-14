use bertha::{
    bincode::SerializeChunnel, reliable::ReliabilityChunnel, tagger::TaggerChunnel,
    udp::UdpChunnel, ChunnelConnector, ChunnelListener, OptionWrap,
};
use burrito_shard_ctl::proto::{self, ShardInfo};
use burrito_shard_ctl::{ShardCanonicalServer, ShardServer};
use structopt::StructOpt;
use tracing::{debug, info, trace};

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
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let opt = Opt::from_args();

    let num_shards = match opt.num_shards {
        None | Some(1) => 0, // having 1 shard is pointless, same as 0, might as well avoid the extra channel sends.
        Some(x) => x,
    };

    let addrs = sk_shard_addrs(opt.ip_addr, num_shards, opt.port);
    let si = ShardInfo {
        canonical_addr: proto::Addr::Udp(addrs[0]),
        shard_addrs: addrs[1..].iter().map(|a| proto::Addr::Udp(*a)).collect(),
        shard_info: proto::SimpleShardPolicy {
            packet_data_offset: 18,
            packet_data_length: 4,
        },
    };

    // internal connection to shards
    let (internal_srv, internal_cli) =
        bertha::chan_transport::RendezvousChannel::<proto::Addr, kvstore::Msg, _>::new(100).split();

    // chunnels for the shards
    for a in si.shard_addrs {
        let srv = UdpChunnel::default();
        let external = OptionWrap::new(SerializeChunnel::<_, kvstore::Msg>::from(
            TaggerChunnel::from(ReliabilityChunnel::from(srv)),
        ));

        let t = internal_srv.listen(a).await;
        let j = external.listen(a).await;

        //let shard = ShardServer::<(), ()>::new(internal_srv.clone(), external);
        //let c = shard.listen(a).await;
    }

    // external connection to listen on
    let srv = UdpChunnel::default();
    let external = SerializeChunnel::<_, kvstore::Msg>::from(TaggerChunnel::from(
        ReliabilityChunnel::from(srv),
    ));

    let canonical = ShardCanonicalServer::<(), ()>::new(external, internal_cli, opt.burrito_root);
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
