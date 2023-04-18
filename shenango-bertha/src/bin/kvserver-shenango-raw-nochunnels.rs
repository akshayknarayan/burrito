use color_eyre::eyre::Report;
use shenango_bertha::{single_shard_conns, single_shard_no_conns};
use std::net::SocketAddrV4;
use structopt::StructOpt;
use tracing::info;

#[derive(Debug, StructOpt)]
#[structopt(name = "kvserver")]
struct Opt {
    #[structopt(short, long, default_value = "0.0.0.0:5001")]
    addr: SocketAddrV4,

    #[structopt(short, long)]
    num_shards: u16,

    #[structopt(short, long)]
    cfg: std::path::PathBuf,

    #[structopt(short, long)]
    use_connections: bool,

    #[structopt(short, long)]
    log: bool,
}

fn main() -> Result<(), Report> {
    let opt = Opt::from_args();
    color_eyre::install()?;
    if opt.log {
        tracing_subscriber::fmt::init();
    }

    info!("KV Server, no chunnels");
    shenango::runtime_init(opt.cfg.to_str().unwrap().to_owned(), move || {
        run_server(opt).unwrap();
    })
    .unwrap();
    Ok(())
}

fn run_server(opt: Opt) -> Result<(), Report> {
    let use_conns = opt.use_connections;
    for i in 1..opt.num_shards {
        let ip = opt.addr.ip();
        let base_port = opt.addr.port();
        let shard_addr = SocketAddrV4::new(*ip, base_port + i + 1);
        shenango::thread::spawn_detached(move || {
            if use_conns {
                single_shard_conns(shard_addr)
            } else {
                single_shard_no_conns(shard_addr).unwrap()
            }
        });
    }

    let ip = opt.addr.ip();
    let base_port = opt.addr.port();
    let shard_addr = SocketAddrV4::new(*ip, base_port + 1);
    if use_conns {
        single_shard_conns(shard_addr)
    } else {
        single_shard_no_conns(shard_addr).unwrap()
    }

    Ok(())
}
