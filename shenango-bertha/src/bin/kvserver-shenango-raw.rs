use color_eyre::eyre::Report;
use shenango_bertha::serve;
use structopt::StructOpt;
use tracing::info;

#[derive(Debug, StructOpt)]
#[structopt(name = "kvserver")]
struct Opt {
    #[structopt(short, long, default_value = "0.0.0.0:5001")]
    addr: std::net::SocketAddrV4,

    #[structopt(short, long)]
    num_shards: u16,

    #[structopt(short, long)]
    cfg: std::path::PathBuf,

    #[structopt(short, long)]
    skip_negotiation: bool,

    #[structopt(short, long)]
    log: bool,
}

fn main() -> Result<(), Report> {
    let opt = Opt::from_args();
    color_eyre::install()?;
    if opt.log {
        tracing_subscriber::fmt::init();
    }

    info!("KV Server");
    shenango::runtime_init(opt.cfg.to_str().unwrap().to_owned(), move || {
        run_server(opt).unwrap();
    })
    .unwrap();
    Ok(())
}

fn run_server(opt: Opt) -> Result<(), Report> {
    serve(opt.addr, opt.num_shards, opt.skip_negotiation)
}
