use std::error::Error;
use structopt::StructOpt;

type StdError = Box<dyn Error + Send + Sync + 'static>;

#[derive(Debug, StructOpt)]
#[structopt(name = "xdp_clear")]
struct Opt {
    #[structopt(short, long)]
    ip_addr: Option<std::net::IpAddr>,

    #[structopt(long)]
    interface: Option<String>,
}

fn main() -> Result<(), StdError> {
    let opt = Opt::from_args();
    tracing_subscriber::fmt::init();

    if let Some(addr) = opt.ip_addr {
        xdp_shard::remove_xdp_on_address(addr)?
    }

    if let Some(ifce) = opt.interface {
        xdp_shard::remove_xdp_on_ifname(&ifce)?
    }

    Ok(())
}
