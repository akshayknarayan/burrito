use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "xdp_clear")]
struct Opt {
    #[structopt(short, long)]
    ip_addr: Option<std::net::IpAddr>,

    #[structopt(long)]
    interface: Option<String>,
}

fn main() -> Result<(), color_eyre::eyre::Report> {
    let opt = Opt::from_args();
    tracing_subscriber::fmt::init();
    color_eyre::install()?;

    if let Some(addr) = opt.ip_addr {
        xdp_shard::remove_xdp_on_address(addr)?
    }

    if let Some(ifce) = opt.interface {
        xdp_shard::remove_xdp_on_ifname(&ifce)?
    }

    Ok(())
}
