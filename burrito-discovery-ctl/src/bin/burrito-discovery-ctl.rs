use anyhow::Error;
use burrito_discovery_ctl::ctl;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "burrito")]
struct Opt {
    #[structopt(short, long)]
    force_burrito: bool,

    #[structopt(short, long)]
    redis_addr: String,

    #[structopt(short, long)]
    net_addr: Option<String>,

    #[structopt(short, long)]
    burrito_root: Option<std::path::PathBuf>,
}

fn get_net_addrs() -> Result<Vec<std::net::IpAddr>, Error> {
    let hostname_ret = std::process::Command::new("hostname")
        .arg("-I")
        .output()
        .expect("failed to exec hostname")
        .stdout;

    let ips = String::from_utf8(hostname_ret)?;
    ips.split_whitespace().map(|s| Ok(s.parse()?)).collect()
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let opt = Opt::from_args();
    tracing_subscriber::fmt::init();

    ctl::serve_ctl(
        opt.burrito_root,
        &opt.redis_addr,
        match opt.net_addr {
            Some(a) => vec![a.parse()?],
            None => get_net_addrs()?,
        },
        opt.force_burrito,
    )
    .await?;
    Ok(())
}
