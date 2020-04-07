use burrito_localname_ctl::ctl;
use failure::Error;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "burrito")]
struct Opt {
    #[cfg(feature = "docker")]
    #[structopt(short, long)]
    in_addr_docker: std::path::PathBuf,

    #[cfg(feature = "docker")]
    #[structopt(short, long)]
    out_addr_docker: std::path::PathBuf,

    #[structopt(short, long)]
    force_burrito: bool,

    #[structopt(short, long)]
    burrito_coordinator_addr: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let opt = Opt::from_args();
    let log = burrito_util::logger();
    tracing_subscriber::fmt::init();

    if cfg!(feature = "docker") {
        #[cfg(feature = "docker")]
        ctl::serve_ctl_and_docker(
            opt.burrito_coordinator_addr,
            opt.force_burrito,
            opt.in_addr_docker,
            opt.out_addr_docker,
            log,
        )
        .await;
        Ok(())
    } else {
        ctl::serve_ctl(opt.burrito_coordinator_addr, opt.force_burrito, log).await?;
        Ok(())
    }
}
