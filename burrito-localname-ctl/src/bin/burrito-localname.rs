use burrito_localname_ctl::ctl;
use color_eyre::eyre::Error;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "burrito")]
struct Opt {
    #[structopt(short, long)]
    force_burrito: bool,

    #[structopt(short, long)]
    burrito_coordinator_addr: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let opt = Opt::from_args();
    color_eyre::install()?;
    tracing_subscriber::fmt::init();

    ctl::serve_ctl(opt.burrito_coordinator_addr, opt.force_burrito).await?;
    Ok(())
}
