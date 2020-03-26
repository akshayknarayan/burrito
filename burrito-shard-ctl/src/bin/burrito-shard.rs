use anyhow::Error;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "burrito-shard")]
struct Opt {
    #[structopt(short, long)]
    force_burrito: bool,

    #[structopt(short, long)]
    burrito_root: Option<std::path::PathBuf>,

    #[structopt(short, long)]
    redis_addr: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let opt = Opt::from_args();
    tracing_subscriber::fmt::init();

    let burrito = burrito_shard_ctl::ShardCtl::new(&opt.redis_addr).await?;
    let burrito_addr = opt
        .burrito_root
        .unwrap_or_else(|| std::path::PathBuf::from("/tmp/burrito"))
        .join(burrito_shard_ctl::CONTROLLER_ADDRESS);

    // if force_burrito, then we are ok with hijacking /controller, potentially from another
    // instance of burrito. Might cause bad things.
    if opt.force_burrito {
        std::fs::remove_file(&burrito_addr).unwrap_or_default(); // ignore error if file was not present
    }

    let ba = burrito_addr.clone();
    ctrlc::set_handler(move || {
        std::fs::remove_file(&ba).expect("Remove file for currently listening controller");
        std::process::exit(0);
    })?;

    tracing::info!(addr = ?&burrito_addr, "Starting ShardCtl");
    let ul = tokio::net::UnixListener::bind(burrito_addr)?;
    burrito.serve_on(ul).await?;
    Ok(())
}
