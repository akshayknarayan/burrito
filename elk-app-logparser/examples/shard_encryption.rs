use std::{
    future::{ready, Ready},
    net::SocketAddr,
    time::Duration,
};

use bertha::ChunnelConnection;
use color_eyre::eyre::{Report, WrapErr};
use elk_app_logparser::{
    connect,
    listen::{self, Line, ProcessLine},
};
use structopt::{clap::ArgGroup, StructOpt};
use tracing::info;
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

#[derive(Debug, StructOpt)]
#[structopt(name = "sharding_encryption", group = ArgGroup::with_name("addr").required(true))]
struct Opt {
    #[structopt(long)]
    redis_addr: String,

    #[structopt(long, group = "addr")]
    connect_addr: Option<SocketAddr>,

    #[structopt(long, group = "addr", requires_all(&["hostname", "num-workers"]))]
    listen_addr: Option<SocketAddr>,

    #[structopt(long)]
    hostname: Option<String>,

    #[structopt(long)]
    num_workers: Option<usize>,

    #[structopt(long)]
    logging: bool,
}

fn main() -> Result<(), Report> {
    color_eyre::install().unwrap();
    let opt = Opt::from_args();
    if opt.logging {
        let subscriber = tracing_subscriber::registry();
        let subscriber = subscriber
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let d = tracing::Dispatch::new(subscriber);
        d.init();
    }

    if let Some(addr) = opt.connect_addr {
        client(addr, opt.redis_addr)
    } else if let Some(addr) = opt.listen_addr {
        server(
            addr,
            opt.hostname.unwrap(),
            opt.num_workers.unwrap(),
            opt.redis_addr,
        )
    } else {
        unreachable!()
    }
}

struct DoNothing;
impl<Line> ProcessLine<(SocketAddr, Line)> for DoNothing
where
    Line: 'static,
{
    type Error = std::convert::Infallible;
    type Future<'a> = Ready<Result<(), Self::Error>>;

    fn process_lines<'a>(
        &'a self,
        _line_batch: &'a mut [Option<(SocketAddr, Line)>],
    ) -> Self::Future<'a> {
        ready(Ok(()))
    }
}

fn server(
    listen_addr: SocketAddr,
    hostname: String,
    num_workers: usize,
    redis_addr: String,
) -> Result<(), Report> {
    info!("starting server");
    listen::serve(
        listen_addr,
        hostname,
        num_workers,
        redis_addr,
        DoNothing,
        false,
        None,
    )
}

fn client(connect_addr: SocketAddr, redis_addr: String) -> Result<(), Report> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .wrap_err("Building tokio runtime")?;
    rt.block_on(async move {
        info!("starting client");
        let cn = connect::connect(connect_addr, redis_addr, false)
            .await
            .wrap_err("connect error")?;
        info!(?connect_addr, "got connection, starting");
        for _ in 0..100 {
            cn.send((0..10).map(|i| Line::Report(format!("{} abcdefg", i))))
                .await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Ok(())
    })
}
