use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{eyre, Report, WrapErr};
use futures_util::stream::{FuturesUnordered, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use shenango_chunnel::ShenangoUdpSkChunnel;
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use structopt::StructOpt;
use tracing::{debug, info, instrument};
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

lazy_static::lazy_static! {
    static ref START: Instant = Instant::now();
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct Msg(Duration);

impl Msg {
    fn new() -> Self {
        Msg(START.elapsed())
    }

    fn elapsed(&self) -> Duration {
        START.elapsed() - self.0
    }
}

#[instrument]
async fn server(mut s: ShenangoUdpSkChunnel, addr: SocketAddrV4) -> Result<(), Report> {
    debug!(?addr, "listen");
    let st = s.listen(addr).await.wrap_err("Listen on UdpReqChunnel")?;
    st.try_for_each_concurrent(None, |cn| async move {
        let mut sends = FuturesUnordered::new();
        loop {
            tokio::select! (
                Ok(m) = cn.recv() => {
                    let f = cn.send(m);
                    sends.push(f);
                },
                Some(_) = sends.next() => {},
            );
        }
    })
    .await
    .wrap_err("Error while processing requests")?;
    unreachable!()
}

#[instrument]
async fn client(
    idx: usize,
    mut s: ShenangoUdpSkChunnel,
    addr: SocketAddrV4,
) -> Result<Vec<Duration>, Report> {
    debug!(?addr, "connect");
    let cn = s.connect(()).await?;
    debug!("starting");
    let mut durs = vec![];
    for _ in 0..1000 {
        let m = Msg::new();
        let v = bincode::serialize(&m).unwrap();
        cn.send((addr, v)).await.wrap_err("client send")?;

        let (_, v) = cn.recv().await.wrap_err("client recv")?;
        let m: Msg = bincode::deserialize(&v).unwrap();
        durs.push(m.elapsed());
    }

    info!("done");

    Ok(durs)
}

#[derive(Debug, StructOpt)]
#[structopt(name = "echo")]
struct Opt {
    #[structopt(short, long)]
    port: Option<u16>,

    #[structopt(short, long)]
    addr: Option<SocketAddrV4>,

    #[structopt(short, long, default_value = "1")]
    num_clients: usize,

    #[structopt(short, long)]
    cfg: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    color_eyre::install()?;
    let subscriber = tracing_subscriber::registry();
    let subscriber = subscriber
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(ErrorLayer::default());
    let d = tracing::Dispatch::new(subscriber);
    d.init();

    let opt = Opt::from_args();
    let ch = ShenangoUdpSkChunnel::new(&opt.cfg)?;
    let num_cl = opt.num_clients;

    if let Some(p) = opt.port {
        info!(mode = "server", "created ShenangoUdpSkChunnel");
        let a = SocketAddrV4::new(std::net::Ipv4Addr::new(0, 0, 0, 0), p);
        server(ch, a).await
    } else if let Some(a) = opt.addr {
        info!(mode = "client", "created ShenangoUdpSkChunnel");
        let jhs = FuturesUnordered::new();
        let start = Instant::now();
        for i in 0..num_cl {
            let jh = tokio::spawn(client(i, ch.clone(), a));
            jhs.push(async move { jh.await.unwrap() });
        }

        let durs: Result<Vec<_>, _> = jhs.try_collect().await;
        let time = start.elapsed();
        let mut durs: Vec<_> = durs?.into_iter().flatten().collect();
        durs.sort();
        let len = durs.len() as f64;
        let quantile_idxs = [0.25, 0.5, 0.75, 0.95];
        let quantiles: Vec<_> = quantile_idxs
            .iter()
            .map(|q| (len * q) as usize)
            .map(|i| durs[i])
            .collect();
        let num = durs.len() as f64;
        info!(
            ?num, ?time,
            min = ?durs[0], p25 = ?quantiles[0], p50 = ?quantiles[1],
            p75 = ?quantiles[2], p95 = ?quantiles[3], max = ?durs[durs.len() - 1],
            "Did accesses"
        );

        Ok(())
    } else {
        Err(eyre!(
            "Did not specify port for server or address for client"
        ))
    }
}
