use bertha::{
    bincode::SerializeChunnelProject,
    reliable::ReliabilityProjChunnel,
    tagger::OrderedChunnelProj,
    udp::{UdpReqChunnel, UdpSkChunnel},
    util::ProjectLeft,
    ChunnelConnection, ChunnelConnector, ChunnelListener, CxList,
};
use color_eyre::eyre::{eyre, Report, WrapErr};
use futures_util::stream::{FuturesUnordered, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use structopt::StructOpt;
use tracing::{debug, debug_span, info, instrument, trace};
use tracing_error::ErrorLayer;
use tracing_futures::Instrument;
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
async fn server(addr: SocketAddr, reliable: bool) -> Result<(), Report> {
    let st = UdpReqChunnel::default()
        .listen(addr)
        .await
        .wrap_err("Listen on UdpReqChunnel")?;
    if reliable {
        let stack = CxList::from(OrderedChunnelProj::default())
            .wrap(ReliabilityProjChunnel::<_, _>::default())
            .wrap(SerializeChunnelProject::<_, _>::default());
        let st = bertha::negotiate::negotiate_server(stack, st)
            .instrument(tracing::info_span!("negotiate_server"))
            .await
            .wrap_err("negotiate_server")?;
        st.try_for_each_concurrent(None, |cn| {
            async move {
                let mut sends = FuturesUnordered::new();
                loop {
                    trace!("loop start");
                    tokio::select! (
                        Ok(m) = cn.recv() => {
                            trace!("loop recvd");
                            let m: (_, Msg) = m;
                            let f = cn.send(m);
                            sends.push(f);
                            trace!("loop push send");
                        },
                        Some(_) = sends.next() => {
                            trace!("loop sent");
                        },
                    );
                    trace!("loop end");
                }
            }
            .instrument(tracing::info_span!("server_conn"))
        })
        .await
        .wrap_err("Error while processing requests")?;
        unreachable!()
    } else {
        let stack = CxList::from(OrderedChunnelProj::default())
            .wrap(SerializeChunnelProject::<_, _>::default());
        let st = bertha::negotiate::negotiate_server(stack, st)
            .instrument(tracing::info_span!("negotiate_server"))
            .await
            .wrap_err("negotiate_server")?;
        st.try_for_each_concurrent(None, |cn| {
            async move {
                let mut sends = FuturesUnordered::new();
                loop {
                    tokio::select! (
                        Ok(m) = cn.recv() => {
                            let m: (_, Msg) = m;
                            let f = cn.send(m);
                            sends.push(f);
                        },
                        Some(_) = sends.next() => {},
                    );
                }
            }
            .instrument(tracing::info_span!("server_conn"))
        })
        .await
        .wrap_err("Error while processing requests")?;
        unreachable!()
    };
}

#[instrument]
async fn client(addr: SocketAddr, reliable: bool) -> Result<Vec<Duration>, Report> {
    async fn reqs(cn: impl ChunnelConnection<Data = Msg>) -> Result<Vec<Duration>, Report> {
        let mut durs = vec![];
        for _ in 0..100_000 {
            cn.send(Msg::new()).await.wrap_err("client send")?;
            let m = cn.recv().await.wrap_err("client recv")?;
            durs.push(m.elapsed());
        }

        Ok(durs)
    }

    let raw_cn = UdpSkChunnel::default().connect(()).await?;
    debug!("make client");
    if reliable {
        let stack = CxList::from(ProjectLeft::from(addr))
            .wrap(OrderedChunnelProj::default())
            .wrap(ReliabilityProjChunnel::default())
            .wrap(SerializeChunnelProject::default());

        debug!("negotiation");
        let cn = bertha::negotiate::negotiate_client(stack, raw_cn, addr)
            .instrument(debug_span!("client_negotiate"))
            .await?;
        reqs(cn).await
    } else {
        let stack = CxList::from(ProjectLeft::from(addr))
            .wrap(OrderedChunnelProj::default())
            .wrap(SerializeChunnelProject::default());

        debug!("negotiation");
        let cn = bertha::negotiate::negotiate_client(stack, raw_cn, addr)
            .instrument(debug_span!("client_negotiate"))
            .await?;
        reqs(cn).await
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "echo")]
struct Opt {
    #[structopt(short, long)]
    port: Option<u16>,

    #[structopt(short, long)]
    addr: Option<SocketAddr>,

    #[structopt(short, long)]
    reliable: bool,
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    color_eyre::install()?;
    let subscriber = tracing_subscriber::registry();
    let timing_layer = tracing_timing::Builder::default()
        .no_span_recursion()
        .layer(|| tracing_timing::Histogram::new_with_max(1_000_000, 2).unwrap());
    let timing_downcaster = timing_layer.downcaster();
    let subscriber = subscriber
        .with(timing_layer)
        //.with(tracing_subscriber::fmt::layer())
        //.with(tracing_subscriber::EnvFilter::from_default_env())
        .with(ErrorLayer::default());
    let d = tracing::Dispatch::new(subscriber);
    d.clone().init();
    let d1 = d.clone();
    ctrlc::set_handler(move || {
        let timing = timing_downcaster
            .downcast(&d1)
            .expect("downcast timing layer");
        dump_tracing(timing);
    })
    .expect("set ctrlc handler");

    let opt = Opt::from_args();

    async move {
        tokio::time::delay_for(Duration::from_millis(1)).await;
        trace!("1 ms later");
    }
    .instrument(tracing::info_span!("calibration_span"))
    .await;

    if let Some(p) = opt.port {
        let a = ([0, 0, 0, 0], p).into();
        server(a, opt.reliable).await
    } else if let Some(a) = opt.addr {
        let jhs = FuturesUnordered::new();
        let start = Instant::now();
        for _ in 0..8 {
            let jh = tokio::spawn(client(a, opt.reliable));
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
        println!(
            "Did accesses \
            num = {}, \
            time = {:?}, \
            min   = {:?}, \
            p25   = {:?}, \
            p50   = {:?}, \
            p75   = {:?}, \
            p95   = {:?}, \
            max   = {:?}",
            num,
            time,
            durs[0],
            quantiles[0],
            quantiles[1],
            quantiles[2],
            quantiles[3],
            durs[durs.len() - 1],
        );

        let timing = timing_downcaster
            .downcast(&d)
            .expect("downcast timing layer");
        dump_tracing(timing);
        Ok(())
    } else {
        Err(eyre!(
            "Did not specify port for server or address for client"
        ))
    }
}

fn dump_tracing(timing: &'_ tracing_timing::TimingLayer) {
    timing.force_synchronize();
    timing.with_histograms(|hs| {
        for (span_group, hs) in hs {
            for (event_group, h) in hs {
                let tag = format!("{}:{}", span_group, event_group);
                println!(
                    "tracing: \
                        event = {}, \
                        min   = {}, \
                        p25   = {}, \
                        p50   = {}, \
                        p75   = {}, \
                        p95   = {}, \
                        max   = {}, \
                        cnt   = {} \
                        ",
                    &tag,
                    h.min(),
                    h.value_at_quantile(0.25),
                    h.value_at_quantile(0.5),
                    h.value_at_quantile(0.75),
                    h.value_at_quantile(0.95),
                    h.max(),
                    h.len(),
                );
            }
        }
    });

    std::process::exit(0);
}
