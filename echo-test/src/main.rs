use bertha::{
    bincode::SerializeChunnelProject, reliable::ReliabilityProjChunnel, tagger::OrderedChunnelProj,
    util::ProjectLeft, ChunnelConnection, ChunnelConnector, ChunnelListener, CxList,
};
use color_eyre::eyre::{eyre, Report, WrapErr};
use futures_util::stream::{FuturesUnordered, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use structopt::StructOpt;
use tracing::{debug, debug_span, info, instrument};
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
async fn server(
    addr: SocketAddr,
    reliable: bool,
    shenango_cfg: Option<std::path::PathBuf>,
) -> Result<(), Report> {
    #[cfg(not(feature = "use-shenango"))]
    fn get_raw(
        cfg: Option<PathBuf>,
    ) -> Result<
        impl ChunnelListener<
                Addr = SocketAddr,
                Connection = impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)>
                                 + Send
                                 + Sync
                                 + 'static,
                Error = impl Into<Report> + Send + Sync + 'static,
            > + Clone
            + Send
            + 'static,
        Report,
    > {
        if cfg.is_some() {
            tracing::warn!(cfg_file = ?cfg, "Shenango is disabled, ignoring config");
        }

        Ok(bertha::udp::UdpReqChunnel::default())
    }

    #[cfg(feature = "use-shenango")]
    fn get_raw(
        cfg: Option<PathBuf>,
    ) -> Result<
        impl ChunnelListener<
                Addr = SocketAddr,
                Connection = impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)>
                                 + Send
                                 + Sync
                                 + 'static,
                Error = impl Into<Report> + Send + Sync + 'static,
            > + Clone
            + Send
            + 'static,
        Report,
    > {
        if cfg.is_none() {
            return Err(eyre!(
                "If shenango feature is enabled, shenango_cfg must be specified"
            ));
        }

        let s = shenango_chunnel::ShenangoUdpSkChunnel::new(&cfg.unwrap());
        Ok(shenango_chunnel::ShenangoUdpReqChunnel(s))
    }

    let st = get_raw(shenango_cfg)?
        .listen(addr)
        .await
        .map_err(Into::into)
        .wrap_err("Listen on UdpReqChunnel")?;
    if reliable {
        let stack = CxList::from(OrderedChunnelProj::default())
            .wrap(ReliabilityProjChunnel::<_, _>::default())
            .wrap(SerializeChunnelProject::<_, _>::default());
        let st = bertha::negotiate::negotiate_server(stack, st)
            .instrument(tracing::info_span!("negotiate_server"))
            .await
            .wrap_err("negotiate_server")?;
        st.try_for_each_concurrent(None, |cn| async move {
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
        st.try_for_each_concurrent(None, |cn| async move {
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
        })
        .await
        .wrap_err("Error while processing requests")?;
        unreachable!()
    };
}

#[instrument]
async fn client(
    addr: SocketAddr,
    reliable: bool,
    shenango_cfg: Option<PathBuf>,
) -> Result<Vec<Duration>, Report> {
    #[cfg(feature = "use-shenango")]
    fn get_raw_connector(
        path: Option<PathBuf>,
    ) -> Result<
        impl ChunnelConnector<
                Addr = (),
                Connection = impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)>,
                Error = impl Into<Report> + Send + Sync + 'static,
            > + Clone,
        Report,
    > {
        let path = path.ok_or_else(|| {
            eyre!("If shenango feature is enabled, shenango_cfg must be specified")
        })?;
        Ok(shenango_chunnel::ShenangoUdpSkChunnel::new(&path))
    }

    #[cfg(not(feature = "use-shenango"))]
    fn get_raw_connector(
        p: Option<PathBuf>,
    ) -> Result<
        impl ChunnelConnector<
                Addr = (),
                Connection = impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)>,
                Error = impl Into<Report> + Send + Sync + 'static,
            > + Clone,
        Report,
    > {
        if p.is_some() {
            tracing::warn!(cfg_file = ?p, "Shenango is disabled, ignoring config");
        }

        Ok(bertha::udp::UdpSkChunnel::default())
    }

    async fn reqs(cn: impl ChunnelConnection<Data = Msg>) -> Result<Vec<Duration>, Report> {
        let mut durs = vec![];
        for _ in 0..100_000 {
            cn.send(Msg::new()).await.wrap_err("client send")?;
            let m = cn.recv().await.wrap_err("client recv")?;
            durs.push(m.elapsed());
        }

        Ok(durs)
    }

    let raw_cn = get_raw_connector(shenango_cfg)?
        .connect(())
        .await
        .map_err(Into::into)?;
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

    #[structopt(short, long, default_value = "1")]
    num_clients: usize,

    #[structopt(short, long)]
    shenango_cfg: Option<PathBuf>,
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

    if let Some(p) = opt.port {
        let a = ([0, 0, 0, 0], p).into();
        server(a, opt.reliable, opt.shenango_cfg).await
    } else if let Some(a) = opt.addr {
        let jhs = FuturesUnordered::new();
        let start = Instant::now();
        for _ in 0..opt.num_clients {
            let jh = tokio::spawn(client(a, opt.reliable, opt.shenango_cfg.clone()));
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
