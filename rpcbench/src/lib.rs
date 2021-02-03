//! RPC pings.

#![warn(clippy::all)]

use bertha::ChunnelConnection;
use color_eyre::eyre::{bail, eyre, Report};
use futures_util::stream::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::future::Future;
use std::str::FromStr;
use std::sync::{atomic::AtomicUsize, Arc};
use std::time::Duration;
use tracing::{debug, info, instrument, trace};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum Work {
    Immediate,
    Const(u64),
    Poisson(u64),
    BusyTimeConst(u64),
    BusyWorkConst(u64),
}

impl FromStr for Work {
    type Err = Report;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let sp: Vec<_> = s.split(':').collect();
        match &sp[..] {
            [variant] if *variant == "immediate" || *variant == "imm" => Ok(Work::Immediate),
            [variant, amt] if *variant == "const" => Ok(Work::Const(amt.parse()?)),
            [variant, amt] if *variant == "poisson" => Ok(Work::Poisson(amt.parse()?)),
            [variant, amt] if *variant == "busytime" || *variant == "bt" => {
                Ok(Work::BusyTimeConst(amt.parse()?))
            }
            [variant, amt] if *variant == "busywork" || *variant == "bw" => {
                Ok(Work::BusyWorkConst(amt.parse()?))
            }
            x => {
                bail!("Could not parse work: {:?}", x)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PingParams {
    pub work: Work,
    pub padding: Vec<u8>,
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct Pong {
    pub duration_us: i64,
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub enum Msg {
    Ping(PingParams),
    Pong(Pong),
}

impl From<Pong> for Msg {
    fn from(p: Pong) -> Self {
        Msg::Pong(p)
    }
}

#[derive(Clone, Debug)]
pub struct Server {
    req_cnt: Arc<AtomicUsize>,
}

impl Default for Server {
    fn default() -> Self {
        Self {
            req_cnt: Arc::new(0.into()),
        }
    }
}

impl Server {
    pub fn get_counter(&self) -> Arc<AtomicUsize> {
        self.req_cnt.clone()
    }

    pub async fn do_ping(&mut self, ping_req: PingParams) -> Result<Pong, Report> {
        let then = std::time::Instant::now();

        self.req_cnt
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        match ping_req.work {
            Work::Immediate => (),
            Work::Const(amt) => {
                tokio::time::sleep(Duration::from_micros(amt)).await;
            }
            Work::Poisson(amt) => {
                tokio::time::sleep(gen_poisson_duration(amt as f64)?).await;
            }
            Work::BusyTimeConst(amt) => {
                let completion_time = then + Duration::from_micros(amt);
                while std::time::Instant::now() < completion_time {
                    // spin
                }
            }
            Work::BusyWorkConst(amt) => {
                // copy from shenango:
                // https://github.com/shenango/shenango/blob/master/apps/synthetic/src/fakework.rs#L54
                let k = 2350845.545;
                for i in 0..amt {
                    criterion::black_box(f64::sqrt(k * i as f64));
                }
            }
        }

        Ok(Pong {
            duration_us: then
                .elapsed()
                .as_micros()
                .try_into()
                .expect("u128 to i64 cast"),
        })
    }

    #[instrument(skip(l), err)]
    pub async fn serve<A>(
        self,
        l: impl futures_util::stream::Stream<
            Item = Result<impl ChunnelConnection<Data = (A, crate::Msg)>, Report>,
        >,
    ) -> Result<(), Report> {
        l.try_for_each_concurrent(None, |cn| {
            let mut srv = self.clone();
            async move {
                debug!("New connection");
                loop {
                    let (a, p) = match cn.recv().await? {
                        (a, Msg::Ping(p)) => (a, p),
                        _ => bail!("Didn't receive PingParams"),
                    };
                    let ans: Pong = srv.do_ping(p).await.unwrap();
                    cn.send((a, ans.into())).await?;
                }
            }
        })
        .await?;
        Ok(())
    }
}

#[tracing::instrument(skip(addr, connector))]
pub async fn client_ping<A, C, F, S>(
    addr: A,
    connector: C,
    msg: PingParams,
    iters: usize,
    reqs_per_iter: usize,
) -> Result<Vec<(Duration, i64, i64)>, Report>
where
    A: Clone,
    C: Fn(A) -> F,
    F: Future<Output = Result<S, Report>>,
    S: ChunnelConnection<Data = Msg> + Send + 'static,
{
    let start = std::time::Instant::now();
    let mut durs = vec![];
    for i in 0..iters {
        trace!(iter = i, "start_loop");

        let then = std::time::Instant::now();
        let st = connector(addr.clone()).await?;
        trace!(iter = i, "connected");
        for j in 0..reqs_per_iter {
            trace!(iter = i, which = j, "ping_start");
            let (tot, srv) = do_one_ping(&st, msg.clone()).await?;
            trace!(iter = i, which = j, "ping_end");
            durs.push((start.elapsed(), tot, srv));
        }

        let elap: i64 = then.elapsed().as_micros().try_into()?;
        trace!(iter = i, overall_time = elap, "end_loop");
    }

    dump_durs(&durs);

    Ok(durs)
}

async fn do_one_ping(
    cn: &impl ChunnelConnection<Data = Msg>,
    msg: PingParams,
) -> Result<(i64, i64), Report> {
    let then = std::time::Instant::now();
    cn.send(Msg::Ping(msg)).await?;
    let response = match cn.recv().await? {
        Msg::Pong(p) => p,
        _ => bail!("Didn't receive Pong"),
    };

    let elap = then.elapsed().as_micros().try_into()?;
    let srv_dur = response.duration_us;
    Ok((elap, srv_dur))
}

fn gen_poisson_duration(amt: f64) -> Result<std::time::Duration, Report> {
    let mut rng = rand::thread_rng();
    use rand_distr::{Distribution, Poisson};
    let pois = Poisson::new(amt as f64).map_err(|e| eyre!("Invalid amount {}: {:?}", amt, e))?;
    Ok(std::time::Duration::from_micros(pois.sample(&mut rng)))
}

pub fn dump_durs(durs: &[(Duration, i64, i64)]) {
    let mut just_durs: Vec<_> = durs.iter().map(|(x, _, _)| x).collect();
    just_durs.sort();
    let len = just_durs.len() as f64;
    let quantile_idxs = [0.25, 0.5, 0.75, 0.95];
    let quantiles: Vec<_> = quantile_idxs
        .iter()
        .map(|q| (len * q) as usize)
        .map(|i| just_durs[i])
        .collect();
    info!(?quantiles, "done");
}

#[cfg(test)]
mod test {
    use super::Report;
    use bertha::{
        bincode::SerializeChunnelProject,
        either::Either,
        negotiate_client, negotiate_server,
        udp::{UdpReqChunnel, UdpSkChunnel},
        uds::UnixSkChunnel,
        util::ProjectLeft,
        ChunnelConnector, ChunnelListener, CxList,
    };
    use burrito_localname_ctl::{ctl::serve_ctl, LocalNameChunnel};
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use tracing::{info, info_span, instrument};
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[test]
    fn rpcbench_ping() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or(());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let addr = "127.0.0.1:18649".parse().unwrap();
        let root = PathBuf::from("./tmp-test-ping/");
        test_util::reset_root_dir(&root);

        #[instrument]
        async fn server(root: PathBuf, addr: SocketAddr) -> Result<(), Report> {
            let srv = super::Server::default();
            let lch = LocalNameChunnel::new(
                root.clone(),
                Some(addr),
                UnixSkChunnel,
                SerializeChunnelProject::default(),
            )
            .await?;
            let stack = CxList::from(SerializeChunnelProject::default()).wrap(lch);
            info!("Serving localname mode");
            let st = negotiate_server(stack, UdpReqChunnel.listen(addr).await?).await?;
            srv.serve(st).await
        }

        rt.block_on(
            async move {
                // start ctl
                tokio::spawn(serve_ctl(Some(root.clone()), true));
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                tokio::spawn(server(root.clone(), addr));
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                let fncl = |addr| {
                    let r = root.clone();
                    async move {
                        let lch = LocalNameChunnel::new(
                            r,
                            None,
                            UnixSkChunnel,
                            SerializeChunnelProject::default(),
                        )
                        .await?;
                        let stack = CxList::from(SerializeChunnelProject::default()).wrap(lch);
                        let cn =
                            negotiate_client(stack, UdpSkChunnel.connect(()).await?, addr).await?;
                        let cn = ProjectLeft::new(Either::Left(addr), cn);
                        Ok(cn)
                    }
                };

                super::client_ping(
                    addr,
                    fncl,
                    super::PingParams {
                        work: super::Work::Immediate,
                        padding: vec![],
                    },
                    2, // iters
                    3, // reqs_per_iter
                )
                .await
            }
            .instrument(info_span!("rpcbench_ping")),
        )
        .unwrap();
    }
}
