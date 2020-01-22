//! RPC utility library that connects either to a remote machine
//! or to a local pipe depending on what burrito-ctl says
#![feature(test)]

use failure::Error;
use std::convert::TryInto;

mod ping {
    tonic::include_proto!("ping");
}

pub use ping::ping_server::{Ping, PingServer};
pub use ping::{ping_params::Work, PingParams, Pong};

#[derive(Clone, Debug)]
pub struct Server;

#[tonic::async_trait]
impl Ping for Server {
    async fn ping(
        &self,
        req: tonic::Request<PingParams>,
    ) -> Result<tonic::Response<Pong>, tonic::Status> {
        let then = std::time::Instant::now();
        let ping_req = req.into_inner();

        use ping::ping_params::Work;

        let w: Work = Work::from_i32(ping_req.work).ok_or_else(|| {
            tonic::Status::new(
                tonic::Code::InvalidArgument,
                format!("Unknown value {} for PingParams.Work", ping_req.work),
            )
        })?;

        let amt = ping_req.amount.try_into().expect("u64 to i64 cast");

        match w {
            Work::Immediate => (),
            Work::Const => {
                let completion_time = then + std::time::Duration::from_micros(amt);
                tokio::time::delay_until(tokio::time::Instant::from_std(completion_time)).await;
            }
            Work::Poisson => {
                let completion_time = then + gen_poisson_duration(amt as f64)?;
                tokio::time::delay_until(tokio::time::Instant::from_std(completion_time)).await;
            }
            Work::BusyTimeConst => {
                let completion_time = then + std::time::Duration::from_micros(amt);
                while std::time::Instant::now() < completion_time {
                    // spin
                }
            }
            Work::BusyWorkConst => {
                let k = 2350845.545;
                for i in 0..amt {
                    core::hint::black_box(f64::sqrt(k * i as f64));
                }
            }
        }

        Ok(tonic::Response::new(Pong {
            duration_us: then
                .elapsed()
                .as_micros()
                .try_into()
                .expect("u128 to i64 cast"),
        }))
    }
}

/// Issue many requests to a tonic endpoint.
pub async fn client_ping<A, C>(
    addr: A,
    connector: C,
    msg: PingParams,
    iters: usize,
) -> Result<Vec<(i64, i64)>, Error>
where
    A: std::convert::TryInto<tonic::transport::Endpoint> + Clone,
    A::Error: Send + Sync + std::error::Error + 'static,
    C: tower_make::MakeConnection<hyper::Uri> + Send + 'static + Clone,
    C::Connection: Unpin + Send + 'static,
    C::Future: Send + 'static,
    Box<dyn std::error::Error + Send + Sync>: From<C::Error> + Send + 'static,
{
    let mut durs = vec![];
    for _ in 0..iters {
        let ctr = connector.clone();
        let endpoint = addr.clone().try_into()?;
        let channel = endpoint.connect_with_connector(ctr).await?;
        let mut client = ping::ping_client::PingClient::new(channel);
        let then = std::time::Instant::now();
        let req = tonic::Request::new(msg.clone());
        let response = client.ping(req).await?;
        let elap = then.elapsed().as_micros().try_into()?;
        durs.push((elap, response.into_inner().duration_us));
    }

    Ok(durs)
}

fn gen_poisson_duration(amt: f64) -> Result<std::time::Duration, tonic::Status> {
    use rand_distr::{Distribution, Poisson};

    let mut rng = rand::thread_rng();
    let pois = Poisson::new(amt as f64).map_err(|e| {
        tonic::Status::new(
            tonic::Code::InvalidArgument,
            format!("Invalid amount {}: {:?}", amt, e),
        )
    })?;
    Ok(std::time::Duration::from_micros(pois.sample(&mut rng)))
}

#[cfg(test)]
mod test {
    use failure::Error;
    use slog::{debug, trace};

    #[test]
    // The sleeps are unfortunate, but there's not really a way to tell when
    // a server has started serving :(
    fn ping() -> Result<(), Error> {
        let log = test_logger();
        let mut rt = tokio::runtime::Runtime::new()?;

        rt.block_on(async move {
            let l = log.clone();
            tokio::spawn(async move { start_burrito_ctl(&l).await.expect("Burrito Ctl") });
            block_for(std::time::Duration::from_millis(200)).await;
            let l = log.clone();
            tokio::spawn(async move { start_sever_burrito(&l).await.expect("RPC Server") });
            block_for(std::time::Duration::from_millis(200)).await;

            trace!(&log, "connecting to burrito controller"; "burrito_root" => "./tmp-test-bn");
            let cl = burrito_addr::Client::new("./tmp-test-bn", &log).await?;
            let a: hyper::Uri = burrito_addr::Uri::new("test-rpcbench").into();
            super::client_ping(
                a,
                cl,
                super::PingParams {
                    work: super::Work::Immediate as i32,
                    amount: 0,
                },
                1,
            )
            .await?;

            Ok(())
        })
    }

    async fn start_sever_burrito(log: &slog::Logger) -> Result<(), Error> {
        let l2 = log.clone();

        // get serving address
        let srv = burrito_addr::Server::start("test-rpcbench", "./tmp-test-bn", Some(log)).await?;
        let ping_srv = super::PingServer::new(super::Server);

        trace!(l2, "spawning test-rpcbench");

        hyper::server::Server::builder(srv)
            .serve(hyper::service::make_service_fn(move |_| {
                let ps = ping_srv.clone();
                async move { Ok::<_, hyper::Error>(ps) }
            }))
            .await?;

        Ok(())
    }

    async fn start_burrito_ctl(log: &slog::Logger) -> Result<(), Error> {
        trace!(&log, "removing"; "dir" => "./tmp-test-bn/");
        std::fs::remove_dir_all("./tmp-test-bn/").unwrap_or_default();
        trace!(&log, "creating"; "dir" => "./tmp-test-bn/");
        std::fs::create_dir_all("./tmp-test-bn/")?;
        use burrito_ctl::BurritoNet;
        let bn = BurritoNet::new(
            Some(std::path::PathBuf::from("./tmp-test-bn/")),
            log.clone(),
        );

        let burrito_addr = bn.listen_path();
        debug!(&log, "burrito_addr"; "addr" => ?&burrito_addr);
        use hyper_unix_connector::UnixConnector;
        let uc: UnixConnector = tokio::net::UnixListener::bind(&burrito_addr)?.into();
        let bn = bn.start()?;
        let burrito_rpc_server =
            hyper::server::Server::builder(uc).serve(hyper::service::make_service_fn(move |_| {
                let bs = bn.clone();
                async move { Ok::<_, hyper::Error>(bs) }
            }));

        let l2 = log.clone();
        trace!(l2, "spawning burrito_ctl");
        burrito_rpc_server.await?;
        Ok(())
    }

    async fn block_for(d: std::time::Duration) {
        tokio::time::delay_for(d).await;
    }

    pub(crate) fn test_logger() -> slog::Logger {
        use slog::Drain;
        let plain = slog_term::PlainSyncDecorator::new(slog_term::TestStdoutWriter);
        let drain = slog_term::FullFormat::new(plain).build().fuse();
        slog::Logger::root(drain, slog::o!())
    }
}
