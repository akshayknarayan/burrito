//! RPC utility library that connects either to a remote machine
//! or to a local pipe depending on what burrito-ctl says

use failure::Error;
use std::convert::TryInto;
use tracing::{span, trace, Level};
use tracing_futures::Instrument;

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
        let span = span!(Level::DEBUG, "ping()", req = ?req);
        let _span = span.enter();
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
                // copy from shenango:
                // https://github.com/shenango/shenango/blob/master/apps/synthetic/src/fakework.rs#L54
                let k = 2350845.545;
                for i in 0..amt {
                    criterion::black_box(f64::sqrt(k * i as f64));
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
#[tracing::instrument(skip(addr, connector))]
pub async fn client_ping<A, C>(
    addr: A,
    connector: C,
    msg: PingParams,
    iters: usize,
    reqs_per_iter: usize,
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
    for i in 0..iters {
        trace!(iter = i, "start_loop");
        let ctr = connector.clone();
        let endpoint = addr.clone().try_into()?;

        let then = std::time::Instant::now();
        let channel = endpoint
            .tcp_nodelay(false)
            .connect_with_connector(ctr)
            .instrument(span!(Level::DEBUG, "connector"))
            .await?;
        trace!(iter = i, "connected");
        let mut client = ping::ping_client::PingClient::new(channel);

        for j in 0..reqs_per_iter {
            trace!(iter = i, which = j, "ping_start");
            let (tot, srv) = do_one_ping(&mut client, msg.clone()).await?;
            trace!(iter = i, which = j, "ping_end");
            durs.push((tot, srv));
        }

        let elap: i64 = then.elapsed().as_micros().try_into()?;
        trace!(iter = i, overall_time = elap, "end_loop");
    }

    Ok(durs)
}

async fn do_one_ping<T>(
    client: &mut ping::ping_client::PingClient<T>,
    msg: PingParams,
) -> Result<(i64, i64), Error>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::ResponseBody: tonic::body::Body + http_body::Body + Send + 'static,
    T::Error: Into<Box<dyn std::error::Error>>,
    <T::ResponseBody as http_body::Body>::Error:
        Into<Box<dyn std::error::Error + Send + Sync + 'static>> + Send,
{
    let req = tonic::Request::new(msg.clone());
    let then = std::time::Instant::now();
    let response = client
        .ping(req)
        .instrument(span!(Level::DEBUG, "tonic_ping"))
        .await?;
    let elap = then.elapsed().as_micros().try_into()?;
    let srv_dur = response.into_inner().duration_us;
    Ok((elap, srv_dur))
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
