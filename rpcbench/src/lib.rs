//! RPC utility library that connects either to a remote machine
//! or to a local pipe depending on what burrito-ctl says

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
