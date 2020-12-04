//! Chunnel-enabled key-value protocol, server, and client.

mod client;
mod kv;
mod msg;
mod server;

pub use client::KvClient;
pub use msg::Msg;
pub use server::serve;

#[cfg(test)]
mod tests {
    use super::{serve, KvClient};
    use color_eyre::eyre::{eyre, Report, WrapErr};
    use std::net::SocketAddr;
    use tracing::{info, info_span};
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[test]
    fn put_get() -> Result<(), Report> {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or_else(|_| ());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                let redis_port = 61179;
                info!(port = ?redis_port, "start redis");
                let _redis_guard = test_util::start_redis(redis_port);

                let srv_port = 15125;
                let srv_addr = format!("127.0.0.1:{}", 15125);

                let redis_sk_addr = SocketAddr::new("127.0.0.1".parse()?, redis_port);

                let (s, r) = tokio::sync::oneshot::channel();

                info!("start server");
                tokio::spawn(
                    serve(
                        bertha::udp::UdpReqChunnel::default(),
                        redis_sk_addr,
                        "127.0.0.1".parse()?,
                        srv_port,
                        2,
                        s,
                    )
                    .instrument(info_span!("server")),
                );

                r.await?;

                info!("make client");
                use bertha::ChunnelConnector;
                let raw_cn = bertha::udp::UdpSkChunnel::default().connect(()).await?;
                let client = KvClient::new_shardclient(raw_cn, redis_sk_addr, srv_addr.parse()?)
                    .instrument(info_span!("make kvclient"))
                    .await
                    .wrap_err("make KvClient")?;

                info!("do put");
                match client
                    .update(String::from("fooo"), String::from("barr"))
                    .await?
                {
                    None => {}
                    x => Err(eyre!("unexpected value from put {:?}", x))?,
                }

                info!("do get");
                match client.get(String::from("fooo")).await? {
                    Some(x) if x == "barr" => {}
                    x => Err(eyre!("unexpected value from get {:?}", x))?,
                }

                Ok(())
            }
            .instrument(info_span!("put_get")),
        )
    }
}
