//! Chunnel-enabled key-value protocol, server, and client.

mod client;
pub mod kv;
mod msg;
mod opt;
pub mod reliability;
mod server;

#[cfg(feature = "bin-helper")]
pub mod bin;

pub use client::{KvClient, KvClientBuilder};
pub use msg::{Msg, Op};
pub use server::{serve, serve_lb, single_shard};

#[cfg(test)]
mod tests {
    use super::{serve, serve_lb, single_shard, KvClient, KvClientBuilder};
    use bertha::{
        udp::{UdpReqChunnel, UdpSkChunnel},
        ChunnelConnector,
    };
    use color_eyre::eyre::{eyre, Report, WrapErr};
    use std::net::SocketAddr;
    use tracing::{info, info_span};
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    async fn putget<C: bertha::ChunnelConnection<Data = super::Msg> + Send + Sync + 'static>(
        client: KvClient<C>,
        op: (String, String),
    ) -> Result<(), Report> {
        info!("do put");
        match client.update(op.0.clone(), op.1.clone()).await? {
            None => {}
            x => return Err(eyre!("unexpected value from put {:?}", x)),
        }

        info!("do get");
        match client.get(op.0).await? {
            Some(x) if x == op.1 => {}
            x => return Err(eyre!("unexpected value from get {:?}", x)),
        }

        info!("client done");
        Ok(())
    }

    #[test]
    fn put_get() -> Result<(), Report> {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or(());

        let redis_port = 61179;
        info!(port = ?redis_port, "start redis");
        let _redis_guard = test_util::start_redis(redis_port);

        let srv_port = 15125;
        let srv_addr = format!("127.0.0.1:{}", 15125);
        let redis_sk_addr = SocketAddr::new("127.0.0.1".parse()?, redis_port);
        let (s, r) = tokio::sync::oneshot::channel();
        let listen_addr = "127.0.0.1".parse()?;
        info!("start server");
        std::thread::spawn(move || {
            let serve_span = info_span!("server");
            let _serve_span_g = serve_span.entered();
            if let Err(err) = serve(
                UdpReqChunnel::default(),
                redis_sk_addr,
                listen_addr,
                srv_port,
                2,
                Some(s),
                false,
            ) {
                tracing::error!(?err, "server errored");
            }
        });

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let srv_addr = srv_addr.parse()?;
        rt.block_on(
            async move {
                r.await?;

                async {
                    info!("make client");
                    let raw_cn = bertha::udp::UdpSkChunnel::default()
                        .connect(srv_addr)
                        .await?;
                    let client = KvClientBuilder::new(srv_addr)
                        .new_nonshardclient(raw_cn)
                        .instrument(info_span!("make kvclient"))
                        .await
                        .wrap_err("make basic KvClient")?;
                    putget(client, ("foooo".into(), "barrrr".into())).await?;
                    Ok::<_, Report>(())
                }
                .instrument(info_span!("basic client"))
                .await?;

                async {
                    info!("make shardclient");
                    let raw_cn = bertha::udp::UdpSkChunnel::default()
                        .connect(srv_addr)
                        .await?;
                    let client = KvClientBuilder::new(srv_addr)
                        .new_shardclient(raw_cn, redis_sk_addr)
                        .instrument(info_span!("make shard kvclient"))
                        .await
                        .wrap_err("make shard KvClient")?;
                    putget(client, ("bazzzzzz".into(), "quxxxxx".into())).await?;
                    Ok::<_, Report>(())
                }
                .instrument(info_span!("shard client"))
                .await?;

                Ok(())
            }
            .instrument(info_span!("put_get")),
        )
    }

    #[test]
    fn serve_manual() -> Result<(), Report> {
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
        rt.block_on(
            async move {
                let redis_port = 60179;
                info!(port = ?redis_port, "start redis");
                let _redis_guard = test_util::start_redis(redis_port);

                let srv_port = 12125;
                let srv_addr = format!("127.0.0.1:{}", 12125);
                let srv_addr: SocketAddr = srv_addr.parse()?;
                let redis_sk_addr =
                    SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), redis_port);

                let shard_internal_addr_from_external = |sa: SocketAddr| {
                    let mut internal_addr = sa;
                    internal_addr.set_port(sa.port() + 100);
                    internal_addr
                };

                let shard_addrs: Vec<_> = (1..=2)
                    .map(|i| SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), srv_port + i))
                    .collect();
                info!(?shard_addrs, "start shards");
                for sa in &shard_addrs {
                    let (s, r) = tokio::sync::oneshot::channel();
                    let internal_addr = shard_internal_addr_from_external(*sa);
                    tokio::spawn(
                        single_shard(
                            *sa,
                            UdpReqChunnel::default(),
                            Some(internal_addr),
                            Some(UdpReqChunnel::default()),
                            true,
                            s,
                            false,
                        )
                        .instrument(info_span!("shard", addr = ?sa)),
                    );
                    r.await?; // wait for shard, but can throw the offer away
                }

                info!(?srv_addr, "start lb");
                let shard_internal_addrs: Vec<_> = shard_addrs
                    .iter()
                    .copied()
                    .map(shard_internal_addr_from_external)
                    .collect();
                let (s, r) = tokio::sync::oneshot::channel();
                tokio::spawn(
                    serve_lb(
                        srv_addr,
                        shard_addrs,
                        UdpReqChunnel::default(),
                        shard_internal_addrs,
                        UdpSkChunnel::default(),
                        redis_sk_addr,
                        s,
                    )
                    .instrument(info_span!("server", addr = ?srv_addr)),
                );

                r.await?; // wait for lb to spawn

                async {
                    info!("make client");
                    let raw_cn = bertha::udp::UdpSkChunnel::default()
                        .connect(srv_addr)
                        .await?;
                    // TODO only works with new_nonshardclient, not basicclient
                    // for some reason the message is getting echoed.
                    let client = KvClientBuilder::new(srv_addr)
                        .new_nonshardclient(raw_cn)
                        .instrument(info_span!("make kvclient"))
                        .await
                        .wrap_err("make basic KvClient")?;
                    putget(client, ("foooo".into(), "barrrr".into())).await?;
                    Ok::<_, Report>(())
                }
                .instrument(info_span!("basic client"))
                .await?;
                Ok(())
            }
            .instrument(info_span!("serve_manual")),
        )
    }
}
