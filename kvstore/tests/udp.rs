use tracing::debug;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

#[test]
fn single_shard_udp() -> Result<(), Error> {
    let mut rt = tokio::runtime::Runtime::new()?;
    tracing_subscriber::fmt::init();

    rt.block_on(async move {
        let sk = tokio::net::UdpSocket::bind("[::1]:47242").await.unwrap();
        debug!("spawning server");
        tokio::spawn(kvstore::shard_server_udp(vec![sk], |_| 0));

        use std::net::ToSocketAddrs;
        let cl = kvstore::UdpClientService::new(
            tokio::net::UdpSocket::bind("[::1]:0").await.unwrap(),
            "[::1]:47242".to_socket_addrs().unwrap().next().unwrap(),
        )
        .await
        .unwrap();

        let mut cl = kvstore::Client::from(cl);

        // do some stuff
        debug!("update");
        cl.update("foo".into(), "bar".into()).await.unwrap();
        debug!("get");
        assert_eq!(Some("bar".into()), cl.get("foo".into()).await.unwrap());

        Ok(())
    })
}

pub async fn block_for(d: std::time::Duration) {
    tokio::time::delay_for(d).await;
}
