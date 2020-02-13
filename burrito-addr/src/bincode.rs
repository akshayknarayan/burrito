use super::uri::Uri;
use super::{Addr, Conn};
use burrito_ctl::SRMsg;
use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use failure::{bail, Error};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::trace;

/// A burrito resolver client that connects to a UDS server.
/// The wire protocol prefixes messages with their length in u32, followed by
/// [`bincode`](https://crates.io/crates/bincode)-encoded [`SRMsg`].
#[derive(Debug, Clone)]
pub struct StaticClient {
    burrito_root: PathBuf,
    uc: Arc<Mutex<tokio::net::UnixStream>>,
}

impl StaticClient {
    pub async fn new(burrito_root: std::path::PathBuf) -> Self {
        let controller_addr = burrito_root.join(burrito_ctl::burrito_net::CONTROLLER_ADDRESS);
        let uc = Arc::new(Mutex::new(
            tokio::net::UnixStream::connect(controller_addr)
                .await
                .expect("connect failed"),
        ));
        StaticClient { burrito_root, uc }
    }

    /// Resolves a [`hyper::Uri`] to an [`Addr`].
    ///
    /// The [`hyper::Uri`] must have the uri scheme `burrito`.
    pub async fn resolve(&mut self, dst: hyper::Uri) -> Result<Addr, Error> {
        let dst_addr = Uri::socket_path(&dst)?;
        trace!(addr = ?&dst_addr, "Resolving_burrito_address");
        self.write_into_sk(&SRMsg(dst_addr, "burrito".into()))
            .await?;
        trace!("wrote into sk");
        let resp = self.read_from_sk().await?;
        let addr = resp.0;
        let addr_type = resp.1;
        trace!(resolved_addr = ?&addr, "Resolved_burrito_address");

        match addr_type.as_str() {
            "unix" => Ok(crate::conn::Addr::Unix(
                self.burrito_root
                    .join(addr)
                    .into_os_string()
                    .into_string()
                    .expect("OS string as valid string"),
            )),
            "tcp" => Ok(crate::conn::Addr::Tcp(addr)),
            _ => unreachable!(),
        }
    }

    async fn write_into_sk(&mut self, msg: &SRMsg) -> Result<(), Error> {
        use tokio::io::AsyncWriteExt;

        let mut uc = self.uc.lock().await;

        let req: Vec<u8> = bincode::serialize(&msg)?;
        let req_len = req.len() as u32;
        uc.write_all(&req_len.to_le_bytes()).await?;
        uc.write_all(&req).await?;
        Ok(())
    }

    async fn read_from_sk(&mut self) -> Result<SRMsg, Error> {
        use tokio::io::AsyncReadExt;
        let mut buf = [0u8; 128];

        let mut uc = self.uc.lock().await;

        let len_to_read = loop {
            uc.read_exact(&mut buf[0..4]).await?;

            // unsafe transmute ok because endianness is guaranteed to be the
            // same on either side of a UDS
            // will read only the firself.uc sizeof(u32) = 4 bytes
            let len_to_read: u32 = unsafe { std::mem::transmute_copy(&buf) };

            if len_to_read == 0 {
                tokio::task::yield_now().await;
                continue;
            } else if len_to_read > 128 {
                bail!("message size too large: {:?} > 128", len_to_read);
            } else {
                break len_to_read;
            }
        };

        uc.read_exact(&mut buf[0..len_to_read as usize]).await?;
        Ok(bincode::deserialize(&buf[0..len_to_read as usize])?)
    }
}

impl hyper::service::Service<hyper::Uri> for StaticClient {
    type Response = Conn;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, dst: hyper::Uri) -> Self::Future {
        let mut cl = self.clone();
        Box::pin(async move { cl.resolve(dst).await?.connect().await })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use burrito_ctl::static_net::*;

    #[test]
    fn static_unix_ping() {
        std::fs::remove_dir_all("./tmp-test-sr/").unwrap_or_default();
        std::fs::create_dir_all("./tmp-test-sr/").unwrap();

        // start the server
        std::thread::spawn(|| {
            let sr = StaticResolver::new(
                Some(std::path::PathBuf::from("./tmp-test-sr")),
                "127.0.0.1:4242",
                "tcp",
            );

            tokio::runtime::Builder::new()
                .basic_scheduler()
                .enable_all()
                .build()
                .unwrap()
                .block_on(sr.start())
                .expect("static burrito crashed")
        });

        std::thread::sleep(std::time::Duration::from_millis(100));

        // ping from a client
        tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let mut sc = StaticClient::new(std::path::PathBuf::from("./tmp-test-sr")).await;
                let addr: hyper::Uri = crate::Uri::new("staticping").into();
                let addr = sc.resolve(addr).await.unwrap();

                match addr {
                    crate::Addr::Tcp(addr) => assert_eq!(addr, "127.0.0.1:4242"),
                    _ => panic!("wrong address"),
                }
            });
    }
}
