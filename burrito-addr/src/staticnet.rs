use super::{rpc, Uri};
use burrito_ctl::SRMsg;
use failure::{bail, ensure, format_err, Error};
use std::path::PathBuf;
use tracing::trace;

/// For benchmarking purposes, a burrito resolver client that connects to a static,
/// fixed-buffer-write UDS server
pub struct StaticClient {
    burrito_root: PathBuf,
    uc: tokio::net::UnixStream,
}

impl StaticClient {
    pub async fn new(burrito_root: std::path::PathBuf) -> Self {
        let controller_addr = burrito_root.join(burrito_ctl::burrito_net::CONTROLLER_ADDRESS);
        let uc = tokio::net::UnixStream::connect(controller_addr)
            .await
            .expect("connect failed");
        StaticClient { burrito_root, uc }
    }

    pub async fn resolve(
        &mut self,
        dst: hyper::Uri,
    ) -> Result<(String, rpc::open_reply::AddrType), Error> {
        ensure!(
            dst.scheme_str().map(|s| s == "burrito").is_some(),
            "URL scheme does not match: {:?}",
            dst.scheme_str()
        );

        let dst_addr = Uri::socket_path(&dst)
            .ok_or_else(|| format_err!("Could not get socket path for Destination"))?;
        let dst_addr_log = dst_addr.clone();

        trace!(addr = ?&dst_addr, "Resolving_burrito_address");

        self.write_into_sk(&SRMsg(dst_addr, "burrito".into()))
            .await?;

        trace!("wrote into sk");

        let resp = self.read_from_sk().await?;
        let addr = resp.0;
        let addr_type = resp.1;

        trace!(addr = ?&dst_addr_log, addr_type = ?addr_type, resolved_addr = ?&addr, "Resolved_burrito_address");

        // It's somewhat unfortunate to have to match twice, once here and once in impl Service::call.
        // Could just return the string and handle there, but that would expose the message abstraction.
        match addr_type.as_str() {
            "unix" => Ok((
                self.burrito_root
                    .join(addr)
                    .into_os_string()
                    .into_string()
                    .expect("OS string as valid string"),
                rpc::open_reply::AddrType::Unix,
            )),
            "tcp" => Ok((addr, rpc::open_reply::AddrType::Tcp)),
            _ => unreachable!(),
        }
    }

    async fn write_into_sk(&mut self, msg: &SRMsg) -> Result<(), Error> {
        use tokio::io::AsyncWriteExt;

        let req: Vec<u8> = bincode::serialize(&msg)?;
        let req_len = req.len() as u32;
        self.uc.write_all(&req_len.to_le_bytes()).await?;
        self.uc.write_all(&req).await?;
        Ok(())
    }

    async fn read_from_sk(&mut self) -> Result<SRMsg, Error> {
        use tokio::io::AsyncReadExt;
        let mut buf = [0u8; 128];

        let len_to_read = loop {
            self.uc.read_exact(&mut buf[0..4]).await?;

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

        self.uc
            .read_exact(&mut buf[0..len_to_read as usize])
            .await?;

        Ok(bincode::deserialize(&buf[0..len_to_read as usize])?)
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
                std::path::PathBuf::from("./tmp-test-sr"),
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
                let (addr, atype) = sc.resolve(addr).await.unwrap();
                assert_eq!(addr, "127.0.0.1:4242");
                assert_eq!(atype, rpc::open_reply::AddrType::Tcp);
            });
    }
}
