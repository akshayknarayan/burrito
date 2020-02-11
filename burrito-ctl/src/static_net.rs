use failure::Error;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tracing::trace;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct SRMsg(pub String, pub String);

#[derive(Debug, Clone)]
pub struct StaticResolver {
    root: std::path::PathBuf,
    response: SRMsg,
}

impl StaticResolver {
    pub fn new(root: std::path::PathBuf, addr: &str, addrtype: &str) -> Self {
        StaticResolver {
            root,
            response: SRMsg(addr.into(), addrtype.into()),
        }
    }

    pub fn listen_path(&self) -> std::path::PathBuf {
        self.root.join(crate::burrito_net::CONTROLLER_ADDRESS)
    }

    #[allow(clippy::cognitive_complexity)]
    #[tracing::instrument(level = "debug")]
    pub async fn start(self) -> Result<(), Error> {
        // bind
        let mut sk = UnixListener::bind(&self.listen_path())?;
        trace!(addr = ?self.listen_path(), "bound to addr");

        let r_msg: Vec<u8> = bincode::serialize(&self.response).unwrap();
        let mut r = (r_msg.len() as u32).to_le_bytes().to_vec();
        r.extend(r_msg);
        let r = r; // no more mut

        // listen
        sk.incoming()
            .for_each_concurrent(None, |st| {
                async {
                    let mut buf = [0u8; 128];
                    let mut st = st.expect("No connection");
                    trace!(conn = ?st, "new incoming conn");
                    loop {
                        if st.read_exact(&mut buf[0..4]).await.is_err() {
                            return;
                        }

                        // unsafe transmute ok because endianness is guaranteed to be the
                        // same on either side of a UDS
                        // will read only the first sizeof(u32) = 4 bytes
                        let len_to_read: u32 = unsafe { std::mem::transmute_copy(&buf) };

                        if len_to_read == 0 {
                            continue;
                        } else if len_to_read > 128 {
                            trace!(declared_len = len_to_read, "message too big");
                            return;
                        }

                        if st
                            .read_exact(&mut buf[0..len_to_read as usize])
                            .await
                            .is_err()
                        {
                            return;
                        }

                        let dec: SRMsg = match bincode::deserialize(&buf[0..len_to_read as usize]) {
                            Ok(m) => m,
                            _ => {
                                tokio::task::yield_now().await;
                                continue;
                            }
                        };

                        trace!(recvd_msg = ?dec, "read from conn");

                        if st.write_all(&r).await.is_err() {
                            break;
                        }

                        trace!("wrote to conn");
                    }

                    trace!(conn = ?st, "conn closed");
                }
            })
            .await;
        Ok(())
    }
}
