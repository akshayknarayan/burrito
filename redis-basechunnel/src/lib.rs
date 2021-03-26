// Pin<Box<...>> is necessary and not worth breaking up
#![allow(clippy::type_complexity)]

use bertha::negotiate::{Rendezvous, RendezvousEntry};
use color_eyre::eyre::{ensure, eyre, Report, WrapErr};
use std::sync::Arc;
use std::{future::Future, pin::Pin};
use tokio::sync::Mutex;

pub struct RedisBase {
    redis_conn: Arc<Mutex<redis::aio::Connection>>,
}

impl RedisBase {
    pub async fn new(redis_addr: &str) -> Result<Self, Report> {
        let redis_client = redis::Client::open(redis_addr)
            .wrap_err(eyre!("Opening redis connection: {:?}", redis_addr))?;
        let redis_conn = Arc::new(Mutex::new(
            redis_client
                .get_async_connection()
                .await
                .wrap_err("Connecting to redis")?,
        ));

        Ok(RedisBase { redis_conn })
    }
}

impl<'a> Rendezvous<'a> for RedisBase {
    type Error = Report;
    type Future = Pin<Box<dyn Future<Output = Result<Option<RendezvousEntry>, Self::Error>> + 'a>>;

    fn negotiate(&'a mut self, addr: String, offer: RendezvousEntry) -> Self::Future {
        Box::pin(async move {
            let neg_nonce = bincode::serialize(&offer).wrap_err("serialize entry")?;
            let mut redis_conn = self.redis_conn.lock().await;
            let mut r = redis::pipe();
            let (set_res, nonce_val): (isize, Vec<u8>) = r
                .atomic()
                .set_nx(&addr, &neg_nonce[..])
                .get(addr.clone())
                .query_async(&mut *redis_conn)
                .await?;
            if set_res == 1 {
                // was set
                // anything supercedes a null entry.
                ensure!(nonce_val == neg_nonce, "Nonce values mismatched");
                Ok(None)
            } else if set_res == 0 {
                // was not set
                let nonce_val: RendezvousEntry =
                    bincode::deserialize(&nonce_val).wrap_err("deserialize entry")?;

                // `multi = true` supercedes `multi = false`.
                match (nonce_val, offer) {
                    (
                        nv @ RendezvousEntry { multi: true, .. },
                        RendezvousEntry { multi: false, .. },
                    ) => {
                        // we are superceded by nonce_val.
                        Ok(Some(nv))
                    }
                    (RendezvousEntry { multi: false, .. }, RendezvousEntry { multi: true, .. }) => {
                        // we supercede. overwrite.
                        let mut r = redis::pipe();
                        r.atomic()
                            .set(&addr, &neg_nonce[..])
                            .query_async(&mut *redis_conn)
                            .await?;
                        Ok(None)
                    }
                    (
                        nv @ RendezvousEntry { .. },
                        RendezvousEntry {
                            nonce: our_nonce, ..
                        },
                    ) => {
                        if nv.nonce == our_nonce {
                            // either we set the same thing, or no one else has set since our last time.
                            Ok(None)
                        } else {
                            // nonces didn't match, but multi-mode did. weird. maybe we're trying
                            // to access using a wrong stack. we defer to what's already
                            // registered.
                            Ok(Some(nv))
                        }
                    }
                }
            } else {
                unreachable!()
            }
        })
    }
}
