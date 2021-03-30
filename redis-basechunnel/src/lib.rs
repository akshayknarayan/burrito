// Pin<Box<...>> is necessary and not worth breaking up
#![allow(clippy::type_complexity)]

use bertha::negotiate::{NegotiateRendezvousResult, Rendezvous, RendezvousEntry};
use color_eyre::eyre::{ensure, eyre, Report, WrapErr};
use std::sync::Arc;
use std::{future::Future, pin::Pin};
use tokio::sync::Mutex;
use tracing::{debug, debug_span};
use tracing_futures::Instrument;

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
    type Future =
        Pin<Box<dyn Future<Output = Result<NegotiateRendezvousResult, Self::Error>> + Send + 'a>>;

    fn negotiate(&'a mut self, addr: String, offer: RendezvousEntry, new: bool) -> Self::Future {
        let o = offer.clone();
        let mut addr_ctr = addr.clone();
        addr_ctr.push_str("-conn-ctr");
        Box::pin(
            async move {
                let neg_nonce = bincode::serialize(&offer).wrap_err("serialize entry")?;
                let mut redis_conn = self.redis_conn.lock().await;
                let mut r = redis::pipe();
                let (set_res, nonce_val, conn_count): (isize, Vec<u8>, usize) = r
                    .atomic()
                    .set_nx(&addr, &neg_nonce[..])
                    .get(addr.clone())
                    .incr(&addr_ctr, if new { 1usize } else { 0usize })
                    .query_async(&mut *redis_conn)
                    .await?;
                if set_res == 1 {
                    // was set
                    // anything supercedes a null entry.
                    debug!(res = "new_entry", "polled redis");
                    ensure!(
                        conn_count == 1,
                        "KV store polluted: {:?} != 1 on setnx",
                        conn_count
                    );
                    ensure!(nonce_val == neg_nonce, "Nonce values mismatched");
                    Ok(NegotiateRendezvousResult::Matched)
                } else if set_res == 0 {
                    // was not set
                    let nonce_val: RendezvousEntry =
                        bincode::deserialize(&nonce_val).wrap_err("deserialize entry")?;
                    debug!(
                        res = "existed",
                        was_multi = nonce_val.multi,
                        ?conn_count,
                        "polled redis"
                    );

                    // `multi = false` + `multi = false` => `multi = true`
                    match (nonce_val, offer) {
                        (
                            nv @ RendezvousEntry { multi: true, .. },
                            RendezvousEntry { multi: false, .. },
                        ) => {
                            // we are superceded by nonce_val.
                            Ok(NegotiateRendezvousResult::Superceded(nv))
                        }
                        (
                            RendezvousEntry { multi: false, .. },
                            RendezvousEntry { multi: true, .. },
                        ) => {
                            // we supercede. overwrite.
                            let mut r = redis::pipe();
                            r.atomic()
                                .set(&addr, &neg_nonce[..])
                                .query_async(&mut *redis_conn)
                                .await?;
                            Ok(NegotiateRendezvousResult::Matched)
                        }
                        (
                            RendezvousEntry { multi: false, .. },
                            RendezvousEntry { multi: false, .. },
                        ) if conn_count >= 3 => {
                            // both we and the existing value believe we are alone, and there are
                            // at least 3 clients. we need an upgrade.
                            Ok(NegotiateRendezvousResult::NeedUpgrade)
                        }
                        (
                            nv @ RendezvousEntry { .. },
                            RendezvousEntry {
                                nonce: our_nonce, ..
                            },
                        ) => {
                            if nv.nonce == our_nonce {
                                // either we set the same thing, or no one else has set since our last time.
                                Ok(NegotiateRendezvousResult::Matched)
                            } else {
                                // nonces didn't match, but multi-mode did. weird. maybe we're trying
                                // to access using a wrong stack. we defer to what's already
                                // registered.
                                Ok(NegotiateRendezvousResult::Superceded(nv))
                            }
                        }
                    }
                } else {
                    unreachable!()
                }
            }
            .instrument(debug_span!("redis poll negotiate", offer = ?&o)),
        )
    }
}
