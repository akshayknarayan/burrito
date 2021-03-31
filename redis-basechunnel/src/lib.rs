// Pin<Box<...>> is necessary and not worth breaking up
#![allow(clippy::type_complexity)]

use bertha::negotiate::{NegotiateRendezvousResult, Rendezvous, RendezvousEntry};
use color_eyre::eyre::{ensure, eyre, Report, WrapErr};
use futures_util::stream::StreamExt;
use std::{future::Future, pin::Pin};
use tracing::{debug, debug_span};
use tracing_futures::Instrument;

pub struct RedisBase {
    redis_conn: redis::aio::Connection,
    redis_pubsub: redis::aio::PubSub,
}

impl RedisBase {
    pub async fn new(redis_addr: &str) -> Result<Self, Report> {
        async fn get(redis_addr: &str) -> Result<redis::aio::Connection, Report> {
            let redis_client = redis::Client::open(redis_addr)
                .wrap_err(eyre!("Opening redis connection: {:?}", redis_addr))?;
            let redis_conn = redis_client
                .get_async_connection()
                .await
                .wrap_err("Connecting to redis")?;
            Ok(redis_conn)
        }

        Ok(RedisBase {
            redis_conn: get(redis_addr).await?,
            redis_pubsub: get(redis_addr).await?.into_pubsub(),
        })
    }
}

impl Rendezvous for RedisBase {
    type Error = Report;

    fn negotiate<'a>(
        &'a mut self,
        addr: String,
        offer: RendezvousEntry,
        new: bool,
    ) -> Pin<Box<dyn Future<Output = Result<NegotiateRendezvousResult, Self::Error>> + Send + 'a>>
    {
        let o = offer.clone();
        let mut addr_ctr = addr.clone();
        addr_ctr.push_str("-conn-ctr");
        Box::pin(
            async move {
                let neg_nonce = bincode::serialize(&offer).wrap_err("serialize entry")?;
                let mut r = redis::pipe();
                let (set_res, nonce_val, conn_count): (isize, Vec<u8>, usize) = r
                    .atomic()
                    .set_nx(&addr, &neg_nonce[..])
                    .get(addr.clone())
                    .incr(&addr_ctr, if new { 1usize } else { 0usize })
                    .query_async(&mut self.redis_conn)
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
                                .query_async(&mut self.redis_conn)
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

    fn notify<'a>(
        &'a mut self,
        addr: String,
        curr_entry: RendezvousEntry,
    ) -> Pin<Box<dyn Future<Output = Result<NegotiateRendezvousResult, Self::Error>> + Send + 'a>>
    {
        let a = addr.clone();
        Box::pin(
            async move {
                let channel_name = format!("__keyspace@*__:{}", &addr);
                self.redis_pubsub.psubscribe(&channel_name).await?;
                // wait for some event on the key.
                let _ = self.redis_pubsub.on_message().next().await;
                debug!("addr value changed");
                self.redis_pubsub.punsubscribe(&channel_name).await?;
                // return result of polling now.
                return self.negotiate(addr, curr_entry, false).await;
            }
            .instrument(debug_span!("redis change negotiate", addr = ?&a)),
        )
    }

    fn commit_upgrade<'a>(
        &'a mut self,
        addr: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        let a = addr.clone();
        Box::pin(
            async move {
                let mut addr_ctr_key = addr.clone();
                addr_ctr_key.push_str("-conn-ctr");
                let mut staged_update_key = addr.clone();
                staged_update_key.push_str("-staged-update");
                let mut r = redis::pipe();
                let (addr_ctr, staged_updates): (usize, usize) = r
                    .atomic()
                    .get(&addr_ctr_key)
                    .incr(&staged_update_key, 1)
                    .query_async(&mut self.redis_conn)
                    .await?;

                debug!(?staged_updates, ?addr_ctr, "incremented");
                use std::cmp::Ordering;
                match staged_updates.cmp(&addr_ctr) {
                    Ordering::Less => {
                        // wait on notify
                        let channel_name = format!("__keyspace@*__:{}", &staged_update_key);
                        self.redis_pubsub.psubscribe(&channel_name).await?;
                        let mut staged_updates = staged_updates;
                        loop {
                            debug!("waiting on further updates");
                            // redis subscribe will never return None.
                            let msg = self.redis_pubsub.on_message().next().await.unwrap();
                            let event = msg.get_payload::<String>()?;
                            debug!(?event, "staged-update value changed");
                            let mut done = false;
                            if event.contains("del") {
                                debug!("del occurred");
                                done = true;
                            } else if event.contains("incrby") {
                                staged_updates += 1;
                                debug!(?staged_updates, ?addr_ctr, "increment occurred");
                                if staged_updates == addr_ctr {
                                    done = true;
                                }
                            }

                            if done {
                                self.redis_pubsub.punsubscribe(&channel_name).await?;
                                debug!("committing");
                                return Ok(());
                            }
                        }
                    }
                    Ordering::Equal => {
                        let _ = r
                            .atomic()
                            .del(&staged_update_key)
                            .query_async(&mut self.redis_conn)
                            .await?;
                        debug!("committing");
                        Ok(())
                    }
                    _ => {
                        unreachable!();
                    }
                }
            }
            .instrument(debug_span!("redis commit_upgrade", addr = ?&a)),
        )
    }
}
