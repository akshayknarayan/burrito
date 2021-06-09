// Pin<Box<...>> is necessary and not worth breaking up
#![allow(clippy::type_complexity)]

use bertha::negotiate::{NegotiateRendezvousResult, RendezvousBackend, RendezvousEntry};
use color_eyre::eyre::{ensure, eyre, Report, WrapErr};
use futures_util::stream::StreamExt;
use std::{future::Future, pin::Pin};
use tracing::{debug, debug_span};
use tracing_futures::Instrument;

pub struct RedisBase {
    redis_conn: redis::aio::Connection,
    redis_pubsub: redis::aio::PubSub,
    liveness_timeout: std::time::Duration,
    // TODO: make this configurable, it could even be queryable and represent the client address if
    // needed
    client_name: String,
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
            liveness_timeout: std::time::Duration::from_secs(1),
            client_name: rand_name(),
        })
    }
}

impl RendezvousBackend for RedisBase {
    type Error = Report;

    /// Set semantics on `addr`, only if no value was previously set.
    ///
    /// If a value *was* previously set and the semantics match, joins the connection.
    /// Otherwise, returns `NegotiateRendezvousResult::NoMatch` and *does not* join the
    /// connection. The client can subsequently join the connection with `poll_entry`.
    fn try_init<'a>(
        &'a mut self,
        addr: String,
        offer: RendezvousEntry,
    ) -> Pin<Box<dyn Future<Output = Result<NegotiateRendezvousResult, Self::Error>> + Send + 'a>>
    {
        let a = addr.clone();
        let mut addr_ctr = addr.clone();
        addr_ctr.push_str("-members");
        Box::pin(
            async move {
                let neg_nonce = bincode::serialize(&offer).wrap_err("serialize entry")?;
                let expiry_time = (std::time::SystemTime::now() - self.liveness_timeout)
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                    .as_millis() as u64;
                // DO NOT zadd here, since we don't know if we're joining yet.
                // but DO zrembyscore, since what if the old one was timed out?
                let mut r = redis::pipe();
                let (setnx, got_value, removed, conn_count): (isize, Vec<u8>, isize, usize) = r
                    .atomic()
                    .set_nx(&addr, &neg_nonce[..])
                    .get(addr.clone())
                    .zrembyscore(&addr_ctr, 0, expiry_time)
                    .zcard(&addr_ctr)
                    .query_async(&mut self.redis_conn)
                    .await?;

                if setnx == 1 {
                    // was set
                    // anything supercedes a null entry.
                    debug!(res = "new_entry", "polled redis");
                    ensure!(
                        conn_count == 0,
                        "KV store polluted: {:?} != 0 on setnx",
                        conn_count
                    );
                    ensure!(got_value == neg_nonce, "Nonce values mismatched");

                    // now join with zadd. if either of the two ensures above failed, the next client will
                    // see the setnx fail but conn_count == 0, so it will override.
                    self.join(&addr_ctr).await?;
                    Ok(NegotiateRendezvousResult::Matched {
                        num_participants: 1,
                    })
                } else if setnx == 0 {
                    // was not set, which means someone was already here.
                    debug!(res = "existed", ?removed, ?conn_count, "polled redis");
                    if conn_count == 0 {
                        // setnx failed, but we're the only ones home. override.
                        // transition will call zadd.
                        self.transition(addr.clone(), offer).await?;
                        Ok(NegotiateRendezvousResult::Matched {
                            num_participants: 1,
                        })
                    } else if got_value == neg_nonce {
                        self.join(&addr_ctr).await?;
                        Ok(NegotiateRendezvousResult::Matched {
                            num_participants: conn_count,
                        })
                    } else {
                        // DO NOT join if semantics didn't match.
                        let present_semantics: RendezvousEntry =
                            bincode::deserialize(&got_value).wrap_err("deserialize entry")?;
                        Ok(NegotiateRendezvousResult::NoMatch {
                            entry: present_semantics,
                            num_participants: conn_count,
                        })
                    }
                } else {
                    unreachable!()
                }
            }
            .instrument(debug_span!("redis::try_init", addr = ?&a)),
        )
    }

    /// Query semantics on `addr`.
    ///
    /// Returns whether the semantics match `curr_entry` (`NegotiateRendezvousResult`), and updates
    /// (or initializes) the expiration timer for this endpoint in the connection.
    fn poll_entry<'a>(
        &'a mut self,
        addr: String,
        curr_entry: RendezvousEntry,
    ) -> Pin<Box<dyn Future<Output = Result<NegotiateRendezvousResult, Self::Error>> + Send + 'a>>
    {
        let a = addr.clone();
        let mut addr_ctr = addr.clone();
        addr_ctr.push_str("-members");
        Box::pin(
            async move {
                let insert_time = std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                    .as_millis() as u64;
                let expiry_time = (std::time::SystemTime::now() - self.liveness_timeout)
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                    .as_millis() as u64;
                let mut r = redis::pipe();
                let (got_value, _, _, conn_count): (Vec<u8>, isize, isize, usize) = r
                    .atomic()
                    .get(&addr)
                    .zrembyscore(&addr_ctr, 0, expiry_time)
                    .zadd(&addr_ctr, &self.client_name, insert_time as usize)
                    .zcard(&addr_ctr)
                    .query_async(&mut self.redis_conn)
                    .await?;

                let present_semantics: RendezvousEntry =
                    bincode::deserialize(&got_value).wrap_err("deserialize entry")?;
                if present_semantics == curr_entry {
                    Ok(NegotiateRendezvousResult::Matched {
                        num_participants: conn_count,
                    })
                } else {
                    Ok(NegotiateRendezvousResult::NoMatch {
                        entry: present_semantics,
                        num_participants: conn_count,
                    })
                }
            }
            .instrument(debug_span!("redis::poll", addr = ?&a)),
        )
    }

    /// After how long without a poll should this connection endpoint be considered dead?
    ///
    /// If < 1ms, uses 1ms.
    fn set_liveness_expiration(&mut self, expiration: std::time::Duration) {
        let expiration = std::cmp::max(expiration, std::time::Duration::from_millis(1));
        self.liveness_timeout = expiration;
    }

    /// Subscribe to the next event on this connection.
    ///
    /// In general, there are three cases to notify about:
    /// 1. A new participant has joined.
    /// 2. A participant left.
    /// 3. The semantics were transitioned.
    ///
    /// If a participant joined, we don't want to have a thundering-horde problem on possibly updating the
    /// semantics, so we just let that participant possibly transition semantics, turning thaat
    /// case into (3). For (2) this is unavoidable - we need the notification.
    ///
    /// Implementors can detect (2) with heartbeats (or timeouts) inside `notify`. e.g. for redis,
    /// using SETEX and/or EXPIRE.
    ///
    /// Default is a poll-based implementation. A more efficient (or correct!) implementation might use
    /// notifications instead.
    fn notify<'a>(
        &'a mut self,
        addr: String,
        curr_entry: RendezvousEntry,
    ) -> Pin<Box<dyn Future<Output = Result<NegotiateRendezvousResult, Self::Error>> + Send + 'a>>
    where
        Self: Send,
    {
        let a = addr.clone();
        let mut addr_ctr = addr.clone();
        addr_ctr.push_str("-members");
        Box::pin(
            async move {
                let channel_name = format!("__keyspace@*__:{}", &addr);
                self.redis_pubsub.psubscribe(&channel_name).await?;
                let channel_ctr_name = format!("__keyspace@*__:{}", &addr_ctr);
                self.redis_pubsub.psubscribe(&channel_ctr_name).await?;
                // wait for some event on the key.
                // TODO XXX also reset expiry at slightly less than expiry time.
                let _ = self.redis_pubsub.on_message().next().await;
                debug!("semantics changed");
                self.redis_pubsub.punsubscribe(&channel_name).await?;
                self.redis_pubsub.punsubscribe(&channel_ctr_name).await?;
                // return result of polling now.
                return self.poll_entry(addr, curr_entry).await;
            }
            .instrument(debug_span!("redis::notify", addr = ?&a)),
        )
    }

    /// Transition to the new semantics `new_entry` on `addr`.
    ///
    /// Begins a commit. Returns once the staged update counter reaches the number of unexpired
    /// partiparticipants.
    /// At that time the new semantics are in play.
    fn transition<'a>(
        &'a mut self,
        addr: String,
        new_entry: RendezvousEntry,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        let a = addr.clone();
        let mut addr_ctr = addr.clone();
        addr_ctr.push_str("-members");
        // As the committer, we are responsible for:
        // 1. making sure there is not an in-progress commit already on -committed (use setnx)
        // 2. deleting the -committed key once the transition is committed.
        let mut commit_ctr = addr.clone();
        commit_ctr.push_str("-committed");

        Box::pin(
            async move {
                let neg_nonce = bincode::serialize(&new_entry).wrap_err("serialize entry")?;
                let insert_time = std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                    .as_millis() as u64;
                let expiry_time = (std::time::SystemTime::now() - self.liveness_timeout)
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                    .as_millis() as u64;
                let mut r = redis::pipe();
                debug!("starting transition");
                let (lock_commit, curr_count, present_semantics, _, _, conn_count): (
                    isize,
                    usize,
                    Vec<u8>,
                    isize,
                    isize,
                    usize,
                ) = r
                    .atomic()
                    .set_nx(&commit_ctr, 1usize)
                    .get(&commit_ctr)
                    .get(&addr)
                    .zrembyscore(&addr_ctr, 0, expiry_time)
                    .zadd(&addr_ctr, &self.client_name, insert_time as usize)
                    .zcard(&addr_ctr)
                    .query_async(&mut self.redis_conn)
                    .await?;
                ensure!(
                    lock_commit == 1 || present_semantics == neg_nonce,
                    "Commit already in progress on {:?}",
                    &addr
                );

                debug!(?lock_commit, ?curr_count, "transition response");

                // if lock_commit == 0, a commit already started, but those semantics match ours.
                // we can just convert into a non-committer.
                // Otherwise, we're locked as the committer. Set the new semantics.
                if lock_commit == 1 {
                    ensure!(
                        curr_count == 1,
                        "Locked committer but count mismatched: {:?}",
                        curr_count
                    );
                    let mut r = redis::pipe();
                    r.atomic()
                        .set(&addr, &neg_nonce[..])
                        .query_async(&mut self.redis_conn)
                        .await?;
                }

                if conn_count > 1 {
                    self.wait_commit(addr.clone(), curr_count, conn_count)
                        .await?;
                }

                if lock_commit == 1 {
                    // we were the committer, so we need to clean up -committed
                    let mut r = redis::pipe();
                    r.atomic()
                        .del(&commit_ctr)
                        .query_async(&mut self.redis_conn)
                        .await?;
                    debug!("cleaned up transition commit lock");
                }

                Ok(())
            }
            .instrument(debug_span!("transition", addr = ?&a)),
        )
    }

    /// Increment the staged update counter on `addr`.
    ///
    /// Returns once the commit concludes: when the staged update counter the number of unexpired
    /// partiparticipants.
    fn staged_update<'a>(
        &'a mut self,
        addr: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        let mut addr_ctr = addr.clone();
        addr_ctr.push_str("-members");
        let mut commit_ctr = addr.clone();
        commit_ctr.push_str("-committed");
        Box::pin(async move {
            let mut r = redis::pipe();
            let insert_time = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                .as_millis() as u64;
            let expiry_time = (std::time::SystemTime::now() - self.liveness_timeout)
                .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                .as_millis() as u64;
            debug!("incrementing commit counter");
            let (commit_ctr, _, _, conn_count): (usize, isize, isize, usize) = r
                .atomic()
                .incr(&commit_ctr, 1)
                .zrembyscore(&addr_ctr, 0, expiry_time)
                .zadd(&addr_ctr, &self.client_name, insert_time as usize)
                .zcard(&addr_ctr)
                .query_async(&mut self.redis_conn)
                .await?;
            if commit_ctr == conn_count {
                debug!(?commit_ctr, ?conn_count, "was committed");
                Ok(())
            } else {
                debug!(?commit_ctr, ?conn_count, "waiting on commit");
                self.wait_commit(addr, commit_ctr, conn_count).await
            }
        })
    }
}

impl RedisBase {
    // wait for the commit counter to match conn_count
    async fn wait_commit(
        &mut self,
        addr: String,
        mut curr_commit_count: usize,
        tot_participant_count: usize,
    ) -> Result<(), Report> {
        let mut addr_ctr = addr.clone();
        addr_ctr.push_str("-members");
        // As the committer, we are responsible for:
        // 1. making sure there is not an in-progress commit already on -committed (use setnx)
        // 2. deleting the -committed key once the transition is committed.
        let mut commit_ctr = addr.clone();
        commit_ctr.push_str("-committed");

        let channel_name = format!("__keyspace@*__:{}", &commit_ctr);
        self.redis_pubsub.psubscribe(&channel_name).await?;
        loop {
            debug!("waiting on further updates");
            // wait for some event on the key.
            // redis subscribe will never return None.
            // TODO XXX also reset expiry at slightly less than expiry time.
            let msg = self.redis_pubsub.on_message().next().await.unwrap();
            let event = msg.get_payload::<String>()?;
            debug!(
                ?event,
                ?curr_commit_count,
                ?tot_participant_count,
                "staged-update value changed"
            );
            let mut done = false;
            if event.contains("del") {
                done = true;
            } else if event.contains("incrby") {
                curr_commit_count += 1;
                if curr_commit_count == tot_participant_count {
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

    async fn join(&mut self, addr_ctr: &str) -> Result<(), Report> {
        let insert_time = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
            .as_millis() as u64;
        let mut r = redis::pipe();
        r.atomic()
            .zadd(addr_ctr, &self.client_name, insert_time as usize)
            .query_async(&mut self.redis_conn)
            .await
            .wrap_err(eyre!("redis zadd on {:?}", addr_ctr))?;
        Ok(())
    }
}

fn rand_name() -> String {
    use rand::Rng;
    let rng = rand::thread_rng();
    rng.sample_iter(&rand::distributions::Alphanumeric)
        .take(10)
        .map(char::from)
        .collect()
}

#[cfg(test)]
mod t {
    use super::RedisBase;
    use bertha::{negotiate::RendezvousEntry, RendezvousBackend};
    use color_eyre::eyre::{eyre, Report};
    use tokio::sync::oneshot;
    use tracing::{error, info, info_span};
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[test]
    fn liveness_expiration() -> Result<(), Report> {
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

        rt.block_on(async move {
            let redis = start_redis(13521);
            let redis_addr = redis.get_addr();
            let bertha_addr = super::rand_name();
            let offer1 = dummy_unique_rendezvous_entry();
            let offer2 = dummy_unique_rendezvous_entry();

            let mut client1 = RedisBase::new(&redis_addr).await?;
            client1.set_liveness_expiration(std::time::Duration::from_millis(100));
            info!("client1 calling try_init");
            match client1.try_init(bertha_addr.clone(), offer1.clone()).await {
                Ok(bertha::NegotiateRendezvousResult::Matched { num_participants })
                    if num_participants == 1 => info!("client1 try_init ok"),
                Ok(bertha::NegotiateRendezvousResult::Matched { num_participants }) => return Err(eyre!("Wrong number of participants for client1 try_init: expected 1 got {:?}", num_participants)),
                Ok(bertha::NegotiateRendezvousResult::NoMatch { entry,  num_participants }) => return Err(eyre!("Found entry on client1 try_init: {:?}, participants = {:?}", entry, num_participants)),
                Err(e) => return Err(e),
            }

            let mut client2 = RedisBase::new(&redis_addr).await?;
            client2.set_liveness_expiration(std::time::Duration::from_millis(100));
            info!("client2 try_init 1");
            match client2.try_init(bertha_addr.clone(), offer2.clone()).await {
                Ok(bertha::NegotiateRendezvousResult::NoMatch { num_participants, entry }) if num_participants == 1 && entry == offer1 => {
                    info!("client2 try_init 1 ok");
                }
                Ok(bertha::NegotiateRendezvousResult::NoMatch {  num_participants, .. }) => {
                    return Err(eyre!("Wrong number of participants for client2 try_init 1: expected 1 got {:?}", num_participants))
                }
                Ok(bertha::NegotiateRendezvousResult::Matched { num_participants }) => return Err(eyre!("Wrong match for client2 try_init 1: num_participants={:?}", num_participants)),
                Err(e) => return Err(e),
            }

            // after this sleep, the first client will have timed out and our try_init will work.
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;

            info!("client2 try_init");
            match client2.try_init(bertha_addr, offer2).await {
                Ok(bertha::NegotiateRendezvousResult::Matched { num_participants })
                    if num_participants == 1 =>
                {
                    info!("client2 try_init ok");
                }
                Ok(bertha::NegotiateRendezvousResult::Matched { num_participants }) => return Err(eyre!("Wrong number of participants for client2 try_init: expected 1 got {:?}", num_participants)),
                Ok(bertha::NegotiateRendezvousResult::NoMatch { entry,  num_participants }) => return Err(eyre!("Found entry on client2 try_init: was_offer1={:?}, participants = {:?}", entry == offer1, num_participants)),
                Err(e) => return Err(e),
            }

            Ok(())
        }.instrument(info_span!("liveness_expiration")))
    }

    #[test]
    fn upgrade() -> Result<(), Report> {
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
                let redis = start_redis(13522);
                let redis_addr = redis.get_addr();
                let bertha_addr = super::rand_name();
                let ba = bertha_addr.clone();
                let offer1 = dummy_unique_rendezvous_entry();
                let offer2 = dummy_unique_rendezvous_entry();
                let o1 = offer1.clone();
                let o2 = offer2.clone();

                let (s,mut r) = oneshot::channel();
                let (ready_s,ready_r) = oneshot::channel();

                tokio::spawn(async move {
                    fn fail(s: oneshot::Sender<Report>, err: Report) -> ! {
                        error!(%err, "client1 failed");
                        s.send(err).unwrap();
                        panic!("failed");
                    }

                    let mut client1 = match RedisBase::new(&redis_addr).await {
                        Ok(c) => c,
                        Err(e) => {fail(s,e); }
                    };

                    client1.set_liveness_expiration(std::time::Duration::from_millis(100));
                    info!("calling try_init");
                    match client1.try_init(bertha_addr.clone(), offer1.clone()).await {
                        Ok(bertha::NegotiateRendezvousResult::Matched { num_participants })
                            if num_participants == 1 => info!("client1 try_init ok"),
                        Ok(bertha::NegotiateRendezvousResult::Matched { num_participants }) => fail(s,eyre!("Wrong number of participants for client1 try_init: expected 1 got {:?}", num_participants)),
                        Ok(bertha::NegotiateRendezvousResult::NoMatch { entry,  num_participants }) => fail(s,eyre!("Found entry on client1 try_init: {:?}, participants = {:?}", entry, num_participants)),
                        Err(e) => fail(s,e),
                    }

                    ready_s.send(()).unwrap();

                    info!("waiting on notify");
                    match client1.notify(bertha_addr.clone(), offer1.clone()).await {
                        Ok(bertha::NegotiateRendezvousResult::NoMatch { entry,  num_participants }) if entry == o2 => {
                            info!(?num_participants, "got new entry");
                        }
                        Ok(bertha::NegotiateRendezvousResult::NoMatch { entry,  .. })  => {
                            fail(s,eyre!("Entry mismatched: was_offer1={:?}", entry == offer1));
                        }
                        Ok(bertha::NegotiateRendezvousResult::Matched { .. }) => {
                            fail(s,eyre!("Notify shouldn't match"));
                        }
                        Err(e) => fail(s,e),
                    }

                    info!("calling staged_update");
                    if let Err(e) = client1.staged_update(bertha_addr.clone()).await {
                        fail(s,e)
                    }

                    info!("done");
                }.instrument(info_span!("client1")));

                ready_r.await.unwrap();
                if let Ok(err) = r.try_recv() {
                    return Err(err.wrap_err("client 1 failed"));
                }

                let redis_addr = redis.get_addr();
                let bertha_addr = ba;
                let mut client2 = RedisBase::new(&redis_addr).await?;
                client2.set_liveness_expiration(std::time::Duration::from_millis(100));
                info!("client2 try_init");
                match client2.try_init(bertha_addr.clone(), offer2.clone()).await {
                    Ok(bertha::NegotiateRendezvousResult::NoMatch { num_participants, entry }) if num_participants == 1 && entry == o1 => {
                        info!("client2 try_init ok");
                    }
                    Ok(bertha::NegotiateRendezvousResult::NoMatch {  num_participants, entry }) => {
                        return Err(eyre!("Wrong result for client2 try_init: num_participants={:?}, was_offer1={:?}", num_participants, entry == o1))
                    }
                    Ok(bertha::NegotiateRendezvousResult::Matched { num_participants }) => return Err(eyre!("Wrong match for client2 try_init: num_participants={:?}", num_participants)),
                    Err(e) => return Err(e),
                }

                if let Ok(err) = r.try_recv() {
                    return Err(err.wrap_err("client 1 failed"));
                }

                info!("do transition");
                client2.transition(bertha_addr.clone(), offer2.clone()).await?;

                if let Ok(err) = r.try_recv() {
                    return Err(err.wrap_err("client 1 failed"));
                }

                info!("check with poll_entry");
                match client2.poll_entry(bertha_addr.clone(), offer2.clone()).await? {
                    bertha::NegotiateRendezvousResult::Matched { .. } => (),
                    bertha::NegotiateRendezvousResult::NoMatch { entry, .. } => {
                        return Err(eyre!("poll_entry did not match: was_offer1={:?}", entry==o1));
                    }
                }

                if let Ok(err) = r.try_recv() { 
                    return Err(err.wrap_err("client 1 failed"));
                }

                Ok(())
            }
            .instrument(info_span!("upgrade")),
        )
    }

    fn dummy_unique_rendezvous_entry() -> RendezvousEntry {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        RendezvousEntry {
            nonce: [(
                rng.gen::<u64>(),
                bertha::negotiate::Offer {
                    capability_guid: rng.gen(),
                    impl_guid: rng.gen(),
                    sidedness: None,
                    available: vec![],
                },
            )]
            .iter()
            .cloned()
            .collect(),
        }
    }

    fn start_redis(port: u16) -> test_util::Redis {
        info!(?port, "start redis");
        test_util::start_redis(port)
    }
}
