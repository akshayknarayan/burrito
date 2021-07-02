// Pin<Box<...>> is necessary and not worth breaking up
#![allow(clippy::type_complexity)]

use bertha::negotiate::{NegotiateRendezvousResult, RendezvousBackend, RendezvousEntry};
use color_eyre::eyre::{bail, ensure, eyre, Report, WrapErr};
use futures_util::stream::StreamExt;
use std::{future::Future, pin::Pin};
use tracing::{debug, debug_span, trace};
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

lazy_static::lazy_static! {
    static ref ROUND_CTR_LOCK_SCRIPT: redis::Script = redis::Script::new(include_str!("./transition.lua"));
    static ref TRY_INIT_SCRIPT: redis::Script = redis::Script::new(include_str!("./tryinit.lua"));
}

// TODO XXX replace expiry_time with redis server TIME.
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
        let mut round_ctr = addr.clone();
        round_ctr.push_str("-roundctr");
        Box::pin(
            async move {
                let neg_nonce = bincode::serialize(&offer).wrap_err("serialize entry")?;
                let expiry_time = (std::time::SystemTime::now() - self.liveness_timeout)
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                    .as_millis() as u64;
                let insert_time = std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                    .as_millis() as u64;

                let (joined, round_ctr_value, conn_count, got_semantics): (
                    bool,
                    usize,
                    usize,
                    Vec<u8>,
                ) = TRY_INIT_SCRIPT
                    .key(&addr)
                    .key(&round_ctr)
                    .key(&addr_ctr)
                    .arg(&neg_nonce[..])
                    .arg(&self.client_name)
                    .arg(expiry_time)
                    .arg(insert_time)
                    .invoke_async(&mut self.redis_conn)
                    .await
                    .wrap_err(eyre!("try_init failed on {:?}", &addr))?;
                if joined {
                    Ok(NegotiateRendezvousResult::Matched {
                        num_participants: 1,
                        round_number: round_ctr_value,
                    })
                } else {
                    debug!(res = "existed", ?conn_count, "got try_init response");
                    // was not set, which means someone was already here.
                    if conn_count == 0 {
                        // setnx failed, but we're the only ones home. override.
                        self.transition(addr.clone(), offer).await?;
                        Ok(NegotiateRendezvousResult::Matched {
                            num_participants: 1,
                            round_number: round_ctr_value,
                        })
                    } else {
                        let present_semantics: RendezvousEntry =
                            bincode::deserialize(&got_semantics).wrap_err("deserialize entry")?;
                        Ok(NegotiateRendezvousResult::NoMatch {
                            entry: present_semantics,
                            num_participants: conn_count,
                            round_number: round_ctr_value,
                        })
                    }
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
        curr_round: usize,
    ) -> Pin<Box<dyn Future<Output = Result<NegotiateRendezvousResult, Self::Error>> + Send + 'a>>
    {
        let a = addr.clone();
        let mut addr_ctr = addr.clone();
        addr_ctr.push_str("-members");
        let mut round_ctr_key = addr.clone();
        round_ctr_key.push_str("-roundctr");
        Box::pin(
            async move {
                let insert_time = std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                    .as_millis() as u64;
                let expiry_time = (std::time::SystemTime::now() - self.liveness_timeout)
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                    .as_millis() as u64;
                let mut r = redis::pipe();
                let (got_value, round_ctr, _, _, conn_count): (
                    Vec<u8>,
                    usize,
                    isize,
                    isize,
                    usize,
                ) = r
                    .atomic()
                    .get(&addr)
                    .get(&round_ctr_key)
                    .zrembyscore(&addr_ctr, 0usize, expiry_time)
                    .zadd(&addr_ctr, &self.client_name, insert_time as usize)
                    .zcard(&addr_ctr)
                    .query_async(&mut self.redis_conn)
                    .await?;

                // first, check the round.
                // if the round didn't change, then no transition has happened, so the semantics
                // won't have changed.
                // if it did change, then we need to look for the staged semantics.
                if round_ctr == curr_round {
                    debug!(?round_ctr, ?conn_count, "Same round");
                    let present_semantics: RendezvousEntry =
                        bincode::deserialize(&got_value).wrap_err("deserialize entry")?;
                    ensure!(
                        present_semantics == curr_entry,
                        "KV store polluted: round didn't advance but semantics changed"
                    );
                    Ok(NegotiateRendezvousResult::Matched {
                        num_participants: conn_count,
                        round_number: round_ctr,
                    })
                } else if round_ctr > curr_round {
                    let staged_key = format!("{}-{}-staged", &addr, round_ctr);
                    debug!(?round_ctr, ?conn_count, ?staged_key, "New round");
                    let mut r = redis::pipe();
                    let insert_time = std::time::SystemTime::now()
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                        .as_millis() as u64;
                    let expiry_time = (std::time::SystemTime::now() - self.liveness_timeout)
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                        .as_millis() as u64;
                    let (staged_val, _, _, conn_count): (Option<Vec<u8>>, isize, isize, usize) = r
                        .atomic()
                        .get(&staged_key)
                        .zrembyscore(&addr_ctr, 0usize, expiry_time)
                        .zadd(&addr_ctr, &self.client_name, insert_time as usize)
                        .zcard(&addr_ctr)
                        .query_async(&mut self.redis_conn)
                        .await?;
                    let staged_val = staged_val.ok_or(eyre!(
                        "KV store polluted: new round {:?} but staged_val key {:?} empty",
                        round_ctr,
                        &staged_key
                    ))?;
                    ensure!(
                        !staged_val.is_empty(),
                        "KV store polluted: new round {:?} but staged_val key {:?} empty",
                        round_ctr,
                        &staged_key
                    );
                    let present_semantics: RendezvousEntry =
                        bincode::deserialize(&staged_val).wrap_err("deserialize entry")?;
                    Ok(NegotiateRendezvousResult::NoMatch {
                        entry: present_semantics,
                        num_participants: conn_count,
                        round_number: round_ctr,
                    })
                } else {
                    bail!(
                        "KV store polluted: round_ctr ticked backwards: local_curr={:?}, got={:?}",
                        curr_round,
                        round_ctr
                    );
                }
            }
            .instrument(debug_span!("redis_poll", addr = ?&a)),
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
        curr_round: usize,
    ) -> Pin<Box<dyn Future<Output = Result<NegotiateRendezvousResult, Self::Error>> + Send + 'a>>
    where
        Self: Send,
    {
        let a = addr.clone();
        let mut addr_members = addr.clone();
        addr_members.push_str("-members");
        let mut round_ctr_key = addr.clone();
        round_ctr_key.push_str("-roundctr");
        Box::pin(
            async move {
                // there are 2 keys to listen on:
                // 1. -members covers cases (1) and (2) above (join or leave)
                // 2. -roundctr covers case (3) (transition)
                let channel_ctr_name = format!("__keyspace@*__:{}", &addr_members);
                self.redis_pubsub.psubscribe(&channel_ctr_name).await?;
                let channel_name = format!("__keyspace@*__:{}", &round_ctr_key);
                self.redis_pubsub.psubscribe(&channel_name).await?;
                // wait for some event on the key.
                loop {
                    let refresh_to = tokio::time::sleep(self.liveness_timeout / 2);
                    tokio::pin!(refresh_to);
                    match futures_util::future::select(
                        self.redis_pubsub.on_message().next(),
                        refresh_to,
                    )
                    .await
                    {
                        futures_util::future::Either::Left((Some(m), _)) => {
                            trace!(?channel_ctr_name, ?channel_name, ?m, "subscribe got msg");
                            break;
                        }
                        futures_util::future::Either::Left((None, _)) => unreachable!(),
                        futures_util::future::Either::Right((_, _pubsub_fut)) => (),
                    };

                    self.refresh_client(addr.clone()).await?;
                }

                debug!("semantics changed");
                self.redis_pubsub.punsubscribe(&channel_name).await?;
                self.redis_pubsub.punsubscribe(&channel_ctr_name).await?;
                // return result of polling now
                // there is a race between the round_ctr incremented -> our notification -> read and the staged-key being written.
                // try to avoid this with a sleep and a retry loop.
                let mut poll_retries = 0;
                loop {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    match self
                        .poll_entry(addr.clone(), curr_entry.clone(), curr_round)
                        .await
                    {
                        x @ Ok(_) => return x,
                        Err(e) => {
                            debug!(err = ?&e, ?poll_retries, "Poll failed");
                            if poll_retries > 10 {
                                return Err(e).wrap_err("Poll after notify failed 10 times");
                            }

                            poll_retries += 1;
                            self.refresh_client(addr.clone()).await?;
                        }
                    }
                }
            }
            .instrument(debug_span!("redis_notify", addr = ?&a)),
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
    ) -> Pin<Box<dyn Future<Output = Result<usize, Self::Error>> + Send + 'a>> {
        let a = addr.clone();
        let mut addr_members = addr.clone();
        addr_members.push_str("-members");
        let mut round_ctr_key = addr.clone();
        round_ctr_key.push_str("-roundctr");

        // 1. get the round counter.
        // if even: there's no transition happening now, we can go ahead
        // if odd: there is an ongoing transition. convert to an applier. in this case, this
        // transition should re-try later.
        //
        // if committer:
        // 2. write semantics to staged key
        // 3. wait_commit() for everyone else to ack.
        // 4. it's now committed. copy staged key to addr and increment the round counnter again
        //    (so it is even)
        //
        // if applier:
        // 2. read staged key. if == our proposal, just call wait_commit and exit once it returns.
        //    otherwise, error out.

        Box::pin(
            async move {
                let neg_nonce = bincode::serialize(&new_entry).wrap_err("serialize entry")?;
                debug!("starting transition");
                let (locked, round_ctr): (bool, usize) = ROUND_CTR_LOCK_SCRIPT.key(&round_ctr_key).invoke_async(&mut self.redis_conn).await?;
                let staged_val = format!("{}-{}-staged", &addr, round_ctr);
                let staged_commit_counter = format!("{}-{}-committed-cnt", &addr, round_ctr);

                let insert_time = std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                    .as_millis() as u64;
                let expiry_time = (std::time::SystemTime::now() - self.liveness_timeout)
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                    .as_millis() as u64;
                if locked {
                    //  we are the committer
                    debug!(?round_ctr, "Locked commit");
                    let mut r = redis::pipe();
                    let (set_staged, set_commit_ctr, _, _, conn_count): (isize, isize, isize, isize, usize) = r
                        .atomic()
                        .set_nx(&staged_val, neg_nonce.clone())
                        .set_nx(&staged_commit_counter, 1)
                        .zrembyscore(&addr_members, 0, expiry_time)
                        .zadd(&addr_members, &self.client_name, insert_time as usize)
                        .zcard(&addr_members)
                        .query_async(&mut self.redis_conn)
                        .await.wrap_err("Set staged value")?;
                    ensure!(set_staged == 1 && set_commit_ctr == 1, "Polluted KV store: Locked commit but staged values already present");
                    debug!(?set_staged, ?staged_val, "Wrote staged value");
                    if conn_count > 1 {
                        self.wait_committer(addr.clone(), round_ctr, 1, conn_count).await?;
                    } else {
                        debug!(?conn_count, "Trivial commit");
                    }

                    // done. write to real key, delete staging keys, and re-incr round counter to even
                    let mut r = redis::pipe();
                    debug!("Cleaning up commit keys");
                    let (_, _, _, round_ctr): ((), isize, isize, usize) = match r.atomic().set(&addr, neg_nonce).del(&staged_val).del(&staged_commit_counter).incr(&round_ctr_key, 1usize).query_async(&mut self.redis_conn).await.wrap_err("Cleanup on commit keys") {
                        Ok(x) => x,
                        Err(e) => {
                            tracing::error!(err = ?&e, "cleanup commit failed");
                            return Err(e);
                        }
                    };
                    ensure!(round_ctr % 2 == 0, "Polluted KV store: Commit done but round_ctr odd: {:?}", round_ctr);
                    debug!("Wrote committed semantics and cleaned up commit keys");
                    Ok(round_ctr)
                } else {
                    debug!(?round_ctr, "Existing in-progress transition, try converting to applier");
                    // we can be an applier if -staged matches what we are trying to commit.
                    // otherwise, we will error out. 
                    let mut r = redis::pipe();
                    let (present_semantics, curr_count, _, _, conn_count): (Vec<u8>, usize, isize, isize, usize) = r
                        .atomic()
                        .get(&staged_val)
                        .get(&staged_commit_counter)
                    .zrembyscore(&addr_members, 0, expiry_time)
                    .zadd(&addr_members, &self.client_name, insert_time as usize)
                    .zcard(&addr_members)
                        .query_async(&mut self.redis_conn)
                        .await?;
                    ensure!(present_semantics == neg_nonce, "Incompatible commit already in progress on {:?}, round {:?}", &addr, round_ctr);
                    ensure!(conn_count > 1, "KV store polluted on {:?}: {:?} participants but found existing commit for round {:?}", &addr, conn_count, round_ctr);
                    // we are an applier now.
                    debug!(?round_ctr, ?curr_count, ?conn_count, "Converted to applier");
                    self.wait_applier(addr.clone(), round_ctr).await
                }
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
        round_ctr: usize,
    ) -> Pin<Box<dyn Future<Output = Result<usize, Self::Error>> + Send + 'a>> {
        let mut addr_members = addr.clone();
        addr_members.push_str("-members");
        let staged_commit_counter = format!("{}-{}-committed-cnt", &addr, round_ctr);
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
                .incr(&staged_commit_counter, 1usize)
                .zrembyscore(&addr_members, 0usize, expiry_time)
                .zadd(&addr_members, &self.client_name, insert_time as usize)
                .zcard(&addr_members)
                .query_async(&mut self.redis_conn)
                .await?;
            debug!(?commit_ctr, ?conn_count, "waiting on commit");
            self.wait_applier(addr, round_ctr).await
        })
    }
}

impl RedisBase {
    // wait for the commit counter to match conn_count
    async fn wait_applier(&mut self, addr: String, round_ctr: usize) -> Result<usize, Report> {
        let mut addr_members = addr.clone();
        addr_members.push_str("-members");
        let mut round_ctr_key = addr.clone();
        round_ctr_key.push_str("-roundctr");
        let staged_commit_counter = format!("{}-{}-committed-cnt", &addr, round_ctr);
        let channel_name = format!("__keyspace@*__:{}", &staged_commit_counter);
        self.redis_pubsub.psubscribe(&channel_name).await?;
        loop {
            debug!("waiting on further updates");
            // wait for some event on the key.
            // redis subscribe will never return None.
            let refresh_to = tokio::time::sleep(self.liveness_timeout / 2);
            tokio::pin!(refresh_to);
            let mut do_refresh = false;
            let done = match futures_util::future::select(
                self.redis_pubsub.on_message().next(),
                refresh_to,
            )
            .await
            {
                futures_util::future::Either::Left((Some(msg), _)) => {
                    let event = msg.get_payload::<String>()?;
                    debug!(?event, ?round_ctr, "staged-update value changed");

                    event.contains("del")
                }
                futures_util::future::Either::Left((None, _)) => unreachable!(),
                futures_util::future::Either::Right((_, _pubsub_fut)) => {
                    // it's fine to drop pubsub_fut and make a new one the next iteration?
                    // reset expiry since we are still alive.
                    do_refresh = true;
                    false
                }
            };

            if do_refresh {
                self.refresh_commit(addr.clone(), round_ctr).await?;
            }

            if done {
                self.redis_pubsub.punsubscribe(&channel_name).await?;
                debug!("observed commit, fetching new round_ctr");
                let mut r = redis::pipe();
                let insert_time = std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                    .as_millis() as u64;
                let expiry_time = (std::time::SystemTime::now() - self.liveness_timeout)
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                    .as_millis() as u64;
                let (round_ctr, _, _): (usize, isize, isize) = r
                    .atomic()
                    .get(&round_ctr_key)
                    .zrembyscore(&addr_members, 0usize, expiry_time)
                    .zadd(&addr_members, &self.client_name, insert_time as usize)
                    .query_async(&mut self.redis_conn)
                    .await?;
                debug!(?round_ctr, "done applying");
                return Ok(round_ctr);
            }
        }
    }

    async fn wait_committer(
        &mut self,
        addr: String,
        round_ctr: usize,
        mut curr_commit_count: usize,
        mut tot_participant_count: usize,
    ) -> Result<(), Report> {
        let mut addr_members = addr.clone();
        addr_members.push_str("-members");
        let staged_commit_counter = format!("{}-{}-committed-cnt", &addr, round_ctr);
        let channel_name = format!("__keyspace@*__:{}", &staged_commit_counter);
        self.redis_pubsub.psubscribe(&channel_name).await?;
        loop {
            debug!("waiting on further updates");
            // wait for some event on the key.
            // redis subscribe will never return None.
            let refresh_to = tokio::time::sleep(self.liveness_timeout / 2);
            tokio::pin!(refresh_to);
            let mut do_refresh = false;
            let done = match futures_util::future::select(
                self.redis_pubsub.on_message().next(),
                refresh_to,
            )
            .await
            {
                futures_util::future::Either::Left((Some(msg), _)) => {
                    let event = msg.get_payload::<String>()?;
                    debug!(
                        ?event,
                        ?round_ctr,
                        ?curr_commit_count,
                        ?tot_participant_count,
                        "staged-update value changed"
                    );

                    if event.contains("incrby") {
                        curr_commit_count += 1;
                        curr_commit_count == tot_participant_count
                    } else {
                        false
                    }
                }
                futures_util::future::Either::Left((None, _)) => unreachable!(),
                futures_util::future::Either::Right((_, _pubsub_fut)) => {
                    // it's fine to drop pubsub_fut and make a new one the next iteration?
                    // reset expiry since we are still alive.
                    do_refresh = true;
                    false
                }
            };

            if do_refresh {
                let (staged, conn_count) = self.refresh_commit(addr.clone(), round_ctr).await?;
                curr_commit_count = staged;
                tot_participant_count = conn_count;
            }

            if done {
                self.redis_pubsub.punsubscribe(&channel_name).await?;
                debug!("committer has enough acks");
                return Ok(());
            }
        }
    }

    async fn refresh_commit(
        &mut self,
        addr: String,
        round_ctr: usize,
    ) -> Result<(usize, usize), Report> {
        let mut addr_members = addr.clone();
        addr_members.push_str("-members");
        let staged_commit_counter = format!("{}-{}-committed-cnt", &addr, round_ctr);
        let mut r = redis::pipe();
        let insert_time = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
            .as_millis() as u64;
        let expiry_time = (std::time::SystemTime::now() - self.liveness_timeout)
            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
            .as_millis() as u64;
        let (staged, _, _, conn_count): (usize, isize, isize, usize) = r
            .atomic()
            .get(&staged_commit_counter)
            .zrembyscore(&addr_members, 0usize, expiry_time)
            .zadd(&addr_members, &self.client_name, insert_time as usize)
            .zcard(&addr_members)
            .query_async(&mut self.redis_conn)
            .await?;
        debug!(?staged, ?conn_count, "refreshed client expiry");
        Ok((staged, conn_count))
    }

    async fn refresh_client(&mut self, addr: String) -> Result<(), Report> {
        let mut addr_members = addr.clone();
        addr_members.push_str("-members");

        debug!("refresh connection membership");
        let mut r = redis::pipe();
        let insert_time = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
            .as_millis() as u64;
        let expiry_time = (std::time::SystemTime::now() - self.liveness_timeout)
            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
            .as_millis() as u64;
        let (_, _, _): (isize, isize, usize) = r
            .atomic()
            .zrembyscore(&addr_members, 0usize, expiry_time)
            .zadd(&addr_members, &self.client_name, insert_time as usize)
            .zcard(&addr_members)
            .query_async(&mut self.redis_conn)
            .await?;
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
    use color_eyre::eyre::{eyre, Report, WrapErr};
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
                Ok(bertha::NegotiateRendezvousResult::Matched { num_participants, round_number })
                    if num_participants == 1 && round_number == 0 => info!("client1 try_init ok"),
                Ok(bertha::NegotiateRendezvousResult::Matched { num_participants, round_number }) => return Err(eyre!("Wrong number of participants for client1 try_init: round_number = {:?}, expected 1 got {:?}", round_number, num_participants)),
                Ok(bertha::NegotiateRendezvousResult::NoMatch { entry,  num_participants, round_number }) => return Err(eyre!("Found entry on client1 try_init: {:?}, participants = {:?} round_number = {:?}", entry, num_participants, round_number)),
                Err(e) => return Err(e),
            }

            let mut client2 = RedisBase::new(&redis_addr).await?;
            client2.set_liveness_expiration(std::time::Duration::from_millis(100));
            info!("client2 try_init 1");
            match client2.try_init(bertha_addr.clone(), offer2.clone()).await {
                Ok(bertha::NegotiateRendezvousResult::NoMatch { num_participants, entry, .. }) if num_participants == 1 && entry == offer1 => {
                    info!("client2 try_init 1 ok");
                }
                Ok(bertha::NegotiateRendezvousResult::NoMatch {  num_participants, .. }) => {
                    return Err(eyre!("Wrong number of participants for client2 try_init 1: expected 1 got {:?}", num_participants))
                }
                Ok(bertha::NegotiateRendezvousResult::Matched { num_participants, .. }) => return Err(eyre!("Wrong match for client2 try_init 1: num_participants={:?}", num_participants)),
                Err(e) => return Err(e),
            }

            // after this sleep, the first client will have timed out and our try_init will work.
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;

            info!("client2 try_init");
            match client2.try_init(bertha_addr, offer2).await {
                Ok(bertha::NegotiateRendezvousResult::Matched { num_participants, .. })
                    if num_participants == 1 =>
                {
                    info!("client2 try_init ok");
                }
                Ok(bertha::NegotiateRendezvousResult::Matched { num_participants, .. }) => return Err(eyre!("Wrong number of participants for client2 try_init: expected 1 got {:?}", num_participants)),
                Ok(bertha::NegotiateRendezvousResult::NoMatch { entry,  num_participants, .. }) => return Err(eyre!("Found entry on client2 try_init: was_offer1={:?}, participants = {:?}", entry == offer1, num_participants)),
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

                    client1.set_liveness_expiration(std::time::Duration::from_millis(500));
                    info!("calling try_init");
                    match client1.try_init(bertha_addr.clone(), offer1.clone()).await {
                        Ok(bertha::NegotiateRendezvousResult::Matched { num_participants, round_number })
                            if num_participants == 1 && round_number == 0 => info!("client1 try_init ok"),
                        Ok(bertha::NegotiateRendezvousResult::Matched { num_participants, .. }) => fail(s,eyre!("Wrong number of participants for client1 try_init: expected 1 got {:?}", num_participants)),
                        Ok(bertha::NegotiateRendezvousResult::NoMatch { entry,  num_participants, .. }) => fail(s,eyre!("Found entry on client1 try_init: {:?}, participants = {:?}", entry, num_participants)),
                        Err(e) => fail(s,e),
                    }

                    ready_s.send(()).unwrap();

                    info!("waiting on notify");
                    let round_number = match client1.notify(bertha_addr.clone(), offer1.clone(), 0).await {
                        Ok(bertha::NegotiateRendezvousResult::NoMatch { entry,  num_participants, round_number }) if entry == o2 => {
                            info!(?num_participants, "got new entry");
                            round_number
                        }
                        Ok(bertha::NegotiateRendezvousResult::NoMatch { entry,  .. })  => {
                            fail(s,eyre!("Entry mismatched: was_offer1={:?}", entry == offer1));
                        }
                        Ok(bertha::NegotiateRendezvousResult::Matched { .. }) => {
                            fail(s,eyre!("Notify shouldn't match"));
                        }
                        Err(e) => fail(s,e),
                    };

                    info!("calling staged_update");
                    if let Err(e) = client1.staged_update(bertha_addr.clone(), round_number).await {
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
                client2.set_liveness_expiration(std::time::Duration::from_millis(500));
                info!("client2 try_init");
                match client2.try_init(bertha_addr.clone(), offer2.clone()).await {
                    Ok(bertha::NegotiateRendezvousResult::NoMatch { num_participants, entry, .. }) if num_participants == 1 && entry == o1 => {
                        info!("client2 try_init ok");
                    }
                    Ok(bertha::NegotiateRendezvousResult::NoMatch {  num_participants, entry, .. }) => {
                        return Err(eyre!("Wrong result for client2 try_init: num_participants={:?}, was_offer1={:?}", num_participants, entry == o1))
                    }
                    Ok(bertha::NegotiateRendezvousResult::Matched { num_participants, .. }) => return Err(eyre!("Wrong match for client2 try_init: num_participants={:?}", num_participants)),
                    Err(e) => return Err(e),
                }

                if let Ok(err) = r.try_recv() {
                    return Err(err.wrap_err("client 1 failed"));
                }

                info!("do transition");
                let round_number = tokio::time::timeout(std::time::Duration::from_secs(15), client2.transition(bertha_addr.clone(), offer2.clone())).await.wrap_err("test timeout")?.wrap_err("do transition")?;
                info!("did transition");

                if let Ok(err) = r.try_recv() {
                    return Err(err.wrap_err("client 1 failed"));
                }

                info!("check with poll_entry");
                match client2.poll_entry(bertha_addr.clone(), offer2.clone(), round_number).await? {
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
