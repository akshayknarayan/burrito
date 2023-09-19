// Pin<Box<...>> is necessary and not worth breaking up
#![allow(clippy::type_complexity)]

use bertha::negotiate::{NegotiateRendezvousResult, RendezvousBackend, RendezvousEntry};
use color_eyre::eyre::{bail, ensure, eyre, Report, WrapErr};
use futures_util::stream::StreamExt;
use std::{future::Future, pin::Pin, time::Duration};
use tokio::time::Instant;
use tracing::{debug, debug_span, trace};
use tracing_futures::Instrument;

pub struct RedisBase {
    redis_conn: redis::aio::MultiplexedConnection,
    redis_pubsub: redis::aio::PubSub,
    liveness_timeout: std::time::Duration,
    // TODO: make this configurable, it could even be queryable and represent the client address if
    // needed
    client_name: String,
}

impl RedisBase {
    pub async fn new(redis_addr: &str) -> Result<Self, Report> {
        let redis_client = redis::Client::open(redis_addr)
            .wrap_err_with(|| eyre!("Opening redis connection: {:?}", redis_addr))?;
        let mut redis_conn = redis_client
            .get_multiplexed_tokio_connection()
            .await
            .wrap_err("Connecting to redis")?;
        redis::cmd("CONFIG")
            .arg("SET")
            .arg("notify-keyspace-events")
            .arg("KEA")
            .query_async(&mut redis_conn)
            .await
            .wrap_err("Enable keyspace events on redis server")?;
        let redis_pubsub = redis_client
            .get_tokio_connection()
            .await
            .wrap_err("Connecting to redis")?
            .into_pubsub();
        Ok(RedisBase {
            redis_conn,
            redis_pubsub,
            liveness_timeout: std::time::Duration::from_secs(10),
            client_name: rand_name(),
        })
    }
}

lazy_static::lazy_static! {
    static ref ROUND_CTR_LOCK_SCRIPT: redis::Script = redis::Script::new(include_str!("./transition.lua"));
    static ref TRY_INIT_SCRIPT: redis::Script = redis::Script::new(include_str!("./tryinit.lua"));
    static ref POLL_ENTRY_SCRIPT: redis::Script = redis::Script::new(include_str!("./pollentry.lua"));
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
        let mut addr_join_lock = addr.clone();
        addr_join_lock.push_str("-joinlock");
        let joinlock_wait = format!("__keyspace@*__:{} del", &addr_join_lock);
        Box::pin(
            async move {
                let neg_nonce = bincode::serialize(&offer).wrap_err("serialize entry")?;
                loop {
                    let expiry_time = (std::time::SystemTime::now() - self.liveness_timeout)
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                        .as_millis() as u64;
                    let insert_time = std::time::SystemTime::now()
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                        .as_millis() as u64;
                    let (created, round_ctr_value, conn_count, got_semantics): (
                        usize,
                        usize,
                        usize,
                        Vec<u8>,
                    ) = TRY_INIT_SCRIPT
                        .key(&addr)
                        .key(&round_ctr)
                        .key(&addr_ctr)
                        .key(&addr_join_lock)
                        .arg(&neg_nonce[..])
                        .arg(&self.client_name)
                        .arg(expiry_time)
                        .arg(insert_time)
                        .invoke_async(&mut self.redis_conn)
                        .await
                        .wrap_err_with(|| eyre!("try_init failed on {:?}", &addr))?;
                    debug!(
                        ?created,
                        ?conn_count,
                        ?round_ctr_value,
                        "got try_init response"
                    );
                    match created {
                        2 => {
                            return Ok(NegotiateRendezvousResult::Matched {
                                num_participants: conn_count,
                                round_number: round_ctr_value,
                            });
                        }
                        1 if conn_count == 0 => {
                            // setnx failed, but we're the only ones home. override.
                            self.transition(addr.clone(), offer).await?;
                            return Ok(NegotiateRendezvousResult::Matched {
                                num_participants: 1,
                                round_number: round_ctr_value,
                            });
                        }
                        1 => {
                            let present_semantics: RendezvousEntry =
                                bincode::deserialize(&got_semantics)
                                    .wrap_err("deserialize entry")?;
                            return Ok(NegotiateRendezvousResult::NoMatch {
                                entry: present_semantics,
                                num_participants: conn_count,
                                round_number: round_ctr_value,
                            });
                        }
                        0 => {
                            debug!(?addr, "joinlock is locked, waiting");
                            self.redis_pubsub.psubscribe(&joinlock_wait).await?;
                            self.redis_pubsub.on_message().next().await.unwrap();
                            self.redis_pubsub.punsubscribe(&joinlock_wait).await?;
                            continue;
                        }
                        i => return Err(eyre!("Invalid return value from try_init: {}", i)),
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
        let mut addr_members_key = addr.clone();
        addr_members_key.push_str("-members");
        let mut round_ctr_key = addr.clone();
        round_ctr_key.push_str("-roundctr");
        let mut addr_join_lock_key = addr.clone();
        addr_join_lock_key.push_str("-joinlock");
        Box::pin(
            async move {
                let insert_time = std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                    .as_millis() as u64;
                let expiry_time = (std::time::SystemTime::now() - self.liveness_timeout)
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                    .as_millis() as u64;
                let (got_value, round_ctr, conn_count): (
                    Option<Vec<u8>>,
                    Option<usize>,
                    Option<usize>,
                ) = POLL_ENTRY_SCRIPT
                    .key(&addr)
                    .key(&round_ctr_key)
                    .key(&addr_members_key)
                    .key(&addr_join_lock_key)
                    .arg(&self.client_name)
                    .arg(expiry_time)
                    .arg(insert_time)
                    .invoke_async(&mut self.redis_conn)
                    .await
                    .wrap_err_with(|| eyre!("poll_entry failed on {:?}", &addr))?;
                let got_value = got_value.ok_or_else(|| eyre!("nonce value was nil"))?;
                let round_ctr = round_ctr.ok_or_else(|| eyre!("round-ctr was nil"))?;
                let conn_count = conn_count.ok_or_else(|| eyre!("conn-count was nil"))?;
                // first, check the round.
                // if the round didn't change, then no transition has happened, so the semantics
                // won't have changed.
                // if it did change, then we need to look for the staged semantics.
                match round_ctr.cmp(&curr_round) {
                    std::cmp::Ordering::Equal => {
                        trace!(?round_ctr, ?conn_count, "Same round");
                        let present_semantics: RendezvousEntry =
                            bincode::deserialize(&got_value).wrap_err("deserialize entry")?;
                        if present_semantics == curr_entry {
                            Ok(NegotiateRendezvousResult::Matched {
                                num_participants: conn_count,
                                round_number: round_ctr,
                            })
                        } else {
                            // if we tried to transition and converted to applier, this is how we
                            // find the new entry.
                            Ok(NegotiateRendezvousResult::NoMatch {
                                entry: present_semantics,
                                num_participants: conn_count,
                                round_number: round_ctr,
                            })
                        }
                    }
                    std::cmp::Ordering::Greater => {
                        let staged_key = format!("{}-{}-staged", &addr, round_ctr);
                        debug!(?round_ctr, ?conn_count, ?staged_key, "New round");
                        let (staged_val, conn_count) = loop {
                            let mut r = redis::pipe();
                            let insert_time = std::time::SystemTime::now()
                                .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                                .as_millis() as u64;
                            let expiry_time = (std::time::SystemTime::now() - self.liveness_timeout)
                                .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                                .as_millis() as u64;
                            let (staged_val, _, _, conn_count): (
                                Option<Vec<u8>>,
                                isize,
                                isize,
                                usize,
                            ) = r
                                .atomic()
                                .get(&staged_key)
                                .zrembyscore(&addr_members_key, 0usize, expiry_time)
                                .zadd(&addr_members_key, &self.client_name, insert_time as usize)
                                .zcard(&addr_members_key)
                                .query_async(&mut self.redis_conn)
                                .await?;
                            if let Some(sv) = staged_val {
                                break (sv, conn_count);
                            }
                        };
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
                    }
                    std::cmp::Ordering::Less => {
                        bail!(
                        "KV store polluted: round_ctr ticked backwards: local_curr={:?}, got={:?}",
                        curr_round,
                        round_ctr
                    );
                    }
                }
            }
            .instrument(debug_span!("redis_poll", addr = ?&a)),
        )
    }

    fn leave<'a>(
        &'a mut self,
        addr: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        let mut addr_members_key = addr.clone();
        addr_members_key.push_str("-members");
        let mut addr_join_lock_key = addr.clone();
        addr_join_lock_key.push_str("-joinlock");
        Box::pin(async move {
            self.redis_pubsub
                .punsubscribe::<Option<String>>(None)
                .await
                .wrap_err("unsubscribe")?;
            let mut r = redis::pipe();
            let v: Option<String> = r
                .get(&addr_join_lock_key)
                .query_async(&mut self.redis_conn)
                .await?;
            let mut r = redis::pipe();
            if v.is_some_and(|s| s == addr) {
                r.del(&addr_join_lock_key)
                    .query_async(&mut self.redis_conn)
                    .await?;
                Ok(())
            } else {
                r.zrem(&addr_members_key, &self.client_name)
                    .query_async(&mut self.redis_conn)
                    .await?;
                Ok(())
            }
        })
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
        let mut addr_members_key = addr.clone();
        addr_members_key.push_str("-members");
        let mut round_ctr_key = addr.clone();
        round_ctr_key.push_str("-roundctr");
        Box::pin(
            async move {
                let curr_num_participants = match self
                    .poll_entry(addr.clone(), curr_entry.clone(), curr_round)
                    .await?
                {
                    NegotiateRendezvousResult::Matched {
                        num_participants, ..
                    } => num_participants,
                    x @ NegotiateRendezvousResult::NoMatch { .. } => {
                        return Ok(x);
                    }
                };

                // there is a race between the round_ctr incremented -> our notification -> read
                // and the staged-key being written. During this time poll_entry might return the
                // error "round counter advanced but staged key empty".
                // try to avoid this with a sleep and a retry loop.
                async fn retry_poll_loop(
                    this: &mut RedisBase,
                    addr: String,
                    curr_entry: RendezvousEntry,
                    curr_round: usize,
                    curr_num_participants: usize,
                ) -> Result<NegotiateRendezvousResult, Report> {
                    let mut round_ctr_key = addr.clone();
                    round_ctr_key.push_str("-roundctr");
                    let mut addr_members = addr.clone();
                    addr_members.push_str("-members");
                    let mut poll_retries = 0;
                    loop {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        let (round_ctr, conn_count) = match {
                            let mut r = redis::pipe();
                            r.atomic()
                                .get(&round_ctr_key)
                                .zcard(&addr_members)
                                .query_async::<_, (usize, usize)>(&mut this.redis_conn)
                                .await
                        } {
                            Ok(x) => x,
                            Err(e) => {
                                debug!(err = %format!("{:#}", &e), ?poll_retries, "Poll failed");
                                if poll_retries > 10 {
                                    return Err(e).wrap_err("Poll after notify failed 10 times");
                                }

                                poll_retries += 1;
                                continue;
                            }
                        };

                        // first, check the round.
                        // if the round didn't change, then no transition has happened, so the semantics
                        // won't have changed.
                        // if it did change, then we need to look for the staged semantics.
                        if round_ctr == curr_round && conn_count == curr_num_participants {
                            trace!(
                                ?round_ctr,
                                ?conn_count,
                                ?curr_num_participants,
                                "Same round"
                            );
                            return Ok(NegotiateRendezvousResult::Matched {
                                num_participants: conn_count,
                                round_number: round_ctr,
                            });
                        }

                        match this
                            .poll_entry(addr.clone(), curr_entry.clone(), curr_round)
                            .await
                        {
                            x @ Ok(_) => return x,
                            Err(e) => {
                                debug!(err = %format!("{:#}", &e), ?poll_retries, "Poll failed");
                                if poll_retries > 10 {
                                    return Err(e).wrap_err("Poll after notify failed 10 times");
                                }

                                poll_retries += 1;
                            }
                        }
                    }
                }

                async fn notify_refresh_loop(
                    this: &mut RedisBase,
                    addr: String,
                    curr_entry: RendezvousEntry,
                    curr_round: usize,
                    curr_num_participants: usize,
                ) -> Result<NegotiateRendezvousResult, Report> {
                    // wait for some event on the key.
                    let mut next_refresh_time = Instant::now() + (this.liveness_timeout / 2);
                    loop {
                        let mut message_stream = this.redis_pubsub.on_message();
                        'notify: loop {
                            let refresh_to = tokio::time::sleep_until(next_refresh_time);
                            tokio::pin!(refresh_to);
                            match futures_util::future::select(message_stream.next(), refresh_to)
                                .await
                            {
                                futures_util::future::Either::Left((Some(m), _)) => {
                                    debug!(?m, "subscribe got msg");
                                    break 'notify;
                                }
                                futures_util::future::Either::Left((None, _)) => {
                                    trace!("message stream returned None");
                                    continue;
                                    //return Err(eyre!("redis server shut down"))
                                }
                                futures_util::future::Either::Right((_, _pubsub_fut)) => {
                                    let (round_ctr, conn_count) = refresh_client(
                                        &mut this.redis_conn,
                                        this.liveness_timeout,
                                        &this.client_name,
                                        &addr,
                                    )
                                    .await
                                    .wrap_err("call to refresh_client")?;
                                    if round_ctr > curr_round || conn_count != curr_num_participants
                                    {
                                        break 'notify;
                                    }
                                    next_refresh_time =
                                        Instant::now() + (this.liveness_timeout / 2);
                                }
                            };
                        }

                        std::mem::drop(message_stream);
                        match retry_poll_loop(
                            this,
                            addr.clone(),
                            curr_entry.clone(),
                            curr_round,
                            curr_num_participants,
                        )
                        .await?
                        {
                            NegotiateRendezvousResult::Matched {
                                num_participants, ..
                            } if num_participants == curr_num_participants => continue,
                            x => return Ok(x),
                        }
                    }
                }

                // there are 2 keys to listen on:
                // 1. -members covers cases (1) and (2) above (join or leave)
                // 2. -roundctr covers case (3) (transition)
                let channel_name = format!("__keyspace@*__:{}", &addr_members_key);
                let channel_ctr_name = format!("__keyspace@*__:{} incrby", &round_ctr_key);
                debug!(?channel_name, ?channel_ctr_name, "waiting for events");
                self.redis_pubsub
                    .psubscribe(&[&channel_name, &channel_ctr_name])
                    .await?;
                let res =
                    notify_refresh_loop(self, addr, curr_entry, curr_round, curr_num_participants)
                        .await;
                self.redis_pubsub
                    .punsubscribe(&[&channel_name, &channel_ctr_name])
                    .await?;
                res
            }
            .instrument(debug_span!("redis_notify", addr = ?&a)),
        )
    }

    /// Transition to the new semantics `new_entry` on `addr`.
    ///
    /// Begins a commit. Returns once the staged update counter reaches the number of unexpired
    /// participants. At that time the new semantics are in play.
    fn transition<'a>(
        &'a mut self,
        addr: String,
        new_entry: RendezvousEntry,
    ) -> Pin<Box<dyn Future<Output = Result<usize, Self::Error>> + Send + 'a>> {
        let a = addr.clone();
        let mut addr_members_key = addr.clone();
        addr_members_key.push_str("-members");
        let mut round_ctr_key = addr.clone();
        round_ctr_key.push_str("-roundctr");
        let mut addr_join_lock_key = addr.clone();
        addr_join_lock_key.push_str("-joinlock");

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
                trace!("starting transition");
                let (locked, round_ctr): (bool, usize) = ROUND_CTR_LOCK_SCRIPT
                    .key(&round_ctr_key)
                    .key(&addr_join_lock_key)
                    .arg(&self.client_name)
                    .invoke_async(&mut self.redis_conn)
                    .await
                    .wrap_err("start transition failed")?;
                let staged_val_key = format!("{}-{}-staged", &addr, round_ctr);
                let staged_commit_counter_key = format!("{}-{}-committed-cnt", &addr, round_ctr);

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
                        .set_nx(&staged_val_key, neg_nonce.clone())
                        .set_nx(&staged_commit_counter_key, 1)
                        .zrembyscore(&addr_members_key, 0, expiry_time)
                        .zadd(&addr_members_key, &self.client_name, insert_time as usize)
                        .zcard(&addr_members_key)
                        .query_async(&mut self.redis_conn)
                        .await.wrap_err("Set staged value")?;
                    ensure!(set_staged == 1 && set_commit_ctr == 1, "Polluted KV store: Locked commit but staged values already present");
                    trace!(?set_staged, ?staged_val_key, "Wrote staged value");
                    if conn_count > 1 {
                        self.wait_committer(addr.clone(), round_ctr, 1, conn_count).await?;
                    } else {
                        trace!(?conn_count, "Trivial commit");
                    }

                    // done. write to real key, delete staging keys, and re-incr round counter to even
                    let mut r = redis::pipe();
                    trace!("Cleaning up commit keys");
                    let (_, _, _, round_ctr): ((), isize, isize, usize) = match {
                        r
                            .atomic()
                            .set(&addr, neg_nonce)
                            .del(&staged_val_key)
                            .del(&staged_commit_counter_key)
                            .incr(&round_ctr_key, 1usize)
                            .query_async(&mut self.redis_conn)
                            .await
                            .wrap_err("Cleanup on commit keys")
                    } {
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
                    debug!(?round_ctr, "Existing in-progress transition, convert to applier");
                    // it's impossible to tell here whether the new stack will apply, so we are
                    // just going to become an applier and wait for commit, then return the stack.
                    // if at that point the stack is incompatible, we can error out or start a new
                    // transition.
                    let mut r = redis::pipe();
                    // the committer might have written the lock but not yet the data, so loop on
                    // reading it since we know it is coming.
                    let (curr_count, conn_count) = loop {
                        let insert_time = std::time::SystemTime::now()
                            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                            .as_millis() as u64;
                        let expiry_time = (std::time::SystemTime::now() - self.liveness_timeout)
                            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                            .as_millis() as u64;
                        let (curr_cnt, _, _, conn_cnt): (Option<usize>, (), (), usize) = r
                            .atomic()
                            .get(&staged_commit_counter_key)
                            .zrembyscore(&addr_members_key, 0, expiry_time)
                            .zadd(&addr_members_key, &self.client_name, insert_time as usize)
                            .zcard(&addr_members_key)
                            .query_async(&mut self.redis_conn)
                            .await?;
                        if let Some(c) = curr_cnt {
                             break (c, conn_cnt);
                        }
                    };

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
        self.redis_pubsub
            .punsubscribe::<Option<String>>(None)
            .await
            .wrap_err("unsubscribe")?;
        let staged_commit_counter = format!("{}-{}-committed-cnt", &addr, round_ctr);
        let channel_name = format!("__keyspace@*__:{}", &staged_commit_counter);
        self.redis_pubsub.psubscribe(&channel_name).await?;
        debug!(this = "applier", "waiting on further updates");
        let mut refresh_to = Box::pin(tokio::time::sleep(self.liveness_timeout / 2));
        loop {
            // wait for some event on the key.
            // redis subscribe will never return None.
            let mut do_refresh = false;
            let done = match futures_util::future::select(
                self.redis_pubsub.on_message().next(),
                &mut refresh_to,
            )
            .await
            {
                futures_util::future::Either::Left((Some(msg), _)) => {
                    if !msg.get_channel::<String>()?.ends_with("committed-cnt") {
                        debug!(?msg, "got spurious notification");
                        continue;
                    }

                    let event = msg
                        .get_payload::<String>()
                        .wrap_err_with(|| eyre!("Get payload from message: {:?}", &msg))?;
                    debug!(?msg, ?event, ?round_ctr, "staged-update value changed");
                    event.contains("del")
                }
                futures_util::future::Either::Left((None, _)) => continue,
                futures_util::future::Either::Right((_, _pubsub_fut)) => {
                    // it's fine to drop pubsub_fut and make a new one the next iteration?
                    // reset expiry since we are still alive.
                    refresh_to = Box::pin(tokio::time::sleep(self.liveness_timeout / 2));
                    do_refresh = true;
                    false
                }
            };

            if do_refresh {
                if let Err(err) = refresh_commit(
                    &mut self.redis_conn,
                    self.liveness_timeout,
                    &self.client_name,
                    &addr,
                    round_ctr,
                )
                .await
                {
                    debug!(?err, "refresh_commit failed");
                    break;
                }
            }

            if done {
                break;
            }
        }

        self.redis_pubsub
            .punsubscribe(&channel_name)
            .await
            .wrap_err("punsubscribe")?;
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
            .await
            .wrap_err("fetching new round_ctr")?;
        debug!(?round_ctr, "done applying");
        return Ok(round_ctr);
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
        self.redis_pubsub
            .punsubscribe::<Option<String>>(None)
            .await
            .wrap_err("unsubscribe")?;
        let staged_commit_counter = format!("{}-{}-committed-cnt", &addr, round_ctr);
        let channel_name = format!("__keyspace@*__:{} incrby", &staged_commit_counter);
        self.redis_pubsub.psubscribe(&channel_name).await?;
        debug!(this = "committer", "waiting on further updates");
        loop {
            // wait for some event on the key.
            // redis subscribe will never return None.
            let refresh_to = tokio::time::sleep(self.liveness_timeout / 2);
            tokio::pin!(refresh_to);
            let mut do_refresh = false;
            let mut done = match futures_util::future::select(
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
                let (staged, conn_count) = refresh_commit(
                    &mut self.redis_conn,
                    self.liveness_timeout,
                    &self.client_name,
                    &addr,
                    round_ctr,
                )
                .await?;
                curr_commit_count = staged;
                tot_participant_count = conn_count;
                debug!(?staged, ?tot_participant_count, "refreshed committer");
                done = staged == tot_participant_count;
            }

            if done {
                self.redis_pubsub.punsubscribe(&channel_name).await?;
                debug!("committer has enough acks");
                return Ok(());
            }
        }
    }
}

async fn refresh_commit(
    redis_conn: &mut redis::aio::MultiplexedConnection,
    liveness_timeout: Duration,
    client_name: &str,
    addr: &str,
    round_ctr: usize,
) -> Result<(usize, usize), Report> {
    let mut addr_members = addr.to_owned();
    addr_members.push_str("-members");
    let staged_commit_counter = format!("{}-{}-committed-cnt", &addr, round_ctr);
    loop {
        let mut r = redis::pipe();
        let insert_time = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
            .as_millis() as u64;
        let expiry_time = (std::time::SystemTime::now() - liveness_timeout)
            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
            .as_millis() as u64;
        let (staged, _, _, conn_count): (Option<usize>, isize, isize, usize) = r
            .atomic()
            .get(&staged_commit_counter)
            .zrembyscore(&addr_members, 0usize, expiry_time)
            .zadd(&addr_members, client_name, insert_time as usize)
            .zcard(&addr_members)
            .query_async(redis_conn)
            .await
            .wrap_err("refresh_commit failed")?;
        trace!(?staged, ?conn_count, "refreshed client expiry");
        if let Some(s) = staged {
            return Ok((s, conn_count));
        }
    }
}

async fn refresh_client(
    redis_conn: &mut redis::aio::MultiplexedConnection,
    liveness_timeout: Duration,
    client_name: &str,
    addr: &str,
) -> Result<(usize, usize), Report> {
    let mut addr_members = addr.to_owned();
    addr_members.push_str("-members");
    let mut round_ctr_key = addr.to_owned();
    round_ctr_key.push_str("-roundctr");
    let mut r = redis::pipe();
    let insert_time = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)?
        .as_millis() as u64;
    let expiry_time = (std::time::SystemTime::now() - liveness_timeout)
        .duration_since(std::time::SystemTime::UNIX_EPOCH)?
        .as_millis() as u64;
    let (_, _, roundctr, conncnt): ((), (), usize, usize) = r
        .atomic()
        .zrembyscore(&addr_members, 0usize, expiry_time)
        .zadd(&addr_members, client_name, insert_time as usize)
        .get(&round_ctr_key)
        .zcard(&addr_members)
        .query_async(redis_conn)
        .await
        .wrap_err("refresh_client redis query failed")?;
    Ok((roundctr, conncnt))
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
        //let redis = start_redis(13521);
        let redis_addr = "redis://192.168.64.2:6379";

        rt.block_on(async move {
            //let redis_addr = redis.get_addr();
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
                //let redis = start_redis(13522);
                //let redis_addr = redis.get_addr();
                let redis_addr = "redis://192.168.64.2:6379";
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

                //let redis_addr = redis.get_addr();
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
            nonce: bertha::negotiate::StackNonce::__from_inner(
                [(
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
            ),
        }
    }

    //fn start_redis(port: u16) -> test_util::Redis {
    //    info!(?port, "start redis");
    //    test_util::start_redis(port)
    //}
}
