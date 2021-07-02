use super::{monomorphize, Apply, ApplyResult, GetOffers, NegotiateMsg, Offer, Pick, Select};
use crate::{util::NeverCn, Chunnel, ChunnelConnection, Either};
use color_eyre::eyre::{Report, WrapErr};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex as TokioMutex};
use tracing::{debug, debug_span, instrument, trace, warn};
use tracing_futures::Instrument;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RendezvousEntry {
    pub nonce: HashMap<u64, Offer>,
}

#[derive(Clone, Debug)]
pub enum NegotiateRendezvousResult {
    /// Either:
    /// 1. the client tried to set new semantics, and they matched.
    /// 2. an endpoint left the connection (`num_participants` decremented).
    Matched {
        num_participants: usize,
        round_number: usize,
    },
    /// The offer did not match.
    /// The currently active semantics are returned.
    NoMatch {
        entry: RendezvousEntry,
        num_participants: usize,
        round_number: usize,
    },
}

/// Mechanism to register semantics on an address.
///
/// Basically a KV store.
pub trait RendezvousBackend {
    type Error: Send + Sync;

    /// Set semantics on `addr`, only if no value was previously set.
    ///
    /// If a value *was* previously set and the semantics match, joins the connection.
    /// Otherwise, returns `NegotiateRendezvousResult::NoMatch` and *does not* join the
    /// connection. The client can subsequently join the connection with `poll_entry`.
    fn try_init<'a>(
        &'a mut self,
        addr: String,
        offer: RendezvousEntry,
    ) -> Pin<Box<dyn Future<Output = Result<NegotiateRendezvousResult, Self::Error>> + Send + 'a>>;

    /// Query semantics on `addr`.
    ///
    /// Returns whether the semantics match `curr_entry` (`NegotiateRendezvousResult`), and updates
    /// (or initializes) the expiration timer for this endpoint in the connection.
    ///
    /// If the next semantics round has started, returns the proposed new semantics via `NoMatch`. To accept
    /// these, call `staged_update`. Otherwise do nothing and error out.
    fn poll_entry<'a>(
        &'a mut self,
        addr: String,
        curr_entry: RendezvousEntry,
        curr_round: usize,
    ) -> Pin<Box<dyn Future<Output = Result<NegotiateRendezvousResult, Self::Error>> + Send + 'a>>;

    /// After how long without a poll should a connection be considered dead?
    ///
    /// If < 1ms, uses 1ms.
    fn set_liveness_expiration(&mut self, expiration: std::time::Duration);

    /// Leave the connection.
    ///
    /// This method is optional, since we have a liveness expiration timeout which will auto-delete
    /// us if we just do nothing. Therefore implementations can't rely on this being called
    /// explicitly.
    fn leave<'a>(
        &'a mut self,
        _addr: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        Box::pin(async move { Ok(()) })
    }

    /// Subscribe to the next event on this connection.
    ///
    /// In general, there are three cases to notify about:
    /// 1. A new participant has joined.
    /// 2. A participant left.
    /// 3. The semantics were transitioned.
    ///
    /// If a participant joined, we don't want to have a thundering-horde problem on possibly updating the
    /// semantics, so we just let that participant possibly transition semantics, turning that
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
        Box::pin(async move {
            use NegotiateRendezvousResult::*;
            let (conn_ctr, round_number) = match self
                .poll_entry(addr.clone(), curr_entry.clone(), curr_round)
                .await?
            {
                Matched {
                    num_participants,
                    round_number,
                } => (num_participants, round_number),
                x @ NoMatch { .. } => return Ok(x),
            };

            assert_eq!(
                curr_round, round_number,
                "Semantic round number mismatch: got {:?} expected {:?}",
                round_number, curr_round
            );

            loop {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                let res = self
                    .poll_entry(addr.clone(), curr_entry.clone(), curr_round)
                    .await;
                match res {
                    Ok(Matched {
                        num_participants, ..
                    }) if num_participants == conn_ctr => {
                        debug!("did poll; no change");
                        continue;
                    }
                    r => return r,
                }
            }
        })
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
    ) -> Pin<Box<dyn Future<Output = Result<usize, Self::Error>> + Send + 'a>>;

    /// Increment the staged update counter on `addr`.
    ///
    /// Returns once the commit concludes: when the staged update counter the number of unexpired
    /// partiparticipants.
    fn staged_update<'a>(
        &'a mut self,
        addr: String,
        round_ctr: usize,
    ) -> Pin<Box<dyn Future<Output = Result<usize, Self::Error>> + Send + 'a>>;
}

/// A policy to select which side of a Select to use.
#[derive(Debug, Clone, Copy)]
pub enum SelectPolicy {
    /// Evaluates true if the number of participants is greater than `participant_threshold`.
    Gt {
        participant_threshold: usize,
        preference: Either<(), ()>,
    },
    /// Evaluates true if the number of participants is less than `participant_threshold`.
    Lt {
        participant_threshold: usize,
        preference: Either<(), ()>,
    },
}

impl SelectPolicy {
    fn eval(&self, num_participants: usize) -> bool {
        match self {
            &SelectPolicy::Gt {
                participant_threshold,
                ..
            } if num_participants > participant_threshold => true,
            &SelectPolicy::Lt {
                participant_threshold,
                ..
            } if num_participants < participant_threshold => true,
            _ => false,
        }
    }

    fn pref(&self) -> Either<(), ()> {
        match self {
            &SelectPolicy::Gt { preference, .. } | &SelectPolicy::Lt { preference, .. } => {
                preference
            }
        }
    }
}

/// Negotiation type to choose between T1 and T2 which can change its mind later.
///
/// This type only works with rendezvous negotiation. Handshake negotiation should just drop the
/// connection and start over to change its mind.
///
/// To specify how you would like to change your mind, use `prefer_when`.
///
/// `UpgradeSelect` implements `Apply` => `UpgradeEitherApply`
/// `UpgradeEitherApply` implements `Chunnel` => `UpgradeEitherConn`
/// `UpgradeEitherConn` implements `ChunnelConnection` and exposes `try_upgrade`.
/// `UpgradeEitherConnWrap` wraps `UpgradeEitherConn` to listen for negotiation updates, and calls
/// `try_upgrade` when needed.
pub struct UpgradeSelect<T1, T2> {
    pub left: T1,
    pub right: T2,
    policies: Vec<SelectPolicy>,
}

impl<T1, T2> From<UpgradeSelect<T1, T2>> for Select<T1, T2> {
    fn from(us: UpgradeSelect<T1, T2>) -> Self {
        Select::from((us.left, us.right))
    }
}

impl<T1, T2> From<Select<T1, T2>> for UpgradeSelect<T1, T2> {
    fn from(s: Select<T1, T2>) -> Self {
        UpgradeSelect {
            left: s.left,
            right: s.right,
            policies: vec![],
        }
    }
}

impl<T1, T2> UpgradeSelect<T1, T2> {
    /// Append a policy to the `UpgradeSelect`.
    ///
    /// At runtime, when an event changing the number of connection participants occurs, we will
    /// check the list of policies in the order this function was called in, and apply the first
    /// one that matches.
    ///
    /// # Example
    /// Use the left semantics if there are less than two participants and the right if there are
    /// more than four.
    /// ```
    /// use bertha::{Either, Select, UpgradeSelect, SelectPolicy};
    /// let mut select: UpgradeSelect<_, _> = Select::from(((), ())).into();
    /// select
    ///   .prefer_when(SelectPolicy::Lt { participant_threshold: 2, preference: Either::Left(()) })
    ///   .prefer_when(SelectPolicy::Gt { participant_threshold: 4, preference: Either::Right(()) });
    /// ```
    pub fn prefer_when(&mut self, policy: SelectPolicy) -> &mut Self {
        self.policies.push(policy);
        self
    }
}

impl<T1, T2> Apply for UpgradeSelect<T1, T2>
where
    T1: Apply + Clone,
    T2: Apply + Clone,
{
    type Applied = UpgradeEitherApply<T1, T2>;

    fn apply(
        mut self,
        picked_offers: HashMap<u64, Offer>,
    ) -> Result<ApplyResult<Self::Applied>, Report> {
        let left_saved = self.left.clone();
        let right_saved = self.right.clone();

        let policies = std::mem::take(&mut self.policies);
        let sel: Select<T1, T2> = self.into();
        let ApplyResult {
            applied,
            picked,
            touched,
            score,
        } = sel.apply(picked_offers)?;

        let applied = UpgradeEitherApply::new(applied, left_saved, right_saved, policies);
        Ok(ApplyResult {
            applied,
            picked,
            touched,
            score,
        })
    }
}

pub struct UpgradeEitherApply<A: Apply, B: Apply> {
    left: A,
    right: B,
    current: Either<A::Applied, B::Applied>,
    policies: Vec<SelectPolicy>,
}

impl<A: Apply, B: Apply> UpgradeEitherApply<A, B> {
    fn new(
        applied: Either<A::Applied, B::Applied>,
        a_saved: A,
        b_saved: B,
        policies: Vec<SelectPolicy>,
    ) -> Self {
        Self {
            left: a_saved,
            right: b_saved,
            current: applied,
            policies,
        }
    }
}

impl<InC, A, B, Acn, Bcn> Chunnel<InC> for UpgradeEitherApply<A, B>
where
    InC: Send + 'static,
    A: Apply + Clone + Send + 'static,
    B: Apply + Clone + Send + 'static,
    <A as Apply>::Applied: Chunnel<InC, Connection = Acn> + Clone + Send + 'static,
    <B as Apply>::Applied: Chunnel<InC, Connection = Bcn> + Clone + Send + 'static,
    <<A as Apply>::Applied as Chunnel<InC>>::Future: Send + 'static,
    <<B as Apply>::Applied as Chunnel<InC>>::Future: Send + 'static,
    <<A as Apply>::Applied as Chunnel<InC>>::Error: Into<Report>,
    <<B as Apply>::Applied as Chunnel<InC>>::Error: Into<Report>,
    Acn: ChunnelConnection + 'static,
    Bcn: ChunnelConnection + 'static,
    UpgradeEitherConn<A, B, Acn, Bcn>: ChunnelConnection,
{
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Connection = UpgradeEitherConn<A, B, Acn, Bcn>;
    type Error = Report;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        let left = self.left.clone();
        let right = self.right.clone();
        let policies = self.policies.clone();
        match self {
            UpgradeEitherApply {
                current: Either::Left(ref mut ach),
                ..
            } => {
                let fut = ach.connect_wrap(cn);
                Box::pin(async move {
                    let acn = fut.await.map_err(Into::into)?;
                    Ok(UpgradeEitherConn {
                        left,
                        right,
                        current: Either::Left(acn),
                        policies,
                    })
                })
            }
            UpgradeEitherApply {
                current: Either::Right(ref mut bch),
                ..
            } => {
                let fut = bch.connect_wrap(cn);
                Box::pin(async move {
                    let bcn = fut.await.map_err(Into::into)?;
                    Ok(UpgradeEitherConn {
                        left,
                        right,
                        current: Either::Right(bcn),
                        policies,
                    })
                })
            }
        }
    }
}

pub struct UpgradeEitherConn<A, B, Acn, Bcn> {
    left: A,
    right: B,
    current: Either<Acn, Bcn>,
    policies: Vec<SelectPolicy>,
}

impl<A, B, Acn, Bcn, D> ChunnelConnection for UpgradeEitherConn<A, B, Acn, Bcn>
where
    Acn: ChunnelConnection<Data = D>,
    Bcn: ChunnelConnection<Data = D>,
{
    type Data = D;

    fn send(&self, data: D) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        use Either::*;
        match self.current {
            Left(ref a) => a.send(data),
            Right(ref b) => b.send(data),
        }
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        use Either::*;
        match self.current {
            Left(ref a) => a.recv(),
            Right(ref b) => b.recv(),
        }
    }
}

impl<A, B, Acn, Bcn> UpgradeEitherConn<A, B, Acn, Bcn>
where
    A: Apply + Clone,
    B: Apply + Clone,
    Select<A, B>: Apply<Applied = Either<<A as Apply>::Applied, <B as Apply>::Applied>>,
    Either<<A as Apply>::Applied, <B as Apply>::Applied>:
        Chunnel<NeverCn, Connection = Either<Acn, Bcn>>,
    <Either<<A as Apply>::Applied, <B as Apply>::Applied> as Chunnel<NeverCn>>::Error: Into<Report>,
{
    async fn try_upgrade(&mut self, new_offers: HashMap<u64, Offer>) -> Result<(), Report> {
        let sel = Select::from((self.left.clone(), self.right.clone()));
        let ApplyResult { mut applied, .. } = sel.apply(new_offers)?;
        self.current = applied
            .connect_wrap(NeverCn::default())
            .await
            .map_err(Into::into)?;
        Ok(())
    }
}

pub struct UpgradeEitherConnWrap<A, B, Acn, Bcn> {
    negotiation: Arc<TokioMutex<mpsc::Receiver<HashMap<u64, Offer>>>>,
    inner: Arc<TokioMutex<UpgradeEitherConn<A, B, Acn, Bcn>>>,
}

impl<D, A, B, Acn, Bcn> ChunnelConnection for UpgradeEitherConnWrap<A, B, Acn, Bcn>
where
    UpgradeEitherConn<A, B, Acn, Bcn>: ChunnelConnection<Data = D>,
    A: Apply + Clone + Send + Sync + 'static,
    B: Apply + Clone + Send + Sync + 'static,
    Select<A, B>: Apply<Applied = Either<<A as Apply>::Applied, <B as Apply>::Applied>>,
    Either<<A as Apply>::Applied, <B as Apply>::Applied>:
        Chunnel<NeverCn, Connection = Either<Acn, Bcn>>,
    <Either<<A as Apply>::Applied, <B as Apply>::Applied> as Chunnel<NeverCn>>::Error: Into<Report>,
    <A as Apply>::Applied: Chunnel<NeverCn, Connection = Acn> + Clone + Send + 'static,
    <B as Apply>::Applied: Chunnel<NeverCn, Connection = Bcn> + Clone + Send + 'static,
    <<A as Apply>::Applied as Chunnel<NeverCn>>::Error: Into<Report>,
    <<B as Apply>::Applied as Chunnel<NeverCn>>::Error: Into<Report>,
    Acn: Send + Sync + 'static,
    Bcn: Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Data = D;
    fn send(&self, data: D) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        use std::task::Poll;
        let inner = Arc::clone(&self.inner);
        let neg = Arc::clone(&self.negotiation);
        Box::pin(async move {
            let mut upgrade_receiver = neg.lock().await;
            let f = Box::pin(upgrade_receiver.recv());
            if let Poll::Ready(Some(upgrade)) = futures_util::poll!(f) {
                debug!("applying upgraded semantics (send)");
                inner.lock().await.try_upgrade(upgrade).await?;
            }

            std::mem::drop(upgrade_receiver);
            inner.lock().await.send(data).await
        })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        use futures_util::future;
        let inner = Arc::clone(&self.inner);
        let neg = Arc::clone(&self.negotiation);
        let upgrade_fut = async move {
            let mut rg = neg.lock().await;
            if let Some(u) = rg.recv().await {
                u
            } else {
                std::mem::drop(rg);
                future::pending().await
            }
        };
        let inner2 = Arc::clone(&self.inner);
        let recv_fut = async move { inner.lock().await.recv().await };

        Box::pin(async move {
            // We need to `Box::pin` this so that the `drop` call below actually drops the future,
            // instead of dropping a `Pin` of the future. We need to actually drop the future so
            // that it drops the `MutexGuard` it holds, so the lock doesn't deadlock.
            let recv_fut = Box::pin(recv_fut);
            let upgrade_fut = Box::pin(upgrade_fut);
            match future::select(recv_fut, upgrade_fut).await {
                future::Either::Left((recvd, _)) => recvd,
                future::Either::Right((upgrade, recvr)) => {
                    debug!("received on upgrade channel");
                    std::mem::drop(recvr); // cancel the future and drop, so its lock on inner is dropped.
                    let mut inner = inner2.lock().await;
                    debug!("applying upgraded semantics (recv)");
                    inner.try_upgrade(upgrade).await?;
                    inner.recv().await
                }
            }
        })
    }
}

impl<D, A, B, Acn, Bcn> UpgradeEitherConnWrap<A, B, Acn, Bcn>
where
    UpgradeEitherConn<A, B, Acn, Bcn>: ChunnelConnection<Data = D>,
    A: Pick + Apply + GetOffers + Clone + Debug + Send + 'static,
    B: Pick + Apply + GetOffers + Clone + Debug + Send + 'static,
    <A as Pick>::Picked: Debug + Clone + Send + 'static,
    <B as Pick>::Picked: Debug + Clone + Send + 'static,
    <A as Apply>::Applied: Chunnel<NeverCn, Connection = Acn> + Clone + Send + 'static,
    <B as Apply>::Applied: Chunnel<NeverCn, Connection = Bcn> + Clone + Send + 'static,
    <<A as Apply>::Applied as Chunnel<NeverCn>>::Error: Into<Report>,
    <<B as Apply>::Applied as Chunnel<NeverCn>>::Error: Into<Report>,
    Select<A, B>: Apply<Applied = Either<<A as Apply>::Applied, <B as Apply>::Applied>>,
{
    async fn with_negotiator<R>(
        mut inner: UpgradeEitherConn<A, B, Acn, Bcn>,
        mut negotiator: R,
        addr: String,
        curr_entry: RendezvousEntry,
        mut curr_participant_count: usize,
        mut curr_round: usize,
    ) -> Self
    where
        R: RendezvousBackend + Send + 'static,
        <R as RendezvousBackend>::Error: Into<Report> + Send,
    {
        let (s, r) = mpsc::channel(4);
        let a = addr.clone();
        let negotiation = Arc::new(TokioMutex::new(r));
        let neg_l = Arc::clone(&negotiation);
        let policies = std::mem::take(&mut inner.policies);
        let mut curr_branch_is_left = inner.current.is_left();
        let left_opt = inner.left.clone();
        let right_opt = inner.right.clone();
        let left_offer = if let (_stack, NegotiateMsg::ServerNonce { picked, .. }, _client_offers) =
            monomorphize(
                left_opt.clone(),
                left_opt.offers().collect(),
                &String::new(),
            )
            .unwrap()
        {
            picked
        } else {
            unreachable!()
        };
        let right_offer =
            if let (_stack, NegotiateMsg::ServerNonce { picked, .. }, _client_offers) =
                monomorphize(
                    right_opt.clone(),
                    right_opt.offers().collect(),
                    &String::new(),
                )
                .unwrap()
            {
                picked
            } else {
                unreachable!()
            };
        tokio::spawn(
            async move {
                debug!("starting");
                loop {
                    let res = negotiator.notify(addr.clone(), curr_entry.clone(), curr_round).await;
                    if s.is_closed() {
                        debug!("upgrade receiver closed, exiting");
                        return;
                    }

                    let nonce = match res {
                        Ok(NegotiateRendezvousResult::Matched { num_participants, .. })
                            if num_participants == curr_participant_count =>
                        {
                            continue; // do nothing.
                        }
                        Ok(NegotiateRendezvousResult::NoMatch {
                            entry,
                            num_participants,
                            round_number,
                        }) => {
                            curr_participant_count = num_participants;
                            curr_round = round_number;
                            // Check if `entry` is compatible. If so, ACK with staged_update.
                            // Don't actually use the applied value, try_upgrade will handle that.
                            let sel = Select::from((left_opt.clone(), right_opt.clone()));
                            match sel.apply(entry.nonce.clone()) {
                                Ok(ApplyResult { applied, ..}) => {
                                    curr_branch_is_left = applied.is_left();
                                    debug!(?num_participants, ?curr_branch_is_left, "Got new compatible semantics");
                                    // we lock this not to use it, but to prevent a send while the commit is happening.
                                    let _neg_g = neg_l.lock().await;
                                    if let Err(e) = negotiator.staged_update(addr.clone(), curr_round).await {
                                        let r = e.into();
                                        warn!(err = ?r, "staged_update failed");
                                        return;
                                    }

                                    entry.nonce
                                }
                                Err(e) => {
                                    debug!(err = %format!("{:#}", e), "Could not apply new semantics, cannot commit");
                                    continue;
                                }
                            }
                        }
                        Err(e) => {
                            let r = e.into();
                            warn!(err = ?r, "notify failed");
                            continue;
                        }
                        Ok(NegotiateRendezvousResult::Matched { num_participants, .. }) => {
                            // the semantics are the same, but the number of participants changed.
                            // consider transitioning.
                            if let Some(ent) = policies.iter().find_map(|&p| {
                                if !p.eval(num_participants) { None } else {
                                    match p.pref() {
                                        Either::Left(_) if !curr_branch_is_left => Some(left_offer.clone()),
                                        Either::Right(_) if curr_branch_is_left => Some(right_offer.clone()),
                                        _ => None,
                                    }
                                }
                            }) {
                                curr_participant_count = num_participants;
                                // we lock this not to use it, but to prevent a send while the commit is happening.
                                let _neg_g = neg_l.lock().await;
                                if let Err(e) = negotiator.transition(addr.clone(), RendezvousEntry { nonce: ent.clone() }).await {
                                    let r = e.into();
                                    warn!(err = ?r, "staged_update failed");
                                    return;
                                }

                                ent
                            } else {
                                debug!(?num_participants, "No policies applied, doing nothing");
                                continue;
                            }
                        }
                    };

                    debug!("upgrade committed, channel send");
                    s.send(nonce).await.unwrap();
                }
            }
            .instrument(debug_span!("negotiate_runtime", addr = ?a)),
        );

        Self {
            negotiation,
            inner: Arc::new(TokioMutex::new(inner)),
        }
    }
}

/// Rendezvous-based negotiation.
#[instrument(skip(stack, rendezvous_point))]
pub async fn negotiate_rendezvous<Sleft, Sright, SleftCn, SrightCn, R>(
    stack: UpgradeSelect<Sleft, Sright>,
    mut rendezvous_point: R,
    addr: String,
) -> Result<UpgradeEitherConnWrap<Sleft, Sright, SleftCn, SrightCn>, Report>
where
    Sleft: Pick + Apply + GetOffers + Debug + Clone + Send + Sync + 'static,
    <Sleft as Pick>::Picked: Clone + Debug + Send,
    <Sleft as Apply>::Applied: Chunnel<NeverCn, Connection = SleftCn> + Clone + Debug + Send,
    <<Sleft as Apply>::Applied as Chunnel<NeverCn>>::Error: Into<Report>,
    Sright: Pick + Apply + GetOffers + Debug + Clone + Send + Sync + 'static,
    <Sright as Pick>::Picked: Clone + Debug + Send,
    <Sright as Apply>::Applied: Chunnel<NeverCn, Connection = SrightCn> + Clone + Debug + Send,
    <<Sright as Apply>::Applied as Chunnel<NeverCn>>::Error: Into<Report>,
    UpgradeEitherApply<Sleft, Sright>:
        Chunnel<NeverCn, Connection = UpgradeEitherConn<Sleft, Sright, SleftCn, SrightCn>>,
    <UpgradeEitherApply<Sleft, Sright> as Chunnel<NeverCn>>::Error: Into<Report>,
    UpgradeEitherConn<Sleft, Sright, SleftCn, SrightCn>: ChunnelConnection,
    R: RendezvousBackend + Send + 'static,
    <R as RendezvousBackend>::Error: Into<Report> + Send,
{
    debug!("starting");
    let left_offer = solo_monomorphize(stack.left.clone())?;
    let right_offer = solo_monomorphize(stack.right.clone())?;

    let offer = stack
        .policies
        .iter()
        .find_map(|&p| {
            if !p.eval(1) {
                None
            } else {
                match p.pref() {
                    Either::Left(_) => Some(left_offer.clone()),
                    Either::Right(_) => Some(right_offer.clone()),
                }
            }
        })
        .unwrap_or_else(|| left_offer.clone());

    trace!(?offer, "monomorphized sole occupancy stack");
    let picked = offer.clone();

    // 1. try_init our favored semantics.
    let res = {
        let rp = &mut rendezvous_point;
        rp.try_init(addr.clone(), RendezvousEntry { nonce: offer })
            .await
            .map_err(Into::into)
            .wrap_err("rendezvous backend try_init")?
    };

    trace!(?res, "got try_init response");
    match res {
        NegotiateRendezvousResult::Matched {
            num_participants,
            round_number,
        } => {
            // if Matched, we joined the connection.
            let ApplyResult { mut applied, .. } = stack
                .apply(picked.clone())
                .expect("solo_monomorphize means self-application will work");
            let cn = applied
                .connect_wrap(NeverCn::default())
                .await
                .map_err(Into::into)?;
            let cn = UpgradeEitherConnWrap::with_negotiator(
                cn,
                rendezvous_point,
                addr.clone(),
                RendezvousEntry { nonce: picked },
                num_participants,
                round_number,
            )
            .await;
            debug!(matched = true, "returning upgradable connection");
            Ok(cn)
        }
        NegotiateRendezvousResult::NoMatch {
            num_participants,
            round_number,
            entry,
        } => {
            let ApplyResult { mut applied, .. } = stack
                .apply(entry.nonce.clone())
                .wrap_err("Could not apply current semantics on addr")?;
            let cn = applied
                .connect_wrap(NeverCn::default())
                .await
                .map_err(Into::into)?;
            let cn = UpgradeEitherConnWrap::with_negotiator(
                cn,
                rendezvous_point,
                addr.clone(),
                entry,
                num_participants,
                round_number,
            )
            .await;
            debug!(matched = false, "returning upgradable connection");
            Ok(cn)
        }
    }
}

// "negotiate" against ourselves for a nonce.
// we impl Pick just to pass to monomorphize, for our actual stack we will use the nonce, since
// our Either type explicitly handles apply.
fn solo_monomorphize<T>(stack: T) -> Result<HashMap<u64, Offer>, Report>
where
    T: Pick + Apply + GetOffers + Debug + Clone + Send + Sync + 'static,
    <T as Pick>::Picked: Clone + Debug + Send + 'static,
{
    let offers = stack.offers().collect();
    if let (_stack, NegotiateMsg::ServerNonce { picked, .. }, _client_offers) =
        monomorphize(stack, offers, &String::new())?
    {
        Ok(picked)
    } else {
        unreachable!()
    }
}
