use super::{monomorphize, Apply, ApplyResult, GetOffers, NegotiateMsg, Pick, Select, StackNonce};
use crate::negotiate::server::stack_pair_valid;
use crate::Offer;
use crate::{util::NeverCn, Chunnel, ChunnelConnection, Either};
use color_eyre::eyre::{bail, eyre, Report, WrapErr};
use futures_util::future;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{oneshot, watch, Mutex as TokioMutex};
use tracing::{debug, debug_span, instrument, trace, Instrument};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RendezvousEntry {
    pub nonce: StackNonce,
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

            debug!(?conn_ctr, ?round_number, "polling for changes");

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
                        trace!("did poll; no change");
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

pub trait CollectUpgradeHandles {
    fn collect_handles(&self) -> Vec<Arc<UpgradeHandle>>;
}

impl<H, T> CollectUpgradeHandles for crate::CxList<H, T>
where
    H: CollectUpgradeHandles,
    T: CollectUpgradeHandles,
{
    fn collect_handles(&self) -> Vec<Arc<UpgradeHandle>> {
        let mut x = self.head.collect_handles();
        x.extend(self.tail.collect_handles());
        x
    }
}

impl<N> CollectUpgradeHandles for N
where
    N: crate::Negotiate,
{
    fn collect_handles(&self) -> Vec<Arc<UpgradeHandle>> {
        vec![]
    }
}

impl<T1, T2> CollectUpgradeHandles for UpgradeSelect<T1, T2>
where
    T1: CollectUpgradeHandles,
    T2: CollectUpgradeHandles,
{
    fn collect_handles(&self) -> Vec<Arc<UpgradeHandle>> {
        let mut x = self.inner.collect_handles();
        x.push(self.handle.clone());
        x
    }
}

impl<T1, T2> CollectUpgradeHandles for Select<T1, T2>
where
    T1: CollectUpgradeHandles,
    T2: CollectUpgradeHandles,
{
    fn collect_handles(&self) -> Vec<Arc<UpgradeHandle>> {
        let mut x = self.left.collect_handles();
        x.extend(self.right.collect_handles());
        x
    }
}

pub trait Prefer {
    fn prefer(
        &mut self,
        _num_participants: usize,
        _selector: impl Fn(usize, &Arc<UpgradeHandle>) -> Option<Either<(), ()>>,
    ) {
    }
}

impl<N> Prefer for N where N: crate::Negotiate {}

impl<H, T> Prefer for crate::CxList<H, T>
where
    H: Prefer,
    T: Prefer,
{
    fn prefer(
        &mut self,
        num_participants: usize,
        selector: impl Fn(usize, &Arc<UpgradeHandle>) -> Option<Either<(), ()>>,
    ) {
        self.head.prefer(num_participants, &selector);
        self.tail.prefer(num_participants, &selector);
    }
}

impl<T1, T2> Prefer for UpgradeSelect<T1, T2>
where
    T1: Prefer,
    T2: Prefer,
{
    fn prefer(
        &mut self,
        num_participants: usize,
        selector: impl Fn(usize, &Arc<UpgradeHandle>) -> Option<Either<(), ()>>,
    ) {
        <Select<T1, T2> as Prefer>::prefer(&mut self.inner, num_participants, &selector);
        if let Some(opt) = selector(num_participants, &self.handle) {
            self.inner.prefer = opt;
        }
    }
}

impl<T1, T2> Prefer for Select<T1, T2>
where
    T1: Prefer,
    T2: Prefer,
{
    fn prefer(
        &mut self,
        num_participants: usize,
        selector: impl Fn(usize, &Arc<UpgradeHandle>) -> Option<Either<(), ()>>,
    ) {
        self.left.prefer(num_participants, &selector);
        self.right.prefer(num_participants, &selector);
    }
}

/// Negotiation type to choose between T1 and T2 which can change its mind later.
///
/// `UpgradeSelect` implements `Apply` => `UpgradeEitherApply`
/// `UpgradeEitherApply` implements `Chunnel` => `UpgradeEitherConn`
/// `UpgradeEitherConn` implements `ChunnelConnection` and exposes `try_upgrade`.
/// `UpgradeEitherConnWrap` wraps `UpgradeEitherConn` to listen for negotiation updates, and calls
/// `try_upgrade` when needed.
#[derive(Clone, Debug)]
pub struct UpgradeSelect<T1, T2> {
    inner: Select<T1, T2>,
    trigger: watch::Receiver<StackNonce>,
    stack_changed: Arc<watch::Sender<Option<Either<(), ()>>>>,
    handle: Arc<UpgradeHandle>,
}

impl<T1, T2> UpgradeSelect<T1, T2>
where
    T1: GetOffers,
    T2: GetOffers,
{
    pub fn from_select(inner: Select<T1, T2>) -> (Self, Arc<UpgradeHandle>) {
        let (stack_sender, stack_receiver) = watch::channel(Default::default());
        let (stack_changed_s, stack_changed_r) = watch::channel(None);
        let stack_changed_s = Arc::new(stack_changed_s);
        let uh = Arc::new(UpgradeHandle::new(&inner, stack_sender, stack_changed_r));
        let this = Self {
            inner,
            trigger: stack_receiver,
            stack_changed: stack_changed_s,
            handle: uh.clone(),
        };

        (this, uh)
    }
}

impl<T1, T2> GetOffers for UpgradeSelect<T1, T2>
where
    Select<T1, T2>: GetOffers,
{
    type Iter = <Select<T1, T2> as GetOffers>::Iter;

    fn offers(&self) -> Self::Iter {
        self.inner.offers()
    }
}

impl<T1, T2> Pick for UpgradeSelect<T1, T2>
where
    Select<T1, T2>: Pick,
{
    type Picked = <Select<T1, T2> as Pick>::Picked;

    fn pick(
        self,
        offer_pairs: Vec<(StackNonce, StackNonce)>,
    ) -> Result<super::pick::PickResult<Self::Picked>, Report> {
        self.inner.pick(offer_pairs)
    }
}

impl<T1, T2> Apply for UpgradeSelect<T1, T2>
where
    T1: Apply + Clone,
    T2: Apply + Clone,
{
    type Applied = UpgradeEitherApply<T1, T2>;

    fn apply(self, picked_offers: StackNonce) -> Result<ApplyResult<Self::Applied>, Report> {
        let left_saved = self.inner.left.clone();
        let right_saved = self.inner.right.clone();
        let ApplyResult {
            applied,
            picked,
            touched,
            score,
        } = self.inner.apply(picked_offers)?;
        self.stack_changed
            .send(match applied {
                Either::Left(_) => Some(Either::Left(())),
                Either::Right(_) => Some(Either::Right(())),
            })
            .unwrap();
        let applied = UpgradeEitherApply::new(
            applied,
            left_saved,
            right_saved,
            picked.clone(),
            self.trigger,
            self.stack_changed,
        );
        Ok(ApplyResult {
            applied,
            picked,
            touched,
            score,
        })
    }
}

#[derive(Clone, Debug)]
pub struct UpgradeEitherApply<A: Apply, B: Apply> {
    left: A,
    right: B,
    current: Either<A::Applied, B::Applied>,
    picked: StackNonce,
    switch_listener: watch::Receiver<StackNonce>,
    stack_changed: Arc<watch::Sender<Option<Either<(), ()>>>>,
}

impl<A: Apply, B: Apply> UpgradeEitherApply<A, B> {
    fn new(
        applied: Either<A::Applied, B::Applied>,
        a_saved: A,
        b_saved: B,
        picked: StackNonce,
        switch_listener: watch::Receiver<StackNonce>,
        stack_changed: Arc<watch::Sender<Option<Either<(), ()>>>>,
    ) -> Self {
        Self {
            left: a_saved,
            right: b_saved,
            current: applied,
            picked,
            switch_listener,
            stack_changed,
        }
    }
}

impl<InC, A, B, Acn, Bcn> Chunnel<InC> for UpgradeEitherApply<A, B>
where
    InC: Send + Sync + 'static,
    A: Apply + Clone + Send + 'static,
    B: Apply + Clone + Send + 'static,
    <A as Apply>::Applied: Chunnel<Arc<InC>, Connection = Acn> + Clone + Send + 'static,
    <B as Apply>::Applied: Chunnel<Arc<InC>, Connection = Bcn> + Clone + Send + 'static,
    <<A as Apply>::Applied as Chunnel<Arc<InC>>>::Future: Send + 'static,
    <<B as Apply>::Applied as Chunnel<Arc<InC>>>::Future: Send + 'static,
    <<A as Apply>::Applied as Chunnel<Arc<InC>>>::Error: Into<Report>,
    <<B as Apply>::Applied as Chunnel<Arc<InC>>>::Error: Into<Report>,
    Acn: ChunnelConnection + Send + 'static,
    Bcn: ChunnelConnection + Send + 'static,
    UpgradeEitherConn<A, B, Acn, Bcn, InC>: ChunnelConnection,
{
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Connection = UpgradeEitherConn<A, B, Acn, Bcn, InC>;
    type Error = Report;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        let cn = Arc::new(cn);
        let left = self.left.clone();
        let right = self.right.clone();
        let sl = self.switch_listener.clone();
        let sc = self.stack_changed.clone();
        let p = self.picked.clone();
        match self {
            UpgradeEitherApply {
                current: Either::Left(ref mut ach),
                ..
            } => {
                let fut = ach.connect_wrap(cn.clone());
                Box::pin(async move {
                    let acn = fut.await.map_err(Into::into)?;
                    Ok(UpgradeEitherConn {
                        left,
                        right,
                        base: cn,
                        current: Arc::new(TokioMutex::new(Either::Left(acn))),
                        switch_listener: Arc::new(TokioMutex::new((sl, p, None))),
                        stack_changed: sc,
                    })
                })
            }
            UpgradeEitherApply {
                current: Either::Right(ref mut bch),
                ..
            } => {
                let fut = bch.connect_wrap(cn.clone());
                Box::pin(async move {
                    let bcn = fut.await.map_err(Into::into)?;
                    Ok(UpgradeEitherConn {
                        left,
                        right,
                        base: cn,
                        current: Arc::new(TokioMutex::new(Either::Right(bcn))),
                        switch_listener: Arc::new(TokioMutex::new((sl, p, None))),
                        stack_changed: sc,
                    })
                })
            }
        }
    }
}

#[derive(Clone)]
pub struct UpgradeEitherConn<A, B, Acn, Bcn, InC> {
    left: A,
    right: B,
    base: Arc<InC>,
    current: Arc<TokioMutex<Either<Acn, Bcn>>>,
    switch_listener: Arc<
        TokioMutex<(
            watch::Receiver<StackNonce>,
            StackNonce,
            Option<Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>>>,
        )>,
    >,
    stack_changed: Arc<watch::Sender<Option<Either<(), ()>>>>,
}

impl<A, B, Acn, Bcn, InC> Debug for UpgradeEitherConn<A, B, Acn, Bcn, InC>
where
    A: Debug,
    B: Debug,
    InC: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpgradeEitherConn")
            .field("left", &self.left)
            .field("right", &self.right)
            .field("base", &self.base)
            .finish()
    }
}

impl<A, B, Acn, Bcn, InC, D> ChunnelConnection for UpgradeEitherConn<A, B, Acn, Bcn, InC>
where
    A: Apply + Clone + Send + Sync + 'static,
    B: Apply + Clone + Send + Sync + 'static,
    Select<A, B>: Apply<Applied = Either<<A as Apply>::Applied, <B as Apply>::Applied>>,
    Either<<A as Apply>::Applied, <B as Apply>::Applied>:
        Chunnel<Arc<InC>, Connection = Either<Acn, Bcn>>,
    <Either<<A as Apply>::Applied, <B as Apply>::Applied> as Chunnel<Arc<InC>>>::Error:
        Into<Report>,
    Acn: ChunnelConnection<Data = D> + Send + Sync + 'static,
    Bcn: ChunnelConnection<Data = D> + Send + Sync + 'static,
    D: Send,
    InC: Send + Sync + 'static,
{
    type Data = D;

    fn send<'cn, R>(
        &'cn self,
        burst: R,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        R: IntoIterator<Item = Self::Data> + Send + 'cn,
        <R as IntoIterator>::IntoIter: Send,
    {
        Box::pin(async move {
            let mut sl_g = self.switch_listener.lock().await;
            let (ref mut listener, ref mut curr_nonce, ref mut in_progress) = *sl_g;
            if in_progress.is_none() && listener.has_changed()? {
                let v = listener.borrow_and_update();
                if *v != *curr_nonce {
                    *curr_nonce = v.clone();
                    *in_progress = Some(self.try_upgrade(v.clone()));
                }
            }

            if let Some(f) = in_progress.as_mut() {
                f.await?;
                *in_progress = None;
            }

            std::mem::drop(sl_g);
            self.current.lock().await.send(burst).await
        })
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        Box::pin(async move {
            // since we select on non-recv stuff, we loop back in those cases until recv() happens.
            loop {
                let upgrade_fut = async {
                    let mut sl_g = self.switch_listener.lock().await;
                    let (ref mut listener, ref mut curr_nonce, ref mut in_progress) = *sl_g;
                    loop {
                        if in_progress.is_some() {
                            debug!("try_upgrade already in progress");
                            break;
                        } else {
                            listener.changed().await.wrap_err("sender dropped")?;
                            let v = listener.borrow_and_update();
                            if *v != *curr_nonce {
                                debug!("new try_upgrade");
                                *curr_nonce = v.clone();
                                *in_progress = Some(self.try_upgrade(v.clone()));
                                break;
                            }
                        }
                    }

                    Ok::<_, Report>(())
                };

                let mut slots: Vec<_> = (0..msgs_buf.len()).map(|_| None).collect();
                let recv_fut = async { self.current.lock().await.recv(&mut slots).await };

                // We need to `Box::pin` this instead of stack pinning so that the `drop` call
                // below actually drops the future, instead of dropping a `Pin` of the future. We
                // need to actually drop the future so that it drops the `MutexGuard` it holds, so
                // the lock doesn't deadlock.
                let recv_fut = Box::pin(recv_fut);
                let upgrade_fut = Box::pin(upgrade_fut);
                // we need a temporary variable here to let the compiler figure out slots_borrow will
                // be dropped before slots
                match future::select(recv_fut, upgrade_fut).await {
                    future::Either::Left((recvd, _)) => {
                        let mut slot_idx = 0;
                        for r in recvd?.iter_mut().map_while(Option::take) {
                            msgs_buf[slot_idx] = Some(r);
                            slot_idx += 1;
                        }

                        return Ok(&mut msgs_buf[..slot_idx]);
                    }
                    future::Either::Right((do_upgrade, recvr)) => {
                        // important: cancel the recv() future and drop, so its lock on inner is
                        // dropped. otherwise try_upgrade() will try to take the lock and deadlock.
                        std::mem::drop(recvr);
                        do_upgrade?;
                        let mut sl_g = self.switch_listener.lock().await;
                        let (_, _, ref mut in_progress) = *sl_g;
                        in_progress
                            .as_mut()
                            .unwrap()
                            .await
                            .wrap_err("UpgradeEitherConn could not apply new stack")?;
                        *in_progress = None;
                        continue;
                    }
                };
            }
        })
    }
}

impl<A, B, Acn, Bcn, InC> UpgradeEitherConn<A, B, Acn, Bcn, InC>
where
    A: Apply + Clone + Send + 'static,
    B: Apply + Clone + Send + 'static,
    Select<A, B>: Apply<Applied = Either<<A as Apply>::Applied, <B as Apply>::Applied>>,
    Either<<A as Apply>::Applied, <B as Apply>::Applied>:
        Chunnel<Arc<InC>, Connection = Either<Acn, Bcn>>,
    InC: Send + Sync + 'static,
    Acn: ChunnelConnection + Send + 'static,
    Bcn: ChunnelConnection + Send + 'static,
    <Either<<A as Apply>::Applied, <B as Apply>::Applied> as Chunnel<Arc<InC>>>::Error:
        Into<Report>,
{
    fn try_upgrade(
        &self,
        new_offers: StackNonce,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        debug!(?new_offers, "received on upgrade channel");
        let sel = Select::from((self.left.clone(), self.right.clone()));
        let base = self.base.clone();
        let current = self.current.clone();
        let changed = self.stack_changed.clone();
        Box::pin(
            async move {
                let new_fut = {
                    let ApplyResult { mut applied, .. } = sel.apply(new_offers)?;
                    debug!("applied new offers");
                    applied.connect_wrap(base)
                }; // drop applied so it is not held across await

                let new = new_fut.await.map_err(Into::into)?;
                let v = match new {
                    Either::Left(_) => Either::Left(()),
                    Either::Right(_) => Either::Right(()),
                };
                let mut inner = current.lock().await;
                debug!("applying upgraded semantics");
                *inner = new;

                changed.send_if_modified(|curr| {
                    if *curr != Some(v) {
                        *curr = Some(v);
                        true
                    } else {
                        false
                    }
                });
                Ok(())
            }
            .instrument(debug_span!("try_upgrade")),
        )
    }
}

impl<A, B, Acn, Bcn, InC> Drop for UpgradeEitherConn<A, B, Acn, Bcn, InC> {
    fn drop(&mut self) {
        self.stack_changed.send(None).unwrap_or(());
    }
}

/// Rendezvous-based negotiation.
///
/// Returns a connection to use, and a `StackUpgradeHandle` to manage the negotiation update task.
/// Triggering a change with an individual `UpgradeHandle` will also cause a negotiation commit
/// cycle.
///
/// The provided `selector` callback is called during connection establishment if (a) there is an
/// existing stack in the `rendezvous_point` and (b) it does not `Apply` to `stack`. The first
/// arugment is the current number of connection participants, and the second is the
/// `UpgradeHandle` corresponding to the `UpgradeSelect` to optionally express a preference for.
#[instrument(skip(stack, rendezvous_point, selector))]
pub async fn negotiate_rendezvous<Srv, Cn, R, E>(
    mut stack: Srv,
    mut rendezvous_point: R,
    addr: String,
    selector: impl Fn(usize, &Arc<UpgradeHandle>) -> Option<Either<(), ()>>,
) -> Result<(Cn, StackUpgradeHandle<R>), Report>
where
    Srv: CollectUpgradeHandles
        + Pick
        + Apply
        + GetOffers
        + Prefer
        + Debug
        + Clone
        + Send
        + Sync
        + 'static,
    <Srv as Pick>::Picked: Debug,
    <Srv as Apply>::Applied: Chunnel<NeverCn, Connection = Cn, Error = E>,
    E: Into<Report>,
    R: RendezvousBackend + Send + 'static,
    <R as RendezvousBackend>::Error: Into<Report> + Send,
{
    debug!("starting");
    // 1. get the stack of us by ourselves
    let offers: Vec<StackNonce> = stack.offers().collect();
    let offer = solo_monomorphize(stack.clone()).wrap_err("Malformed chunnel stack")?;
    trace!(?offer, "monomorphized sole occupancy stack");
    let picked = offer.clone();

    // 2. collect the upgrade handles to use
    let handles = stack.collect_handles();

    // 2. try_init our favored semantics.
    let (entry, num_participants, mut round_number) = match rendezvous_point
        .try_init(addr.clone(), RendezvousEntry { nonce: offer })
        .await
        .map_err(Into::into)
        .wrap_err("rendezvous backend try_init")?
    {
        NegotiateRendezvousResult::Matched {
            num_participants,
            round_number,
        } => {
            // if Matched, we joined the connection.
            (None, num_participants, round_number)
        }
        NegotiateRendezvousResult::NoMatch {
            num_participants,
            round_number,
            entry,
        } => (Some(entry), num_participants, round_number),
    };

    debug!(matched = entry.is_none(), num_selects = ?handles.len(), "try_init completed");
    let (entry, mut applied) = if let Some(existing_remote_stack) = entry {
        match stack.clone().apply(existing_remote_stack.clone().nonce) {
            Ok(ApplyResult { applied, .. }) => {
                debug!("remote stack was compatible, applied");
                (existing_remote_stack, applied)
            }
            Err(orig_apply_error) => {
                debug!(
                    ?existing_remote_stack,
                    "remote stack was incompatible, asking for selection input"
                );

                // TODO there is no way currently to ask the remote for alternate stacks. this
                // means that if the local picked stack is not compatible, and the remote stack is
                // not locally compatible, but there is a third option on both sides that *is*
                // compatible, we will miss it.
                //
                // here, we give the caller a chance to react to new connection information (the
                // number of participants) before we try to transition the connection to a new
                // stack. we have only called `try_init`, which does *not* increment
                // num_participants if the semantics did not match, so we have to +1 to include
                // ourselves here.
                stack.prefer(num_participants + 1, selector);
                let picked =
                    solo_monomorphize(stack.clone()).wrap_err("Malformed chunnel stack")?;
                debug!(?picked, "transition to new stack");
                round_number = rendezvous_point
                    .transition(
                        addr.clone(),
                        RendezvousEntry {
                            nonce: picked.clone(),
                        },
                    )
                    .await
                    .map_err(Into::into)
                    .wrap_err_with(|| eyre!("Transition failed after trying to apply remote stack failed. The stacks must be incompatible.").wrap_err(orig_apply_error))?;
                let ApplyResult { applied, .. } = stack
                    .apply(picked.clone())
                    .expect("solo_monomorphize means self-application will work");
                (RendezvousEntry { nonce: picked }, applied)
            }
        }
    } else {
        let ApplyResult { applied, .. } = stack
            .apply(picked.clone())
            .expect("solo_monomorphize means self-application will work");
        (RendezvousEntry { nonce: picked }, applied)
    };

    let cn = applied
        .connect_wrap(NeverCn::default())
        .await
        .map_err(Into::into)?;
    let stack_upgrade_handle = StackUpgradeHandle::new(
        handles,
        rendezvous_point,
        addr,
        offers,
        entry,
        num_participants,
        round_number,
    );
    Ok((cn, stack_upgrade_handle))
}

#[derive(Debug)]
pub struct UpgradeHandle {
    trigger_locally: watch::Sender<StackNonce>,
    current_stack_changed_receiver: watch::Receiver<Option<Either<(), ()>>>,
    left_offers: Vec<StackNonce>,
    right_offers: Vec<StackNonce>,
    want_transition: flume::Sender<(Vec<StackNonce>, oneshot::Sender<Result<(), Report>>)>,
    want_transition_listener:
        flume::Receiver<(Vec<StackNonce>, oneshot::Sender<Result<(), Report>>)>,
}

impl UpgradeHandle {
    fn new<T1, T2>(
        inner: &Select<T1, T2>,
        stack_sender: watch::Sender<StackNonce>,
        stack_change_r: watch::Receiver<Option<Either<(), ()>>>,
    ) -> Self
    where
        T1: GetOffers,
        T2: GetOffers,
    {
        let (want_trans_s, want_trans_r) = flume::bounded(1);
        UpgradeHandle {
            trigger_locally: stack_sender,
            current_stack_changed_receiver: stack_change_r,
            left_offers: inner.left.offers().collect(),
            right_offers: inner.right.offers().collect(),
            want_transition: want_trans_s,
            want_transition_listener: want_trans_r,
        }
    }

    pub fn current(&self) -> Option<Either<(), ()>> {
        *self.current_stack_changed_receiver.borrow()
    }

    pub async fn stack_changed(&self) {
        let mut r = self.current_stack_changed_receiver.clone();
        {
            r.borrow_and_update();
        }
        r.changed().await.unwrap();
    }

    pub async fn trigger_left(&self) -> Result<(), Report> {
        match self.current() {
            Some(Either::Left(())) => {
                debug!("left-side stack already set");
                return Ok(());
            }
            Some(_) => (),
            None => {
                bail!("Select is not active")
            }
        }

        self.propose_change(self.left_offers.clone()).await
    }

    pub async fn trigger_right(&self) -> Result<(), Report> {
        match self.current() {
            Some(Either::Right(())) => {
                debug!("right-side stack already set");
                return Ok(());
            }
            Some(_) => (),
            None => {
                bail!("Select is not active")
            }
        }

        self.propose_change(self.right_offers.clone()).await
    }

    // TODO custom error enum which covers all the error cases
    async fn propose_change(&self, stack: Vec<StackNonce>) -> Result<(), Report> {
        let (s, r) = oneshot::channel();
        self.want_transition.send_async((stack, s)).await?;
        r.await.expect("sender won't drop")
    }

    fn is_active(&self) -> bool {
        self.current().is_some()
    }

    fn switch_to_stack(&self, new_stack: StackNonce) {
        self.trigger_locally.send_if_modified(|curr_val| {
            if *curr_val != new_stack {
                *curr_val = new_stack;
                true
            } else {
                false
            }
        });
    }

    fn check_compatibility(&self, new_stack: &StackNonce) -> bool {
        trace!(?new_stack, ?self.left_offers, ?self.right_offers, "checking stack");
        // stack_pair_valid should do a partial check based on the stack subset
        // corresponding to this upgradehandle.
        self.left_offers
            .iter()
            .chain(self.right_offers.iter())
            .any(|option| stack_pair_partial_valid(&new_stack.0, &option.0))
    }
}

fn stack_pair_partial_valid(client: &HashMap<u64, Offer>, server: &HashMap<u64, Offer>) -> bool {
    let mut at_least_one_found = false;
    for (guid, offer) in client.iter() {
        // sidedness
        if let Some(univ) = &offer.sidedness {
            let mut joint = offer.available.clone();
            if let Some(srv_offer) = server.get(guid) {
                at_least_one_found = true;
                joint.extend(srv_offer.available.clone());
            }

            if !crate::negotiate::have_all(univ, &joint) {
                return false;
            }
        } else {
            // two-sided, they must be equal
            if let Some(srv_offer) = server.get(guid) {
                at_least_one_found = true;
                if offer.impl_guid != srv_offer.impl_guid
                    || !crate::negotiate::have_all(&offer.available, &srv_offer.available)
                    || !crate::negotiate::have_all(&srv_offer.available, &offer.available)
                {
                    return false;
                }
            }
        }
    }

    at_least_one_found
}

#[derive(Debug)]
pub struct StackUpgradeHandle<R> {
    pub conn_participants_changed_receiver: watch::Receiver<usize>,
    upgrade_handles: Vec<Arc<UpgradeHandle>>,
    conn_participants_changed_notifier: watch::Sender<usize>,
    negotiator: R,
    addr: String,
    offers: Vec<StackNonce>,
    curr_entry: RendezvousEntry,
    curr_num_participants: usize,
    curr_round: usize,
}

impl<R> StackUpgradeHandle<R>
where
    R: RendezvousBackend + Send + 'static,
    <R as RendezvousBackend>::Error: Into<Report> + Send,
{
    fn new(
        upgrade_handles: Vec<Arc<UpgradeHandle>>,
        negotiator: R,
        addr: String,
        offers: Vec<StackNonce>,
        curr_entry: RendezvousEntry,
        curr_num_participants: usize,
        curr_round: usize,
    ) -> Self {
        let (s, r) = watch::channel(curr_num_participants);
        Self {
            conn_participants_changed_receiver: r,
            upgrade_handles,
            conn_participants_changed_notifier: s,
            negotiator,
            addr,
            offers,
            curr_entry,
            curr_num_participants,
            curr_round,
        }
    }

    /// Spawn a task for this function to listen for updates from the rendezvous negotiator.
    ///
    /// Before doing so, clone `conn_participants_changed_receiver` to get updates, to decide
    /// whether to switch stacks.
    #[instrument(skip(self), fields(addr = self.addr), level = "debug", err)]
    pub async fn monitor_connection_negotiation_state(&mut self) -> Result<(), Report> {
        debug!("starting");
        let uhs = self.upgrade_handles.clone();
        loop {
            // select between two things:
            // 1. all upgrade handle receivers we might send to (contained within returned connections) are dropped. In this case, this task is not necessary anymore and should exit.
            // 2. something happens with the rendezvous negotiation. We should handle that.
            let all_closed =
                futures_util::future::join_all(uhs.iter().map(|uh| uh.trigger_locally.closed()));

            let want_transition = futures_util::future::select_all(
                uhs.iter()
                    .map(|uh| uh.want_transition_listener.recv_async()),
            );

            tokio::select! {
                _ = all_closed => {
                    debug!("upgrade receiver closed, exiting");
                    return Ok(());
                }
                (wt, idx, _remaining_futs) = want_transition => {
                    debug!(?idx, "local requested transition");
                    let (wanted_stack, done) = wt.expect("want_transition sender won't drop");
                    if !uhs[idx].is_active() {
                        done.send(Err(eyre!("Select is not active"))).expect("oneshot receiver won't drop");
                    } else {
                        let res = self.handle_trigger(idx, wanted_stack).await;
                        debug!(worked = ?res.is_ok(), "done attempting transition");
                        done.send(res).expect("oneshot receiver won't drop");
                    }
                }
                notify_res = self.negotiator.notify(self.addr.clone(), self.curr_entry.clone(), self.curr_round) => {
                    self.handle_notify(notify_res.map_err(Into::into)).await?;
                }
            };
        }
    }

    async fn handle_trigger(
        &mut self,
        handle_idx: usize,
        wanted: Vec<StackNonce>,
    ) -> Result<(), Report> {
        let full_stack = find_stack_from_stub(&wanted, &self.offers);
        let new_round = self
            .negotiator
            .transition(
                self.addr.clone(),
                RendezvousEntry {
                    nonce: full_stack.clone(),
                },
            )
            .await
            .map_err(Into::into)?;
        self.curr_round = new_round;
        self.curr_entry = RendezvousEntry {
            nonce: full_stack.clone(),
        };

        debug_assert!(
            self.upgrade_handles[handle_idx].check_compatibility(&full_stack),
            "UpgradeHandle not compatible with own stack"
        );

        let compatible_handles: Vec<_> = self
            .upgrade_handles
            .iter()
            .filter(|uh| uh.check_compatibility(&full_stack))
            .collect();
        for uh in &compatible_handles {
            uh.switch_to_stack(full_stack.clone());
        }

        Ok(())
    }

    async fn handle_notify(
        &mut self,
        notify_res: Result<NegotiateRendezvousResult, Report>,
    ) -> Result<(), Report> {
        debug!(?notify_res, ?self.curr_num_participants, ?self.curr_round, "handling rendezvous change notification");
        // match statement returns a nonce if there was a remote update that we need to apply
        // locally. otherwise it will continue to the next loop iteration.
        match notify_res {
            Ok(NegotiateRendezvousResult::Matched {
                num_participants,
                round_number,
            }) if num_participants == self.curr_num_participants => {
                self.curr_round = round_number;
                Ok(()) // do nothing.
            }
            Ok(NegotiateRendezvousResult::Matched {
                num_participants, ..
            }) => {
                // the semantics are the same, but the number of participants changed.
                self.conn_participants_changed_notifier
                    .send(num_participants)
                    .unwrap();
                Ok(())
            }
            Ok(NegotiateRendezvousResult::NoMatch {
                entry,
                num_participants,
                round_number,
            }) => {
                self.curr_num_participants = num_participants;
                self.curr_round = round_number;
                // Check if `entry` is compatible. If so, ACK with staged_update.
                self.offers
                    .iter()
                    .any(|o| stack_pair_valid(&entry.nonce.0, &o.0));
                // we need to check compatibility before sending, because otherwise if the stack
                // isn't compatible the connection won't be able to apply it and will have to
                // return an error, and we can't catch and handle that error here.
                let compatible_handles: Vec<_> = self
                    .upgrade_handles
                    .iter()
                    .filter(|uh| uh.check_compatibility(&entry.nonce))
                    .collect();
                let new_stack_is_compatible = !compatible_handles.is_empty();
                debug!(?new_stack_is_compatible, num_compatible_handles = ?compatible_handles.len(), "checked proposed stack");
                if new_stack_is_compatible {
                    let new_round = self
                        .negotiator
                        .staged_update(self.addr.clone(), self.curr_round)
                        .await
                        .map_err(Into::into)?;
                    if new_round > self.curr_round + 1 {
                        // don't update curr_round. we will be called again in the next loop
                        // iteration.
                        return Ok(());
                    }

                    self.curr_round = new_round;
                    self.curr_entry = entry.clone();
                    debug!(
                        num_upgrade_handles = self.upgrade_handles.len(),
                        "upgrade committed, channel send"
                    );
                    for uh in &compatible_handles {
                        uh.switch_to_stack(entry.nonce.clone());
                    }

                    Ok(())
                } else {
                    debug!("new stack incompatible, cannot commit");
                    // transition back to original stack
                    let new_round = self
                        .negotiator
                        .transition(self.addr.clone(), self.curr_entry.clone())
                        .await
                        .map_err(Into::into)?;
                    debug!("completed transition back to original stack");
                    self.curr_round = new_round;
                    Ok(())
                }
            }
            Err(e) => Err(e),
        }
    }
}

fn find_stack_from_stub(stub: &Vec<StackNonce>, stacks: &Vec<StackNonce>) -> StackNonce {
    for stack in stacks {
        for s in stub {
            // all stacks in s should be in stack
            if s.0.iter().all(|(guid, Offer { impl_guid, .. })| {
                if let Some(Offer {
                    impl_guid: s_impl_guid,
                    ..
                }) = stack.0.get(guid)
                {
                    s_impl_guid == impl_guid
                } else {
                    false
                }
            }) {
                return stack.clone();
            }
        }
    }

    unreachable!()
}

// "negotiate" against ourselves for a nonce.
// we impl Pick just to pass to monomorphize, for our actual stack we will use the nonce, since
// our Either type explicitly handles apply.
fn solo_monomorphize<T>(stack: T) -> Result<StackNonce, Report>
where
    T: Pick + GetOffers + Debug,
    <T as Pick>::Picked: Debug,
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

#[allow(non_upper_case_globals)]
#[cfg(test)]
mod t {
    use super::{find_stack_from_stub, NegotiateRendezvousResult, RendezvousEntry};
    use crate::{
        mock_serve_bothsides_impl, mock_serve_impl, negotiate_rendezvous, CapabilitySet, CxList,
        Select, UpgradeSelect,
    };
    use crate::{Chunnel, ChunnelConnection, GetOffers, Negotiate};
    use ahash::HashMap;
    use color_eyre::eyre::{bail, ensure, eyre, Context, Report};
    use futures_util::future::{ready, Ready};
    use std::cmp::Ordering;
    use std::time::Duration;
    use std::{
        future::Future,
        pin::Pin,
        sync::{Arc, Mutex},
    };
    use tokio::sync::oneshot;
    use tracing::{debug, info, info_span, trace, warn, Instrument};
    use tracing_error::ErrorLayer;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    mock_serve_bothsides_impl!(ChunnelA);
    mock_serve_bothsides_impl!(ChunnelB);
    mock_serve_bothsides_impl!(ChunnelC);
    mock_serve_bothsides_impl!(ChunnelD);
    mock_serve_bothsides_impl!(ChunnelE);

    #[derive(Clone, Debug, Copy)]
    struct MockBaseChunnel;

    impl<C> Chunnel<C> for MockBaseChunnel {
        type Future = Ready<Result<Self::Connection, Self::Error>>;
        type Connection = Self;
        type Error = std::convert::Infallible;

        fn connect_wrap(&mut self, _: C) -> Self::Future {
            ready(Ok(MockBaseChunnel))
        }
    }

    lazy_static::lazy_static! {
        static ref MockBaseChunnelCapGuid: u64 = rand::random();
        static ref MockBaseChunnelImplGuid: u64 = rand::random();
    }

    #[derive(
        Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
    )]
    struct MockBaseChunnelCap;

    impl CapabilitySet for MockBaseChunnelCap {
        fn universe() -> Option<Vec<Self>> {
            None
        }

        fn guid() -> u64 {
            *MockBaseChunnelCapGuid
        }
    }

    impl Negotiate for MockBaseChunnel {
        type Capability = MockBaseChunnelCap;
        fn guid() -> u64 {
            *MockBaseChunnelImplGuid
        }
        fn capabilities() -> Vec<Self::Capability> {
            vec![MockBaseChunnelCap]
        }
    }

    impl ChunnelConnection for MockBaseChunnel {
        type Data = ();

        fn send<'cn, B>(
            &'cn self,
            _: B,
        ) -> Pin<Box<dyn Future<Output = Result<(), color_eyre::eyre::Report>> + Send + 'cn>>
        where
            B: IntoIterator<Item = Self::Data> + Send + 'cn,
            <B as IntoIterator>::IntoIter: Send,
        {
            Box::pin(ready(Ok(()))) as _
        }

        fn recv<'cn, 'buf>(
            &'cn self,
            _: &'buf mut [Option<Self::Data>],
        ) -> Pin<
            Box<
                dyn Future<
                        Output = Result<&'buf mut [Option<Self::Data>], color_eyre::eyre::Report>,
                    > + Send
                    + 'cn,
            >,
        >
        where
            'buf: 'cn,
        {
            Box::pin(futures_util::future::pending())
        }
    }

    #[test]
    fn stack_subset() {
        let sel = Select::from((ChunnelB, ChunnelC));
        let (sel, uh) = UpgradeSelect::from_select(sel);
        let stack = CxList::from(ChunnelA).wrap(sel);

        let all_offers: Vec<_> = stack.offers().collect();
        let b_offer = &uh.left_offers;
        assert_eq!(b_offer.len(), 1);
        let c_offer = &uh.right_offers;
        assert_eq!(c_offer.len(), 1);

        let bn = find_stack_from_stub(b_offer, &all_offers);
        let cn = find_stack_from_stub(c_offer, &all_offers);

        dbg!(&b_offer, &bn);
        assert_eq!(
            bn.0.get(&ChunnelBCapGuid).unwrap().impl_guid,
            b_offer[0].0.get(&ChunnelBCapGuid).unwrap().impl_guid
        );
        dbg!(&c_offer, &cn);
        assert_eq!(
            cn.0.get(&ChunnelCCapGuid).unwrap().impl_guid,
            c_offer[0].0.get(&ChunnelCCapGuid).unwrap().impl_guid
        );
    }

    #[test]
    fn solo_monomorphize_associative() {
        crate::test::COLOR_EYRE.call_once(|| color_eyre::install().unwrap_or(()));
        let stack1 = Select::from((
            CxList::from(ChunnelB).wrap(ChunnelA),
            CxList::from(ChunnelC).wrap(ChunnelA),
        ));

        let stack2 = CxList::from(Select::from((ChunnelB, ChunnelC))).wrap(ChunnelA);

        super::solo_monomorphize(stack1).expect("stack 1");
        super::solo_monomorphize(stack2).expect("stack 2");
    }

    #[test]
    fn single_swap() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        crate::test::COLOR_EYRE.call_once(|| color_eyre::install().unwrap_or(()));

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            let sel = Select::from((ChunnelB, ChunnelC));
            let (sel, upgrade) = UpgradeSelect::from_select(sel);
            let stack = CxList::from(ChunnelA).wrap(sel).wrap(MockBaseChunnel);

            let addr = "single_swap".to_owned();
            let addr2 = addr.clone();

            let r = MockRendezvous::default();
            let r2 = r.clone();

            let (cn, mut handle) = negotiate_rendezvous(stack, r, addr, |_, _| None)
                .await
                .unwrap();

            let mut p_changed = handle.conn_participants_changed_receiver.clone();
            tokio::spawn(
                async move {
                    handle.monitor_connection_negotiation_state().await.unwrap();
                }
                .instrument(info_span!("client1")),
            );

            let (wait_ready_s, wait_ready_r) = oneshot::channel();

            tokio::spawn(
                async move {
                    let sel = Select::from((ChunnelB, ChunnelC));
                    let (sel, _upgrade) = UpgradeSelect::from_select(sel);
                    let stack = CxList::from(ChunnelA).wrap(sel).wrap(MockBaseChunnel);

                    let (cn, mut handle) = negotiate_rendezvous(stack, r2, addr2, |_, _| None)
                        .await
                        .unwrap();
                    wait_ready_s.send(()).unwrap();
                    use futures_util::future::Either as FEither;
                    match futures_util::future::select(
                        Box::pin(handle.monitor_connection_negotiation_state()),
                        cn.recv(&mut []),
                    )
                    .await
                    {
                        FEither::Left((Err(e), _)) | FEither::Right((Err(e), _)) => {
                            return Err(e);
                        }
                        _ => (),
                    }

                    Ok(())
                }
                .instrument(info_span!("client2")),
            );

            wait_ready_r.await.unwrap();

            p_changed.changed().await.unwrap();
            let new_num_participants = *p_changed.borrow_and_update();
            info!(
                ?new_num_participants,
                "num participants change, transitioning"
            );

            upgrade
                .trigger_right()
                .await
                .expect("trigger right upgrade");
            cn.send(std::iter::empty())
                .instrument(info_span!("connection_send"))
                .await
                .wrap_err("cn send")?;
            assert!(matches!(upgrade.current(), Some(crate::Either::Right(()))));
            info!("finished transition");
            Ok::<_, Report>(())
        })
        .unwrap();
    }

    /// What happens if we try to transition to a stack that's incompatible with the other side? In
    /// this test, we try to use `ChunnelD` which is not present on the other side. The
    /// `trigger_right` call should fail.
    #[test]
    fn swap_nested_failure() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        crate::test::COLOR_EYRE.call_once(|| color_eyre::install().unwrap_or(()));

        #[tracing::instrument(skip(r2, wait_ready_s, wait_trans_s), err)]
        async fn client2(
            r2: MockRendezvous,
            addr2: String,
            wait_ready_s: oneshot::Sender<()>,
            wait_trans_s: oneshot::Sender<()>,
        ) -> Result<(), Report> {
            let (inner_select, upgrade_inner) =
                UpgradeSelect::from_select(Select::from((ChunnelC, ChunnelD)));
            let sel = Select::from((inner_select, ChunnelB));
            let (sel, upgrade) = UpgradeSelect::from_select(sel);
            let stack = CxList::from(ChunnelA).wrap(sel).wrap(MockBaseChunnel);

            let (cn, mut handle) = negotiate_rendezvous(stack, r2, addr2, |_, _| None).await?;
            assert!(matches!(upgrade.current(), Some(crate::Either::Left(()))));
            assert!(matches!(
                upgrade_inner.current(),
                Some(crate::Either::Left(()))
            ));
            let mut p_changed = handle.conn_participants_changed_receiver.clone();
            cn.send(std::iter::empty()).await.unwrap();
            tokio::spawn(
                async move {
                    handle.monitor_connection_negotiation_state().await.unwrap();
                }
                .instrument(info_span!("client2")),
            );

            wait_ready_s.send(()).unwrap();
            p_changed.changed().await.unwrap();
            let new_num_participants = *p_changed.borrow_and_update();
            info!(
                ?new_num_participants,
                "num participants change, transitioning"
            );

            if let Err(e) = upgrade_inner.trigger_right().await {
                info!(err = ?format!("{:#?}", e), "transition failed, as expected");
            } else {
                bail!("transition should have failed");
            }

            wait_trans_s.send(()).unwrap();
            assert!(matches!(
                upgrade_inner.current(),
                Some(crate::Either::Left(()))
            ));
            assert!(matches!(upgrade.current(), Some(crate::Either::Left(()))));
            cn.recv(&mut []).await.unwrap();
            Ok(())
        }

        info!(a_guid = ?ChunnelACap::guid(), b_guid = ?ChunnelBCap::guid(), c_guid = ?ChunnelCCap::guid(), d_guid = ?ChunnelDCap::guid(), base_guid = ?MockBaseChunnelCap::guid(), "chunnel ids");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            let sel = Select::from((ChunnelB, ChunnelC));
            let (sel, upgrade) = UpgradeSelect::from_select(sel);
            let stack = CxList::from(ChunnelA).wrap(sel).wrap(MockBaseChunnel);

            let addr = "swap_nested_failure".to_owned();
            let addr2 = addr.clone();

            let r = MockRendezvous::default();
            let r2 = r.clone();

            let (wait_ready_s, wait_ready_r) = oneshot::channel();
            let (wait_trans_s, wait_trans_r) = oneshot::channel();

            tokio::spawn(client2(r2, addr2, wait_ready_s, wait_trans_s));
            wait_ready_r.await.unwrap();
            let (cn, mut handle) = negotiate_rendezvous(stack, r, addr, |_, _| None)
                .await
                .unwrap();
            cn.send(std::iter::empty())
                .instrument(info_span!("connection_send"))
                .await
                .wrap_err("cn send")?;
            assert!(matches!(upgrade.current(), Some(crate::Either::Right(()))));

            tokio::spawn(
                async move {
                    handle.monitor_connection_negotiation_state().await.unwrap();
                }
                .instrument(info_span!("client1")),
            );

            wait_trans_r.await.unwrap();
            cn.send(std::iter::empty())
                .instrument(info_span!("connection_send"))
                .await
                .wrap_err("cn send")?;
            tokio::time::sleep(Duration::from_millis(500)).await;
            cn.send(std::iter::empty())
                .instrument(info_span!("connection_send"))
                .await
                .wrap_err("cn send")?;
            assert!(matches!(upgrade.current(), Some(crate::Either::Right(()))));
            Ok::<_, Report>(())
        })
        .unwrap();
    }

    /// What happens if we trigger a transition on an UpgradeHandle that is not in use? This should
    /// be a non-fatal error (indicating option not in use), since we don't know what the
    /// connection state might be in the future and whether the requested option would be valid at
    /// that time.
    #[test]
    fn swap_nested_useless() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        crate::test::COLOR_EYRE.call_once(|| color_eyre::install().unwrap_or(()));

        #[tracing::instrument(skip(r2), err)]
        async fn client2(r2: MockRendezvous, addr2: String) -> Result<(), Report> {
            let (left_inner_select, left_upgrade_inner) =
                UpgradeSelect::from_select(Select::from((ChunnelB, ChunnelE)));
            let (right_inner_select, right_upgrade_inner) =
                UpgradeSelect::from_select(Select::from((ChunnelC, ChunnelD)));
            let (sel, upgrade_outer) =
                UpgradeSelect::from_select(Select::from((left_inner_select, right_inner_select)));
            let stack = CxList::from(ChunnelA).wrap(sel).wrap(MockBaseChunnel);

            let (cn, mut handle) = negotiate_rendezvous(stack, r2, addr2, |_, _| None).await?;
            assert!(matches!(
                upgrade_outer.current(),
                Some(crate::Either::Left(()))
            ));
            assert!(matches!(
                left_upgrade_inner.current(),
                Some(crate::Either::Left(()))
            ));
            assert!(right_upgrade_inner.current().is_none());

            let mut monitor_fut = Box::pin(handle.monitor_connection_negotiation_state());
            let mut recv_fut = cn.recv(&mut []);
            let mut outer_changed = Box::pin(upgrade_outer.stack_changed());
            let mut left_changed = Box::pin(left_upgrade_inner.stack_changed());
            loop {
                tokio::select! {
                    Err(e) = &mut monitor_fut => return Err(e),
                    Err(e) = &mut recv_fut => return Err(e),
                    _ = &mut outer_changed => {
                        let stack = upgrade_outer.current();
                        if !matches!(stack, Some(crate::Either::Right(()))) {
                            warn!(?stack, "expected Some(Right)");
                            return Err(eyre!("mismatched stacks"));
                        } else {
                            info!(?stack, "outer got correct stack change notification");
                        }

                        outer_changed = Box::pin(upgrade_outer.stack_changed());
                    }
                    _ = &mut left_changed => {
                        let stack = left_upgrade_inner.current();
                        if stack.is_some() {
                            warn!(?stack, "expected None");
                            return Err(eyre!("mismatched stacks"));
                        } else {
                            info!(?stack, "left_inner got correct stack change notification");
                        }

                        left_changed = Box::pin(left_upgrade_inner.stack_changed());
                    }
                }
            }
        }

        info!(a_guid = ?ChunnelACap::guid(), b_guid = ?ChunnelBCap::guid(), c_guid = ?ChunnelCCap::guid(), d_guid = ?ChunnelDCap::guid(), e_guid = ?ChunnelECap::guid(), base_guid = ?MockBaseChunnelCap::guid(), "chunnel ids");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let r = MockRendezvous::default();
            let r2 = r.clone();
            let addr = "swap_nested_useless".to_owned();
            let addr2 = addr.clone();

            let (left_inner_select, left_upgrade_inner) =
                UpgradeSelect::from_select(Select::from((ChunnelB, ChunnelE)));
            let (right_inner_select, right_upgrade_inner) =
                UpgradeSelect::from_select(Select::from((ChunnelC, ChunnelD)));
            let (sel, upgrade_outer) =
                UpgradeSelect::from_select(Select::from((left_inner_select, right_inner_select)));
            let stack = CxList::from(ChunnelA).wrap(sel).wrap(MockBaseChunnel);

            let (cn, mut handle) = negotiate_rendezvous(stack, r, addr, |_, _| None)
                .await
                .unwrap();
            assert!(matches!(
                upgrade_outer.current(),
                Some(crate::Either::Left(()))
            ));
            assert!(matches!(
                left_upgrade_inner.current(),
                Some(crate::Either::Left(()))
            ));
            assert!(right_upgrade_inner.current().is_none());
            cn.send(std::iter::empty())
                .instrument(info_span!("connection_send"))
                .await
                .wrap_err("cn send")?;
            let mut p_changed = handle.conn_participants_changed_receiver.clone();

            tokio::spawn(
                async move {
                    handle.monitor_connection_negotiation_state().await.unwrap();
                }
                .instrument(info_span!("client1")),
            );

            tokio::spawn(client2(r2, addr2));

            p_changed.changed().await.unwrap();
            let new_num_participants = *p_changed.borrow_and_update();
            info!(
                ?new_num_participants,
                "num participants change, transitioning"
            );

            // 1. make sure right_upgrade_inner is definitely going to be useless
            info!("outer trigger_left");
            upgrade_outer
                .trigger_left()
                .await
                .wrap_err("upgrade_outer trigger left")?;
            cn.send(std::iter::empty())
                .instrument(info_span!("connection_send"))
                .await
                .wrap_err("cn send")?;
            assert!(matches!(
                upgrade_outer.current(),
                Some(crate::Either::Left(()))
            ));
            assert!(matches!(
                left_upgrade_inner.current(),
                Some(crate::Either::Left(()))
            ));
            assert!(right_upgrade_inner.current().is_none());

            // 2. right side select is not in use, so this should fail.
            if let Err(err) = right_upgrade_inner.trigger_right().await {
                info!(
                    ?err,
                    "right_upgrade_inner.trigger_right() failed as expected"
                );
                cn.send(std::iter::empty())
                    .instrument(info_span!("connection_send"))
                    .await
                    .wrap_err("cn send")?;
            } else {
                panic!("right_upgrade_inner.trigger_right() did not error");
            }

            // 3. now we switch so that the left side becomes useless
            info!("outer trigger_right");
            upgrade_outer
                .trigger_right()
                .await
                .wrap_err("upgrade_outer trigger right")?;
            cn.send(std::iter::empty())
                .instrument(info_span!("connection_send"))
                .await
                .wrap_err("cn send")?;
            tokio::time::sleep(Duration::from_millis(25)).await;
            assert!(
                matches!(upgrade_outer.current(), Some(crate::Either::Right(()))),
                "Expected upgrade_outer to be Some(Right), got {:?}",
                upgrade_outer.current()
            );
            assert!(left_upgrade_inner.current().is_none());
            assert!(matches!(
                right_upgrade_inner.current(),
                Some(crate::Either::Left(()))
            ));

            info!("left_upgrade_inner trigger_right should fail");
            // 4. left side select is not in use, so this should fail.
            if let Err(err) = left_upgrade_inner.trigger_right().await {
                info!(
                    ?err,
                    "left_upgrade_inner.trigger_right() failed as expected"
                );
            } else {
                panic!("left_upgrade_inner.trigger_right() did not error");
            }

            cn.send(std::iter::empty())
                .instrument(info_span!("connection_send"))
                .await
                .wrap_err("cn send")?;

            // 5. this time, this should work.
            info!("right_upgrade_inner trigger_right should now work");
            right_upgrade_inner.trigger_right().await?;
            cn.send(std::iter::empty())
                .instrument(info_span!("connection_send"))
                .await
                .wrap_err("cn send")?;
            assert!(matches!(
                upgrade_outer.current(),
                Some(crate::Either::Right(()))
            ));
            assert!(left_upgrade_inner.current().is_none());
            assert!(matches!(
                right_upgrade_inner.current(),
                Some(crate::Either::Right(()))
            ));
            info!("done");
            Ok::<_, Report>(())
        })
        .unwrap();
    }

    struct ConnState {
        num_participants: usize,
        round_number: usize,
        curr_semantics: RendezvousEntry,
        staged: Option<RendezvousEntry>,
        commit_count: usize,
    }

    #[derive(Clone, Default)]
    struct MockRendezvous {
        inner: Arc<Mutex<HashMap<String, ConnState>>>,
    }

    impl super::RendezvousBackend for MockRendezvous {
        type Error = Report;

        fn try_init<'a>(
            &'a mut self,
            addr: String,
            offer: RendezvousEntry,
        ) -> Pin<Box<dyn Future<Output = Result<NegotiateRendezvousResult, Self::Error>> + Send + 'a>>
        {
            Box::pin(ready({
                let mut inner_g = self.inner.lock().unwrap();
                let state = inner_g.entry(addr.clone()).or_insert(ConnState {
                    num_participants: 0,
                    round_number: 0,
                    curr_semantics: offer.clone(),
                    staged: None,
                    commit_count: 0,
                });

                state.num_participants += 1;
                if state.curr_semantics == offer {
                    debug!(?state.num_participants, ?state.round_number, "new compatible participant");
                    Ok(NegotiateRendezvousResult::Matched {
                        num_participants: state.num_participants,
                        round_number: state.round_number,
                    })
                } else {
                    debug!(?state.num_participants, ?state.round_number, "incompatible participant");
                    Ok(NegotiateRendezvousResult::NoMatch {
                        entry: state.curr_semantics.clone(),
                        num_participants: state.num_participants,
                        round_number: state.round_number,
                    })
                }
            })) as _
        }

        fn poll_entry<'a>(
            &'a mut self,
            addr: String,
            curr_entry: RendezvousEntry,
            curr_round: usize,
        ) -> Pin<Box<dyn Future<Output = Result<NegotiateRendezvousResult, Self::Error>> + Send + 'a>>
        {
            Box::pin(ready((|| {
                let mut inner_g = self.inner.lock().unwrap();
                let state = inner_g
                    .get_mut(&addr)
                    .ok_or_else(|| eyre!("Connection not found"))?;

                if state.round_number == curr_round && curr_entry == state.curr_semantics {
                    Ok(NegotiateRendezvousResult::Matched {
                        num_participants: state.num_participants,
                        round_number: state.round_number,
                    })
                } else if state.round_number > curr_round {
                    ensure!(
                        state.staged.is_some(),
                        "Round number advanced ({:?} > {:?}) but no staged entry",
                        state.round_number,
                        curr_round,
                    );

                    debug!(?state.num_participants, ?state.round_number, ?curr_round, "Informing client about new round");
                    Ok(NegotiateRendezvousResult::NoMatch {
                        entry: state.staged.clone().unwrap(),
                        num_participants: state.num_participants,
                        round_number: state.round_number,
                    })
                } else {
                    tracing::error!(?state.round_number, curr_round, "round counter ticked backwards");
                    panic!("round counter ticked backwards");
                }
            })())) as _
        }

        fn set_liveness_expiration(&mut self, _expiration: std::time::Duration) {}

        fn transition<'a>(
            &'a mut self,
            addr: String,
            new_entry: RendezvousEntry,
        ) -> Pin<Box<dyn Future<Output = Result<usize, Self::Error>> + Send + 'a>> {
            Box::pin(async move {
                debug!(?addr, "starting transition commit");
                // phase 1: set
                let round_num = {
                    let mut inner_g = self.inner.lock().unwrap();
                    let state = inner_g
                        .get_mut(&addr)
                        .ok_or_else(|| eyre!("Connection not found"))?;

                    state.staged = Some(new_entry);
                    state.round_number += 1;
                    state.commit_count = 1;
                    debug!(?state.round_number, "waiting for commit phase 2");
                    state.round_number
                };

                // phase 2: wait for commit_count == num_participants
                let cnt = loop {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    let mut inner_g = self.inner.lock().unwrap();
                    let state = inner_g
                        .get_mut(&addr)
                        .ok_or_else(|| eyre!("Connection not found"))?;

                    debug!(?state.round_number, ?state.commit_count, ?state.num_participants, "polling commit phase 2");
                    if state.commit_count == state.num_participants {
                        state.round_number += 1;
                        state.commit_count = 0;
                        state.curr_semantics = state.staged.take().unwrap();
                        debug!(?state.round_number, ?state.num_participants, "transition committed");
                        break state.round_number;
                    }

                    if state.round_number > round_num {
                        warn!(?state.commit_count, ?round_num, ?state.round_number, "commit failed");
                        return Err(eyre!("commit failed"));
                    }
                };

                Ok(cnt)
            })
        }

        fn staged_update<'a>(
            &'a mut self,
            addr: String,
            round_ctr: usize,
        ) -> Pin<Box<dyn Future<Output = Result<usize, Self::Error>> + Send + 'a>> {
            Box::pin(async move {
                {
                    trace!("staged_update waiting for lock");
                    let mut inner_g = self.inner.lock().unwrap();
                    trace!("staged_update locked");
                    let state = inner_g
                        .get_mut(&addr)
                        .ok_or_else(|| eyre!("Connection not found"))?;

                    match state.round_number.cmp(&round_ctr) {
                        Ordering::Greater | Ordering::Less => panic!("round counter mismatched"),
                        Ordering::Equal => {
                            assert!(state.staged.is_some());
                            state.commit_count += 1;
                        }
                    }
                } // drop inner_g

                debug!("waiting for commit");
                let round_num = loop {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    trace!("staged_update waiting for lock");
                    let mut inner_g = self.inner.lock().unwrap();
                    trace!("staged_update locked");
                    let state = inner_g
                        .get_mut(&addr)
                        .ok_or_else(|| eyre!("Connection not found"))?;

                    if state.round_number > round_ctr {
                        break state.round_number;
                    }

                    trace!(?state.round_number, orig_round = ?round_ctr, "waiting");
                };

                debug!(?round_num, "commit done");
                Ok(round_num)
            })
        }
    }
}
