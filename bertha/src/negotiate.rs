//! Chunnel wrapper types to negotiate between multiple implementations.

use crate::{
    either::MakeEither, Chunnel, ChunnelConnection, CxList, CxNil, DataEither, Either, FlipEither,
};
use color_eyre::eyre::{eyre, Report, WrapErr};
use futures_util::{
    future::{select, FutureExt},
    stream::{Stream, TryStreamExt},
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::future::Future;
use std::iter::{once, Chain, Once as IterOnce};
use std::pin::Pin;
use std::sync::{atomic::AtomicBool, Arc, Mutex};
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tracing::{debug, debug_span, instrument, trace, warn};
use tracing_futures::Instrument;

/// A type that can list out the `universe()` of possible values it can have.
pub trait CapabilitySet: core::fmt::Debug + PartialEq + Sized {
    fn guid() -> u64;

    /// All possible values this type can have.
    ///
    /// A `None` value indicates that the type is both-sided, so the client and server capabilities
    /// should match.
    // TODO make return an unordered collection
    fn universe() -> Option<Vec<Self>>;
}

impl CapabilitySet for () {
    fn universe() -> Option<Vec<Self>> {
        Some(vec![()])
    }

    fn guid() -> u64 {
        0
    }
}

/// Define an enum that implements the `CapabilitySet` trait.
///
/// Invoke with enum name (with optional `pub`) followed by variant names.
///
/// # Example
/// ```rust
/// # use bertha::enumerate_enum;
/// enumerate_enum!(pub Foo, 0xe1e3ca44a5ece5bb, A, B, C);
/// enumerate_enum!(Bar, 0x829233ff7c2ab87a, A, B, C);
/// enumerate_enum!(pub Baz, 0xe2cab072f664d381, A);
/// fn main() {
///     let f = Foo::B;
///     let b = Bar::C;
///     let z = Baz::A;
///     println!("{:?}, {:?}, {:?}", f, b, z);
/// }
/// ```
#[macro_export]
macro_rules! enumerate_enum {
    ($v:vis $name:ident, $guid:expr, $($variant:ident),+) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
        $v enum $name {
            $(
                $variant
            ),+
        }

        impl $crate::negotiate::CapabilitySet for $name {
            fn universe() -> Option<Vec<Self>> {
                Some(vec![
                    $($name::$variant),+
                ])
            }

            fn guid() -> u64 {
                $guid
            }
        }
    };
}

/// Expresses the ability to negotiate chunnel implementations over a set of capabilities enumerated
/// by the `Capability` type.
///
/// We don't care about intra-stack compatibility here, since the type system will deal with that
/// for us. Instead, focus only on what server and client need to agree on: the set of semantics.
pub trait Negotiate {
    type Capability: CapabilitySet;

    fn guid() -> u64;

    fn capabilities() -> Vec<Self::Capability> {
        vec![]
    }

    /// Callback after this implementation is picked, but *before* `serve`/`connect_wrap`.
    ///
    /// Implementors can pass the nonce to other instances of `negotiate_server` to pick the same
    /// stack.
    fn picked<'s>(&mut self, _nonce: &'s [u8]) -> Pin<Box<dyn Future<Output = ()> + Send + 's>> {
        Box::pin(futures_util::future::ready(()))
    }
}

impl Negotiate for CxNil {
    type Capability = ();

    fn guid() -> u64 {
        0xa1f4d15a09462192
    }
}

impl<T1, T2, C> Negotiate for Either<T1, T2>
where
    T1: Negotiate<Capability = C>,
    T2: Negotiate<Capability = C>,
    C: CapabilitySet,
{
    type Capability = C;

    fn guid() -> u64 {
        T1::guid() ^ T2::guid()
    }

    fn picked<'s>(&mut self, nonce: &'s [u8]) -> Pin<Box<dyn Future<Output = ()> + Send + 's>> {
        match self {
            Either::Left(a) => a.picked(nonce),
            Either::Right(a) => a.picked(nonce),
        }
    }
}

pub trait NegotiatePicked {
    fn call_negotiate_picked<'s>(
        &mut self,
        nonce: &'s [u8],
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 's>>;
}

impl<N> NegotiatePicked for N
where
    N: Negotiate,
{
    fn call_negotiate_picked<'s>(
        &mut self,
        nonce: &'s [u8],
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 's>> {
        self.picked(nonce)
    }
}

impl<H, T> NegotiatePicked for CxList<H, T>
where
    H: NegotiatePicked,
    T: NegotiatePicked,
{
    fn call_negotiate_picked<'s>(
        &mut self,
        nonce: &'s [u8],
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 's>> {
        let head_fut = self.head.call_negotiate_picked(nonce);
        let tail_fut = self.tail.call_negotiate_picked(nonce);
        Box::pin(async move {
            head_fut.await;
            tail_fut.await;
        })
    }
}

impl<L, R> NegotiatePicked for DataEither<L, R>
where
    L: NegotiatePicked,
    R: NegotiatePicked,
{
    fn call_negotiate_picked<'s>(
        &mut self,
        nonce: &'s [u8],
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 's>> {
        let f = match self {
            DataEither::Left(l) => l.call_negotiate_picked(nonce),
            DataEither::Right(r) => r.call_negotiate_picked(nonce),
        };

        Box::pin(f)
    }
}

/// Negotiation type to choose between `T1` and `T2`.
#[derive(Clone, Debug)]
pub struct Select<T1, T2, Inner = Either<T1, T2>> {
    pub left: T1,
    pub right: T2,
    prefer: Either<(), ()>,
    _inner: std::marker::PhantomData<Inner>,
}

impl<T1, T2> From<(T1, T2)> for Select<T1, T2> {
    fn from(f: (T1, T2)) -> Self {
        Self {
            left: f.0,
            right: f.1,
            prefer: Either::Left(()),
            _inner: Default::default(),
        }
    }
}

impl<T1, T2, I> Select<T1, T2, I> {
    /// Change the inner type.
    pub fn inner_type<I1>(self) -> Select<T1, T2, I1> {
        Select {
            left: self.left,
            right: self.right,
            prefer: self.prefer,
            _inner: Default::default(),
        }
    }

    /// Change the left/right preference.
    fn prefer(self, prefer: Either<(), ()>) -> Select<T1, T2, I> {
        Select { prefer, ..self }
    }

    /// Prefer the left side.
    pub fn prefer_left(self) -> Select<T1, T2, I> {
        self.prefer(Either::Left(()))
    }

    /// Prefer the right side.
    pub fn prefer_right(self) -> Select<T1, T2, I> {
        self.prefer(Either::Right(()))
    }
}

/// available is a Vec<T::Capability> where T is a negotiation type.
/// capability_guid identifies T::Capability.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Offer {
    pub capability_guid: u64,
    pub impl_guid: u64,
    /// None => two-sided, Some(Vec<offers>) => one-sided with universe
    pub sidedness: Option<Vec<Vec<u8>>>,
    /// Each serialized T::Capability is the inner Vec<u8>, and the list of them is the list of
    /// capabilities.
    pub available: Vec<Vec<u8>>,
}

fn get_offer<T>() -> Option<Offer>
where
    T: Negotiate,
    <T as Negotiate>::Capability: Serialize + DeserializeOwned,
{
    if T::Capability::guid() != 0 {
        Some(Offer {
            capability_guid: T::Capability::guid(),
            impl_guid: T::guid(),
            sidedness: T::Capability::universe().map(|univ| {
                univ.iter()
                    .map(|c| bincode::serialize(c).unwrap())
                    .collect()
            }),
            available: T::capabilities()
                .iter()
                .map(|c| bincode::serialize(c).unwrap())
                .collect(),
        })
    } else {
        None
    }
}

/// Get an iterator of possible stacks this stack could produce.
///
/// Each stack is represented by a map collecting unique negotiation-capabilities it contains in
/// guid form (u64) -> a list of Offer, which contain in serialized form the capabilities
/// available.
///
/// The number of enumerated stacks will be 2^(number of selects).
///
/// ```rust
/// # use bertha::{reliable::ReliabilityProjChunnel, tagger::OrderedChunnelProj, CxList,
/// negotiate::GetOffers};
/// let ls = CxList::from(OrderedChunnelProj::default()).wrap(ReliabilityProjChunnel::default());
/// let offers: Vec<_> = ls.offers().collect();
/// println!("{:?}", offers);
/// ```
pub trait GetOffers {
    type Iter: Iterator<Item = HashMap<u64, Offer>>;
    fn offers(&self) -> Self::Iter;
}

impl<N, C> GetOffers for N
where
    N: Negotiate<Capability = C>,
    C: CapabilitySet + Serialize + DeserializeOwned,
{
    type Iter = IterOnce<HashMap<u64, Offer>>;

    fn offers(&self) -> Self::Iter {
        let mut h = HashMap::default();
        if let Some(o) = get_offer::<N>() {
            h.insert(C::guid(), o);
        }

        once(h)
    }
}

impl<H, T> GetOffers for CxList<H, T>
where
    H: GetOffers,
    <H as GetOffers>::Iter: Clone,
    T: GetOffers,
{
    type Iter = std::vec::IntoIter<HashMap<u64, Offer>>;

    fn offers(&self) -> Self::Iter {
        let tail_iter = self.tail.offers();
        let head_iter = self.head.offers();

        fn merge(l: HashMap<u64, Offer>, mut r: HashMap<u64, Offer>) -> HashMap<u64, Offer> {
            for (guid, o) in l {
                if let Some(ent) = r.get_mut(&guid) {
                    ent.impl_guid ^= o.impl_guid;
                    ent.available.extend(o.available);
                } else {
                    r.insert(guid, o);
                }
            }

            r
        }

        let mut opts = vec![];
        for tail_opt in tail_iter {
            for head_opt in head_iter.clone() {
                opts.push(merge(head_opt, tail_opt.clone()));
            }
        }

        opts.into_iter()
    }
}

impl<T1, T2, I> GetOffers for Select<T1, T2, I>
where
    T1: GetOffers,
    T2: GetOffers,
{
    type Iter = Chain<T1::Iter, T2::Iter>;

    fn offers(&self) -> Self::Iter {
        let left = self.left.offers();
        let right = self.right.offers();
        left.chain(right)
    }
}

fn have_all(univ: &[Vec<u8>], joint: &[Vec<u8>]) -> bool {
    univ.iter().all(|x| joint.contains(x))
}

fn stack_pair_valid(client: &HashMap<u64, Offer>, server: &HashMap<u64, Offer>) -> bool {
    for (guid, offer) in client.iter() {
        // sidedness
        if let Some(univ) = &offer.sidedness {
            let mut joint = offer.available.clone();
            if let Some(srv_offer) = server.get(&guid) {
                joint.extend(srv_offer.available.clone());
            }

            if !have_all(univ, &joint) {
                return false;
            }
        } else {
            // two-sided, they must be equal
            if let Some(srv_offer) = server.get(&guid) {
                if offer.impl_guid != srv_offer.impl_guid
                    || !have_all(&offer.available, &srv_offer.available)
                    || !have_all(&srv_offer.available, &offer.available)
                {
                    return false;
                }
            } else {
                return false;
            }
        }
    }

    true
}

// returns (client, server) stack pairs to use.
fn compare_offers(
    client: Vec<HashMap<u64, Offer>>,
    server: Vec<HashMap<u64, Offer>>,
) -> Vec<(HashMap<u64, Offer>, HashMap<u64, Offer>)> {
    let mut valid_pairs = vec![];
    for client_stack_candidate in client.iter() {
        for server_stack_candidate in server.iter() {
            if stack_pair_valid(client_stack_candidate, server_stack_candidate) {
                valid_pairs.push((
                    client_stack_candidate.clone(),
                    server_stack_candidate.clone(),
                ));
            }
        }
    }

    valid_pairs
}

/// Result of a `Pick`.
///
/// `filtered_pairs` is a set of pairs that is consistent with `P`.
/// `touched_cap_guids` enumerates the capability guids that this operation touched.
#[derive(Debug, Clone)]
pub struct PickResult<P> {
    stack: P,
    filtered_pairs: Vec<(HashMap<u64, Offer>, HashMap<u64, Offer>)>,
    touched_cap_guids: HashSet<u64>,
}

/// Trait to monomorphize a CxList with possible `Select`s into something that impls Chunnel
pub trait Pick {
    type Picked;

    /// input: set of valid (client, server) offer pairs
    ///
    /// Returns (new_stack, mutated_pairs, handled_cap_guids).
    fn pick(
        self,
        offer_pairs: Vec<(HashMap<u64, Offer>, HashMap<u64, Offer>)>,
    ) -> Result<PickResult<Self::Picked>, Report>;
}

impl<N, C> Pick for N
where
    N: Negotiate<Capability = C>,
    C: CapabilitySet + Serialize + DeserializeOwned + Clone,
{
    type Picked = Self;

    fn pick(
        self,
        offer_pairs: Vec<(HashMap<u64, Offer>, HashMap<u64, Offer>)>,
    ) -> Result<PickResult<Self::Picked>, Report> {
        if C::guid() == 0 {
            return Ok(PickResult {
                stack: self,
                filtered_pairs: offer_pairs,
                touched_cap_guids: Default::default(),
            });
        }

        let filtered_pairs = offer_pairs
            .into_iter()
            .filter_map(|(client, mut server)| {
                let cap_guid = C::guid();
                if let Some(offer) = server.get_mut(&cap_guid) {
                    // one-sided checked in `check_touched`
                    if offer.sidedness.is_none() {
                        // check client matches:
                        if let Some(cl_of) = client.get(&cap_guid) {
                            // client and server must have the same set.
                            if !have_all(&cl_of.available, &offer.available)
                                || !have_all(&offer.available, &cl_of.available)
                            {
                                return None;
                            }

                            offer.impl_guid ^= N::guid();
                        } else {
                            // if this cap_guid is not in the client list, it's not valid because
                            // they must match
                            return None;
                        }
                    }
                } else {
                    // if client has it and we don't, no match.
                    if client.contains_key(&cap_guid) {
                        return None;
                    }
                }

                Some((client, server))
            })
            .collect();

        Ok(PickResult {
            stack: self,
            filtered_pairs,
            touched_cap_guids: [C::guid()].iter().map(|x| *x).collect(),
        })
    }
}

impl<H, T> Pick for CxList<H, T>
where
    H: Pick,
    T: Pick,
{
    type Picked = CxList<H::Picked, T::Picked>;

    fn pick(
        self,
        offer_pairs: Vec<(HashMap<u64, Offer>, HashMap<u64, Offer>)>,
    ) -> Result<PickResult<Self::Picked>, Report> {
        let PickResult {
            stack: head_pick,
            filtered_pairs,
            touched_cap_guids: head_caps,
        } = self.head.pick(offer_pairs)?;
        let PickResult {
            stack: tail_pick,
            filtered_pairs,
            touched_cap_guids: tail_caps,
        } = self.tail.pick(filtered_pairs)?;

        Ok(PickResult {
            stack: CxList {
                head: head_pick,
                tail: tail_pick,
            },
            filtered_pairs,
            touched_cap_guids: head_caps.union(&tail_caps).map(|x| *x).collect(),
        })
    }
}

fn check_touched<T: Pick>(
    t: T,
    pairs: Vec<(HashMap<u64, Offer>, HashMap<u64, Offer>)>,
) -> Result<PickResult<T::Picked>, Report>
where
    T::Picked: Debug,
{
    let pr = t.pick(pairs)?;
    trace!(?pr, "try pick");
    let touched = &pr.touched_cap_guids;
    let pairs: Vec<_> = pr
        .filtered_pairs
        .into_iter()
        .filter(|(client, server)| {
            touched.iter().all(|t| {
                let of = server.get(t).unwrap();
                trace!(offer = ?&of, cap = ?t, "checking");
                match of {
                    Offer { impl_guid, .. } if *impl_guid == 0 => true,
                    Offer {
                        sidedness: Some(univ),
                        available,
                        ..
                    } => {
                        if let Some(cl_of) = client.get(t) {
                            let h: HashSet<&[u8]> = cl_of
                                .available
                                .iter()
                                .map(Vec::as_slice)
                                .chain(available.iter().map(Vec::as_slice))
                                .collect();
                            h.len() == univ.len()
                        } else {
                            available.len() == univ.len()
                        }
                    }
                    _ => false,
                }
            })
        })
        .collect();

    if pairs.is_empty() {
        Err(eyre!("No remaining valid (client, server) offer pairs"))
    } else {
        Ok(PickResult {
            filtered_pairs: pairs,
            ..pr
        })
    }
}

impl<T1, T2, Inner, E> Pick for Select<T1, T2, Inner>
where
    T1: Pick,
    T2: Pick,
    <T1 as Pick>::Picked: Debug,
    <T2 as Pick>::Picked: Debug,
    Inner: MakeEither<T1::Picked, T2::Picked, Either = E> + MakeEither<T2::Picked, T1::Picked>,
    <Inner as MakeEither<T2::Picked, T1::Picked>>::Either: FlipEither<Flipped = E>,
{
    type Picked = E;

    fn pick(
        self,
        offer_pairs: Vec<(HashMap<u64, Offer>, HashMap<u64, Offer>)>,
    ) -> Result<PickResult<Self::Picked>, Report> {
        fn pick_in_preference_order<T1, T2, Inner>(
            first_pick: T1,
            second_pick: T2,
            offer_pairs: Vec<(HashMap<u64, Offer>, HashMap<u64, Offer>)>,
        ) -> Result<PickResult<Inner::Either>, Report>
        where
            T1: Pick,
            T2: Pick,
            <T1 as Pick>::Picked: Debug,
            <T2 as Pick>::Picked: Debug,
            Inner: MakeEither<T1::Picked, T2::Picked>,
        {
            let first_err = match check_touched(first_pick, offer_pairs.clone()) {
                Ok(PickResult {
                    stack,
                    filtered_pairs,
                    touched_cap_guids,
                }) if !filtered_pairs.is_empty() => {
                    return Ok(PickResult {
                        stack: Inner::left(stack),
                        filtered_pairs,
                        touched_cap_guids,
                    })
                }
                Ok(_) => eyre!("first choice pick left no options"),
                Err(e) => e.wrap_err(eyre!("first choice pick erred")),
            };

            match check_touched(second_pick, offer_pairs.clone()) {
                Ok(PickResult {
                    stack,
                    filtered_pairs,
                    touched_cap_guids,
                }) if filtered_pairs.is_empty() => Ok(PickResult {
                    stack: Inner::right(stack),
                    filtered_pairs,
                    touched_cap_guids,
                }),
                Ok(_) => Err(eyre!("both select sides not satisfied").wrap_err(first_err)),
                Err(e) => Err(e
                    .wrap_err(eyre!("second choice pick erred"))
                    .wrap_err(first_err)),
            }
        }

        match self.prefer {
            Either::Left(_) => {
                pick_in_preference_order::<T1, T2, Inner>(self.left, self.right, offer_pairs)
            }
            Either::Right(_) => {
                let PickResult {
                    stack,
                    filtered_pairs,
                    touched_cap_guids,
                } = pick_in_preference_order::<T2, T1, Inner>(self.right, self.left, offer_pairs)?;
                Ok(PickResult {
                    stack: stack.flip(),
                    filtered_pairs,
                    touched_cap_guids,
                })
            }
        }
    }
}

pub struct InjectWithChannel<C, D>(C, Arc<AtomicBool>, Arc<TokioMutex<oneshot::Receiver<D>>>);

impl<C, D> InjectWithChannel<C, D> {
    pub fn make(inner: C) -> (Self, oneshot::Sender<D>) {
        let (s, r) = oneshot::channel();
        (
            Self(
                inner,
                Arc::new(AtomicBool::new(false)),
                Arc::new(TokioMutex::new(r)),
            ),
            s,
        )
    }
}

impl<C, D> ChunnelConnection for InjectWithChannel<C, D>
where
    C: ChunnelConnection<Data = D>,
    D: Send + 'static,
{
    type Data = D;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        self.0.send(data)
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let f = self.0.recv();
        if self.1.load(std::sync::atomic::Ordering::SeqCst) {
            f
        } else {
            let done = Arc::clone(&self.1);
            let r = Arc::clone(&self.2);
            let sel = select(
                f,
                Box::pin(async move {
                    use std::ops::DerefMut;
                    match r.lock().await.deref_mut().await {
                        Ok(d) => {
                            trace!("recirculate first packet on prenegotiated connection");
                            done.store(true, std::sync::atomic::Ordering::SeqCst);
                            Ok(d)
                        }
                        Err(_) => futures_util::future::pending().await,
                    }
                }),
            )
            .map(|e| e.factor_first().0);
            Box::pin(sel)
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum NegotiateMsg {
    ClientOffer(Vec<HashMap<u64, Offer>>),
    ServerReply(Result<Vec<HashMap<u64, Offer>>, String>),
    ServerNonce {
        addr: Vec<u8>,
        picked: HashMap<u64, Offer>,
    },
    ServerNonceAck,
}

type ClientInput<C, A> = InjectWithChannel<C, (A, Vec<u8>)>;

use crate::and_then_concurrent::TryStreamExtExt;

/// Return a stream of connections with `stack`'s semantics, listening on `raw_cn_st`.
pub fn negotiate_server<Srv, Sc, Se, C, A>(
    stack: Srv,
    raw_cn_st: Sc,
) -> impl Future<
    Output = Result<
        impl Stream<
                Item = Result<
                    Either<
                        <<Srv as Pick>::Picked as Chunnel<ClientInput<C, A>>>::Connection,
                        <<Srv as Apply>::Applied as Chunnel<ClientInput<C, A>>>::Connection,
                    >,
                    Report,
                >,
            > + Send
            + 'static,
        Report,
    >,
> + Send
       + 'static
where
    Sc: Stream<Item = Result<C, Se>> + Send + 'static,
    Se: Into<Report> + Send + Sync + 'static,
    C: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync + 'static,
    Srv: Pick + Apply + GetOffers + Clone + Debug + Send + 'static,
    // main-line branch: Pick on incoming negotiation handshake.
    <Srv as Pick>::Picked:
        NegotiatePicked + Chunnel<ClientInput<C, A>> + Clone + Debug + Send + 'static,
    <<Srv as Pick>::Picked as Chunnel<ClientInput<C, A>>>::Connection: Send + Sync + 'static,
    <<Srv as Pick>::Picked as Chunnel<ClientInput<C, A>>>::Error:
        Into<Report> + Send + Sync + 'static,
    // nonce branch: Apply stack from nonce on indicated connections.
    <Srv as Apply>::Applied: Chunnel<ClientInput<C, A>> + Clone + Debug + Send + 'static,
    <<Srv as Apply>::Applied as Chunnel<ClientInput<C, A>>>::Connection: Send + Sync + 'static,
    <<Srv as Apply>::Applied as Chunnel<ClientInput<C, A>>>::Error:
        Into<Report> + Send + Sync + 'static,
    A: Serialize + DeserializeOwned + Eq + std::hash::Hash + Debug + Send + Sync + 'static,
{
    async move {
        // 1. serve (A, Vec<u8>) connections.
        let st = raw_cn_st.map_err(Into::into); // stream of incoming Vec<u8> conns.
        let pending_negotiated_connections: Arc<Mutex<HashMap<A, HashMap<u64, Offer>>>> =
            Default::default();
        Ok(st
            .map_err(Into::into)
            // and_then_concurrent will concurrently poll the stream, and any futures returned by
            // this closure. The futures returned by the closure will form the basis for the output
            // stream. Note that the `process_nonces_connection` case means that some of the
            // futures will never resolve, so and_then_concurrent cannot poll them in order. So,
            // the output stream may be reordered compared to the input stream.
            .and_then_concurrent(move |cn| {
                let stack = stack.clone();
                let pending_negotiated_connections = Arc::clone(&pending_negotiated_connections);
                negotiate_server_connection(cn, stack, pending_negotiated_connections)
            })
            .try_filter_map(|v| futures_util::future::ready(Ok(v))))
    }
}

async fn negotiate_server_connection<C, A, Srv>(
    cn: C,
    stack: Srv,
    pending_negotiated_connections: Arc<Mutex<HashMap<A, HashMap<u64, Offer>>>>,
) -> Result<
    Option<
        Either<
            <<Srv as Pick>::Picked as Chunnel<ClientInput<C, A>>>::Connection,
            <<Srv as Apply>::Applied as Chunnel<ClientInput<C, A>>>::Connection,
        >,
    >,
    Report,
>
where
    C: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync + 'static,
    Srv: Pick + Apply + GetOffers + Clone + Debug + Send + 'static,
    // main-line branch: Pick on incoming negotiation handshake.
    <Srv as Pick>::Picked:
        NegotiatePicked + Chunnel<ClientInput<C, A>> + Clone + Debug + Send + 'static,
    <<Srv as Pick>::Picked as Chunnel<ClientInput<C, A>>>::Connection: Send + Sync + 'static,
    <<Srv as Pick>::Picked as Chunnel<ClientInput<C, A>>>::Error:
        Into<Report> + Send + Sync + 'static,
    // nonce branch: Apply stack from nonce on indicated connections.
    <Srv as Apply>::Applied: Chunnel<ClientInput<C, A>> + Clone + Debug + Send + 'static,
    <<Srv as Apply>::Applied as Chunnel<ClientInput<C, A>>>::Connection: Send + Sync + 'static,
    <<Srv as Apply>::Applied as Chunnel<ClientInput<C, A>>>::Error:
        Into<Report> + Send + Sync + 'static,
    A: Serialize + DeserializeOwned + Eq + std::hash::Hash + Debug + Send + Sync + 'static,
{
    debug!("new connection");
    let (cn, s) = InjectWithChannel::make(cn);
    loop {
        // 2. on new connection, read off Vec<Vec<Offer>> from client
        let (a, buf): (_, Vec<u8>) = cn.recv().await?;
        trace!("got offer pkt");

        // if `a` is in pending_negotiated_connections, this is a post-negotiation message and we
        // should return the applied connection.
        let opt_picked = {
            let guard = pending_negotiated_connections.lock().unwrap();
            guard.get(&a).map(Clone::clone)
        };

        if let Some(picked) = opt_picked {
            let (mut stack, _, _) = stack
                .apply(picked)
                .wrap_err("failed to apply semantics to client connection")?;
            let new_cn = stack.connect_wrap(cn).await.map_err(Into::into)?;

            debug!(addr = ?&a, stack = ?&stack, "returning pre-negotiated connection");
            s.send((a, buf)).map_err(|_| eyre!("Send failed"))?;
            return Ok(Some(Either::Right(new_cn)));
        }

        debug!(client_addr = ?&a, "address not already negotiated, doing negotiation");

        // else, do negotiation
        let negotiate_msg: NegotiateMsg =
            bincode::deserialize(&buf).wrap_err("offer deserialize failed")?;

        use NegotiateMsg::*;
        match negotiate_msg {
            ServerNonce { addr, picked } => {
                let addr: A = bincode::deserialize(&addr).wrap_err("mismatched addr types")?;
                trace!(client_addr = ?&addr, nonce = ?&picked, "got nonce");
                pending_negotiated_connections
                    .lock()
                    .unwrap()
                    .insert(addr, picked.clone());

                // send ack
                let ack = bincode::serialize(&NegotiateMsg::ServerNonceAck).unwrap();
                cn.send((a, ack)).await?;
                debug!("sent nonce ack");

                // need to loop on this connection, processing nonces
                if let Err(e) =
                    process_nonces_connection(cn, Arc::clone(&pending_negotiated_connections))
                        .await
                        .wrap_err("process_nonces_connection")
                {
                    debug!(err = %format!("{:#}", e), "process_nonces_connection exited");
                    return Ok(None);
                }

                unreachable!();
            }
            ClientOffer(client_offers) => {
                let s = stack.clone();
                let (new_stack, client_resp) = match monomorphize(s, client_offers, &a).await {
                    Ok((new_stack, picked_offers)) => (
                        Some(new_stack),
                        NegotiateMsg::ServerReply(Ok(picked_offers)),
                    ),
                    Err(e) => {
                        debug!(err = %format!("{:#}", &e), "negotiation handshake failed");
                        (None, NegotiateMsg::ServerReply(Err(e.to_string())))
                    }
                };

                // 4. Respond to client with offer choice
                let buf = bincode::serialize(&client_resp)?;
                // response has to fit in a packet
                assert!(buf.len() < 1500);
                cn.send((a, buf)).await?;

                if let Some(mut new_stack) = new_stack {
                    debug!(stack = ?&new_stack, "handshake done, picked stack");

                    // 5. new_stack.serve(vec_u8_stream)
                    let new_cn = new_stack.connect_wrap(cn).await.map_err(Into::into)?;

                    debug!("returning connection");
                    return Ok(Some(Either::Left(new_cn)));
                } else {
                    continue;
                }
            }
            _ => unreachable!(),
        }
    }
}

#[instrument(level = "debug", skip(cn, pending_negotiated_connections))]
async fn process_nonces_connection<A>(
    cn: impl ChunnelConnection<Data = (A, Vec<u8>)>,
    pending_negotiated_connections: Arc<Mutex<HashMap<A, HashMap<u64, Offer>>>>,
) -> Result<(), Report>
where
    A: Serialize + DeserializeOwned + Eq + std::hash::Hash + Debug + Send + Sync + 'static,
{
    loop {
        trace!("call recv()");
        let (a, buf): (_, Vec<u8>) = cn.recv().await.wrap_err("conn recv")?;
        let negotiate_msg: NegotiateMsg =
            bincode::deserialize(&buf).wrap_err("offer deserialize failed")?;

        use NegotiateMsg::*;
        match negotiate_msg {
            ServerNonce { addr, picked } => {
                let addr: A = bincode::deserialize(&addr).wrap_err("mismatched addr types")?;
                trace!(client_addr = ?&addr, nonce = ?&picked, "got nonce");
                pending_negotiated_connections
                    .lock()
                    .unwrap()
                    .insert(addr, picked.clone());

                // send ack
                let ack = bincode::serialize(&NegotiateMsg::ServerNonceAck).unwrap();
                cn.send((a, ack)).await?;
                debug!("sent nonce ack");
            }
            x => warn!(msg = ?x, "expected server nonce, got different message"),
        }
    }
}

async fn monomorphize<Srv, A>(
    stack: Srv,
    client_offers: Vec<HashMap<u64, Offer>>,
    from_addr: &A,
) -> Result<(<Srv as Pick>::Picked, Vec<HashMap<u64, Offer>>), Report>
where
    Srv: Pick + GetOffers + Clone + Debug + Send + 'static,
    // main-line branch: Pick on incoming negotiation handshake.
    <Srv as Pick>::Picked: NegotiatePicked + Clone + Debug + Send + 'static,
    A: Serialize + DeserializeOwned + Debug + Send + Sync + 'static,
{
    // enumerate possible offer groups from `stack`.
    let possibilities: Vec<_> = stack.offers().collect();
    let saved_possibilities = possibilities.clone();

    let valid_pairs = compare_offers(client_offers, possibilities);

    // 3. monomorphize: transform the CxList<impl Serve/Select<impl Serve,
    //    impl Serve>> into a CxList<impl Serve>
    let PickResult {
        stack: mut new_stack,
        filtered_pairs,
        ..
    } = check_touched(stack, valid_pairs).wrap_err(eyre!("error monomorphizing stack"))?;
    assert!(!filtered_pairs.is_empty());

    let (client_choices, mut server_choices): (Vec<_>, Vec<_>) = filtered_pairs.into_iter().unzip();
    let server_choice = server_choices.pop().unwrap();

    fn check_possibilities(
        picked: HashMap<u64, Offer>,
        choices: Vec<HashMap<u64, Offer>>,
    ) -> HashMap<u64, Offer> {
        choices
            .into_iter()
            .skip_while(|option| {
                option.iter().any(|(cap_guid, offer)| {
                    if let Some(picked_offer) = picked.get(&cap_guid) {
                        picked_offer.available != offer.available
                    } else {
                        true
                    }
                })
            })
            .next()
            .expect("picked must be in choices")
    }

    let server_choice = check_possibilities(server_choice, saved_possibilities);

    // tell all the stack elements about the nonce = (client addr, chosen stack)
    let nonce = NegotiateMsg::ServerNonce {
        addr: bincode::serialize(from_addr)?,
        picked: server_choice,
    };
    let nonce_buf =
        bincode::serialize(&nonce).wrap_err("Failed to serialize (addr, chosen_stack) nonce")?;

    new_stack
        .call_negotiate_picked(&nonce_buf)
        .instrument(debug_span!("call_negotiate_picked"))
        .await;

    Ok((new_stack, client_choices))
}

pub trait Apply {
    type Applied;
    fn apply(
        self,
        picked_offers: HashMap<u64, Offer>,
    ) -> Result<(Self::Applied, HashMap<u64, Offer>, usize), Report>;
}

impl<N> Apply for N
where
    N: Negotiate,
    <N as Negotiate>::Capability: DeserializeOwned + Ord,
{
    type Applied = Self;
    fn apply(
        self,
        mut picked_offers: HashMap<u64, Offer>,
    ) -> Result<(Self::Applied, HashMap<u64, Offer>, usize), Report> {
        let cap_guid = N::Capability::guid();
        if cap_guid == 0 {
            return Ok((self, picked_offers, 0));
        }

        let offer = match picked_offers.remove(&cap_guid) {
            None => {
                return Err(eyre!(
                    "Missing offer for type {:?}",
                    std::any::type_name::<N::Capability>()
                ));
            }
            Some(o) => o,
        };

        let cs: Result<Vec<N::Capability>, _> = offer
            .available
            .iter()
            .map(|o| {
                bincode::deserialize(&o).wrap_err(eyre!("Failed deserializing offer capabilities"))
            })
            .collect();
        let mut cs = cs?;
        cs.sort();
        let mut this = N::capabilities();
        this.sort();
        if cs != this {
            return Err(eyre!(
                "Capability offers mismatch: offer={:?}, this={:?}",
                cs,
                this
            ));
        }

        Ok((self, picked_offers, 0))
    }
}

impl<H, T> Apply for CxList<H, T>
where
    H: Apply,
    T: Apply,
{
    type Applied = CxList<H::Applied, T::Applied>;

    fn apply(
        self,
        picked_offers: HashMap<u64, Offer>,
    ) -> Result<(Self::Applied, HashMap<u64, Offer>, usize), Report> {
        let (head_pick, picked_offers, h_score) = self.head.apply(picked_offers)?;
        let (tail_pick, picked_offers, t_score) = self.tail.apply(picked_offers)?;
        Ok((
            CxList {
                head: head_pick,
                tail: tail_pick,
            },
            picked_offers,
            h_score + t_score,
        ))
    }
}

impl<T1, T2, Inner, E> Apply for Select<T1, T2, Inner>
where
    T1: Apply,
    T2: Apply,
    Inner: MakeEither<<T1 as Apply>::Applied, <T2 as Apply>::Applied, Either = E>
        + MakeEither<<T2 as Apply>::Applied, <T1 as Apply>::Applied>,
    <Inner as MakeEither<<T2 as Apply>::Applied, <T1 as Apply>::Applied>>::Either:
        FlipEither<Flipped = E>,
{
    type Applied = E;

    fn apply(
        self,
        picked_offers: HashMap<u64, Offer>,
    ) -> Result<(Self::Applied, HashMap<u64, Offer>, usize), Report> {
        fn apply_in_preference_order<T1, T2, Inner>(
            first_pick: T1,
            second_pick: T2,
            picked_offers: HashMap<u64, Offer>,
        ) -> Result<(Inner::Either, HashMap<u64, Offer>, usize), Report>
        where
            T1: Apply,
            T2: Apply,
            Inner: MakeEither<<T1 as Apply>::Applied, <T2 as Apply>::Applied>,
        {
            match first_pick.apply(picked_offers.clone()) {
                Ok((t1_applied, r, sc)) => Ok((Inner::left(t1_applied), r, sc + 1)),
                Err(e) => {
                    debug!(t1 = std::any::type_name::<T1>(), err = %format!("{:#}", &e), "Select::T1 mismatched");
                    let (t2_applied, r, sc) = second_pick.apply(picked_offers)?;
                    Ok((Inner::right(t2_applied), r, sc))
                }
            }
        }

        match self.prefer {
            Either::Left(_) => {
                apply_in_preference_order::<T1, T2, Inner>(self.left, self.right, picked_offers)
            }
            Either::Right(_) => {
                let (eit, hm, sc) = apply_in_preference_order::<T2, T1, Inner>(
                    self.right,
                    self.left,
                    picked_offers,
                )?;
                Ok((eit.flip(), hm, sc))
            }
        }
    }
}

pub type NegotiatedConn<C, S> = <<S as Apply>::Applied as Chunnel<C>>::Connection;

/// Return a connection with `stack`'s semantics, connecting to `a`.
pub fn negotiate_client<C, A, S>(
    stack: S,
    cn: C,
    addr: A,
) -> impl Future<Output = Result<NegotiatedConn<C, S>, Report>> + Send + 'static
where
    C: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync + 'static,
    S: Apply + GetOffers + Clone + Send + 'static,
    <S as Apply>::Applied: Chunnel<C> + Clone + Debug + Send + 'static,
    <<S as Apply>::Applied as Chunnel<C>>::Error: Into<Report> + Send + Sync + 'static,
    A: Send + Sync + 'static,
{
    async move {
        // 1. get Vec<u8> connection.
        debug!("client negotiation starting");

        // 2. send offers
        let offers = NegotiateMsg::ClientOffer(stack.offers().collect());
        let buf = bincode::serialize(&offers)?;
        cn.send((addr, buf)).await?;

        // 3. receive picked
        let (_, buf) = cn.recv().await?;
        let resp: NegotiateMsg = bincode::deserialize(&buf).wrap_err(eyre!(
            "Could not deserialize negotiate_server response: {:?}",
            buf
        ))?;
        match resp {
            NegotiateMsg::ServerReply(Ok(picked)) => {
                // 4. monomorphize `stack`, picking received choices
                let mut sc = 0;
                let mut new_stack = None;
                for p in picked {
                    let (ns, _, p_sc) = stack
                        .clone()
                        .apply(p)
                        .wrap_err(eyre!("Could not apply received impls to stack"))?;
                    // TODO what if two options are tied? This will arbitrarily pick the first.
                    if p_sc > sc || new_stack.is_none() {
                        sc = p_sc;
                        new_stack = Some(ns);
                    }
                }

                let mut new_stack = new_stack.unwrap();
                debug!(applied = ?&new_stack, "applied to stack");

                // 5. return new_stack.connect_wrap(vec_u8_conn)
                new_stack.connect_wrap(cn).await.map_err(Into::into)
            }
            NegotiateMsg::ServerReply(Err(errmsg)) => Err(eyre!("{:?}", errmsg)),
            _ => Err(eyre!("Received unknown message type")),
        }
    }
}

#[allow(non_upper_case_globals)]
#[cfg(test)]
mod test {
    use super::{negotiate_client, negotiate_server, CapabilitySet, Negotiate, Select};
    use crate::{
        chan_transport::Chan, Chunnel, ChunnelConnection, ChunnelConnector, ChunnelListener, CxList,
    };
    use color_eyre::eyre::{eyre, Report};
    use futures_util::{
        future::{ready, Ready},
        stream::StreamExt,
    };
    use tracing::{debug, debug_span, info, info_span};
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[allow(non_upper_case_globals)]
    macro_rules! mock_serve_impl {
        (StructDef==>$name:ident) => {
            #[derive(Debug, Clone, Copy)]
            struct $name;

            impl<D, InC> Chunnel<InC> for $name
            where
                InC: ChunnelConnection<Data = D> + Send + Sync + 'static,
                D: Send + Sync + 'static,
            {
                type Future = Ready<Result<Self::Connection, Self::Error>>;
                type Connection = InC;
                type Error = Report;

                fn connect_wrap(&mut self, inner: InC) -> Self::Future {
                    ready(Ok(inner))
                }
            }
        };
        ($name:ident) => {
            paste::paste! {
                lazy_static::lazy_static! {
                    static ref [<$name CapGuid>]: u64 = rand::random();
                    static ref [<$name ImplGuid>]: u64 = rand::random();
                }

                enumerate_enum!([<$name Cap>], *[<$name CapGuid>], A, B, C);
            }

            mock_serve_impl!(StructDef==>$name);

            paste::paste! {
            impl Negotiate for $name {
                type Capability = [<$name Cap>];
                fn guid() -> u64 { *[<$name ImplGuid>] }
                fn capabilities() -> Vec<Self::Capability> {
                    [<$name Cap>]::universe().unwrap()
                }
            }
            }
        };
    }

    mock_serve_impl!(ChunnelA);
    mock_serve_impl!(ChunnelB);
    mock_serve_impl!(ChunnelC);

    #[allow(non_upper_case_globals)]
    macro_rules! mock_alt_impl {
        ($name:ident) => {
            paste::paste! {
            mock_serve_impl!(StructDef==>[< $name Alt >]);

            lazy_static::lazy_static! {
                static ref [<$name ImplGuidAlt>]: u64 = rand::random();
            }

            impl Negotiate for [< $name Alt >] {
                type Capability = [<$name Cap>];
                fn guid() -> u64 { *[<$name ImplGuidAlt>] }
                fn capabilities() -> Vec<Self::Capability> {
                    [<$name Cap>]::universe().unwrap()
                }
            }
            }
        };
    }

    mock_alt_impl!(ChunnelB);

    #[test]
    fn both_select() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or_else(|_| ());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                info!("starting");
                let (mut srv, mut cln) = Chan::default().split();
                let stack = CxList::from(ChunnelA)
                    .wrap(Select::from((ChunnelB, ChunnelBAlt)))
                    .wrap(ChunnelC);
                let srv_stack = stack.clone();

                let (s, r) = tokio::sync::oneshot::channel();
                tokio::spawn(
                    async move {
                        info!("starting");
                        let raw_st = srv.listen(()).await?;
                        let srv_stream = negotiate_server(srv_stack, raw_st).await?;
                        s.send(()).unwrap();
                        tokio::pin!(srv_stream);
                        let cn = srv_stream
                            .next()
                            .await
                            .ok_or_else(|| eyre!("srv_stream returned none"))??;
                        let buf = cn.recv().await?;
                        cn.send(buf).await?;
                        Ok::<_, Report>(())
                    }
                    .instrument(debug_span!("server")),
                );

                r.await.unwrap();

                let raw_cn = cln.connect(()).await?;
                let cn = negotiate_client(stack, raw_cn, ())
                    .instrument(info_span!("negotiate_client"))
                    .await?;

                cn.send(((), vec![1u8; 10])).await?;
                let (_, buf) = cn.recv().await?;

                assert_eq!(buf, vec![1u8; 10]);

                info!("done");

                Ok::<_, Report>(())
            }
            .instrument(info_span!("both_select")),
        )
        .unwrap();
    }

    #[test]
    fn multiclient() {
        use crate::udp::{UdpReqChunnel, UdpSkChunnel};
        use futures_util::TryStreamExt;
        use std::net::ToSocketAddrs;

        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or_else(|_| ());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                info!("starting");
                let stack = CxList::from(ChunnelA)
                    .wrap(Select::from((ChunnelB, ChunnelBAlt)))
                    .wrap(ChunnelC);
                let srv_stack = stack.clone();
                let addr = "127.0.0.1:42184".to_socket_addrs().unwrap().next().unwrap();
                let (s, r) = tokio::sync::oneshot::channel();
                tokio::spawn(
                    async move {
                        info!("starting");
                        let raw_st = UdpReqChunnel::default().listen(addr).await?;
                        let srv_stream = negotiate_server(srv_stack, raw_st).await?;
                        s.send(()).unwrap();
                        srv_stream
                            .try_for_each_concurrent(None, |cn| async move {
                                info!("got connection");
                                loop {
                                    let buf = cn.recv().await?;
                                    cn.send(buf).await?;
                                    debug!("echoed");
                                }
                            })
                            .instrument(info_span!("server"))
                            .await
                            .unwrap();
                        Ok::<_, Report>(())
                    }
                    .instrument(info_span!("server")),
                );

                r.await.unwrap();
                info!("starting client");
                let raw_cn = UdpSkChunnel::default().connect(()).await?;
                let cn1 = negotiate_client(stack.clone(), raw_cn, addr)
                    .instrument(info_span!("negotiate_client"))
                    .await?;

                let raw_cn = UdpSkChunnel::default().connect(()).await?;
                let cn2 = negotiate_client(stack, raw_cn, addr)
                    .instrument(info_span!("negotiate_client"))
                    .await?;

                for _ in 0..10 {
                    debug!("sending");
                    cn1.send((addr, vec![1u8; 10])).await?;
                    cn2.send((addr, vec![2u8; 10])).await?;
                    let (_, buf1) = cn1.recv().await?;
                    let (_, buf2) = cn2.recv().await?;
                    assert_eq!(buf1, vec![1u8; 10]);
                    assert_eq!(buf2, vec![2u8; 10]);
                }
                info!("done");
                Ok::<_, Report>(())
            }
            .instrument(info_span!("negotiate::multiclient")),
        )
        .unwrap();
    }

    #[allow(non_upper_case_globals)]
    macro_rules! mock_serve_bothsides_impl {
        ($name:ident) => {
            paste::paste! {
            lazy_static::lazy_static! {
                static ref [<$name CapGuid>]: u64 = rand::random();
                static ref [<$name ImplGuid>]: u64 = rand::random();
            }

            #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
            enum [<$name Cap>] {
                A,
            }

            impl $crate::negotiate::CapabilitySet for [<$name Cap>] {
                fn guid() -> u64 {
                    *[<$name CapGuid>]
                }

                fn universe() -> Option<Vec<Self>> { None }
            }
            }

            mock_serve_impl!(StructDef==>$name);

            paste::paste! {
            impl Negotiate for $name {
                type Capability = [<$name Cap>];
                fn guid() -> u64 { *[<$name ImplGuid>] }
                fn capabilities() -> Vec<Self::Capability> {
                    vec![ [<$name Cap>]::A ]
                }
            }
            }
        };
    }

    mock_serve_bothsides_impl!(ChunnelD);

    #[test]
    fn ensure_bothsides() {
        use crate::udp::{UdpReqChunnel, UdpSkChunnel};
        use futures_util::TryStreamExt;
        use std::net::ToSocketAddrs;

        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or_else(|_| ());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                info!("starting");
                let stack = CxList::from(ChunnelA).wrap(ChunnelD);
                debug!(chunnelA = ?*ChunnelACapGuid, chunnelD = ?*ChunnelDCapGuid, "guids");
                let srv_stack = stack.clone();
                let addr = "127.0.0.1:52184".to_socket_addrs().unwrap().next().unwrap();
                let (s, r) = tokio::sync::oneshot::channel();
                tokio::spawn(
                    async move {
                        info!("starting");
                        let raw_st = UdpReqChunnel::default().listen(addr).await?;
                        let srv_stream = negotiate_server(srv_stack, raw_st).await?;
                        s.send(()).unwrap();
                        srv_stream
                            .try_for_each_concurrent(None, |cn| async move {
                                info!("got connection");
                                loop {
                                    let buf = cn.recv().await?;
                                    cn.send(buf).await?;
                                    debug!("echoed");
                                }
                            })
                            .instrument(info_span!("negotiate_server"))
                            .await
                            .unwrap();
                        Ok::<_, Report>(())
                    }
                    .instrument(info_span!("server")),
                );

                r.await.unwrap();
                info!("starting client");
                //let cl_stack = CxList::from(ChunnelA).wrap(ChunnelD);
                let cl_stack = ChunnelA;
                let raw_cn = UdpSkChunnel::default().connect(()).await?;
                let _ = negotiate_client(cl_stack, raw_cn, addr)
                    .instrument(info_span!("negotiate_client"))
                    .await
                    .unwrap_err();
                Ok::<_, Report>(())
            }
            .instrument(info_span!("negotiate::multiclient")),
        )
        .unwrap();
    }
}
