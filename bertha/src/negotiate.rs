//! Chunnel wrapper types to negotiate between multiple implementations.

use crate::{ChunnelConnection, Client, CxList, CxListReverse, CxNil, Either};
use color_eyre::eyre::{eyre, Report, WrapErr};
use futures_util::{
    future::{select, FutureExt},
    stream::{Stream, TryStreamExt},
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::iter::{empty, once, Chain, Empty, Once as IterOnce};
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
}

impl<T1, T2, C> Negotiate for Either<T1, T2>
where
    T1: Negotiate<Capability = C>,
    T2: Negotiate<Capability = C>,
    C: CapabilitySet,
{
    type Capability = C;

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

/// Negotiation type to choose between `T1` and `T2`.
#[derive(Clone)]
pub struct Select<T1, T2>(pub T1, pub T2);

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Offer {
    capability_guid: u64,
    available: Vec<u8>,
}

fn get_offer<T>() -> Option<Offer>
where
    T: Negotiate,
    <T as Negotiate>::Capability: Serialize + DeserializeOwned,
{
    if T::Capability::guid() != 0 {
        Some(Offer {
            capability_guid: T::Capability::guid(),
            available: bincode::serialize(&T::capabilities()).unwrap(),
        })
    } else {
        None
    }
}

pub trait GetOffers {
    type Iter: Iterator<Item = Offer>;
    fn offers(&self) -> Self::Iter;
}

impl<H, T> GetOffers for CxList<H, T>
where
    H: GetOffers,
    T: GetOffers,
{
    type Iter = Chain<H::Iter, T::Iter>;
    fn offers(&self) -> Self::Iter {
        self.head.offers().chain(self.tail.offers())
    }
}

impl<N> GetOffers for N
where
    N: Negotiate,
    <N as Negotiate>::Capability: Serialize + DeserializeOwned,
{
    type Iter = Either<IterOnce<Offer>, Empty<Offer>>;

    fn offers(&self) -> Self::Iter {
        if let Some(o) = get_offer::<N>() {
            Either::Left(once(o))
        } else {
            Either::Right(empty())
        }
    }
}

impl<T1, T2> GetOffers for Select<T1, T2>
where
    T1: GetOffers,
    T2: GetOffers,
{
    type Iter = Chain<T1::Iter, T2::Iter>;
    fn offers(&self) -> Self::Iter {
        self.0.offers().chain(self.1.offers())
    }
}

/// Trait to monomorphize a CxList with possible `Select`s into something that impls Client
pub trait Pick {
    type Picked;
    type Iter: Iterator<Item = Offer>;

    /// input: set of offers from a client (`client_offers`), grouped by capability_guid
    ///
    /// Returns (new_stack, picked_offers, leftover)
    fn pick(
        self,
        client_offers: HashMap<u64, Vec<Offer>>,
    ) -> Result<(Self::Picked, Self::Iter, HashMap<u64, Vec<Offer>>), Report>;
}

impl<N, C> Pick for N
where
    N: Negotiate<Capability = C>,
    C: CapabilitySet + Serialize + DeserializeOwned + Clone,
{
    type Picked = Self;
    type Iter = Either<IterOnce<Offer>, Empty<Offer>>;

    fn pick(
        self,
        mut client_offers: HashMap<u64, Vec<Offer>>,
    ) -> Result<(Self::Picked, Self::Iter, HashMap<u64, Vec<Offer>>), Report> {
        if N::Capability::universe().is_none() {
            let self_caps = N::capabilities();
            if let Some(os) = client_offers.remove(&N::Capability::guid()) {
                let caps: Result<Vec<Vec<N::Capability>>, Report> = os
                    .iter()
                    .map(|o| {
                        let c: Vec<N::Capability> =
                            bincode::deserialize(&o.available).wrap_err(eyre!(
                                "Could not deserialize capability set: {:?} to type {:?}",
                                o,
                                std::any::type_name::<C>()
                            ))?;
                        Ok(c)
                    })
                    .collect();
                let caps = caps?;
                for caps_co in caps.iter() {
                    trace!(?self_caps, check = ?&caps_co, "checking client offer");
                    if have_all(&self_caps, &caps_co) {
                        return Ok((self, Either::Left(once(caps_co.into())), client_offers));
                    }
                }

                debug!(?self_caps, available = ?&caps, "Did not find matching client offer");
            } else {
                debug!(
                    ?self_caps,
                    available = "None",
                    "Did not find matching client offer"
                );
            }

            Err(eyre!(
                "Could not find satisfying client/server capability set for {:?}",
                std::any::type_name::<C>()
            ))
        } else {
            if N::Capability::guid() == 0 {
                Ok((self, Either::Right(empty()), client_offers))
            } else if let Some(os) = client_offers.remove(&N::Capability::guid()) {
                let o = os.into_iter().next().unwrap();
                Ok((self, Either::Left(once(o)), client_offers))
            } else {
                Ok((self, Either::Right(empty()), client_offers))
            }
        }
    }
}

impl<H, T> Pick for CxList<H, T>
where
    H: Pick,
    T: Pick,
{
    type Picked = CxList<H::Picked, T::Picked>;
    type Iter = Chain<H::Iter, T::Iter>;

    fn pick(
        self,
        client_offers: HashMap<u64, Vec<Offer>>,
    ) -> Result<(Self::Picked, Self::Iter, HashMap<u64, Vec<Offer>>), Report> {
        let (head_pick, cl_pick, client_offers) = self.head.pick(client_offers)?;
        let (tail_pick, rest_cl_pick, client_offers) = self.tail.pick(client_offers)?;
        let pick = cl_pick.chain(rest_cl_pick);
        Ok((
            CxList {
                head: head_pick,
                tail: tail_pick,
            },
            pick,
            client_offers,
        ))
    }
}

impl<C> From<&Vec<C>> for Offer
where
    C: CapabilitySet + Serialize,
{
    fn from(f: &Vec<C>) -> Self {
        Offer {
            capability_guid: C::guid(),
            available: bincode::serialize(f).unwrap(),
        }
    }
}

fn lacks<T: PartialEq>(a: &[T], univ: &[T]) -> bool {
    univ.iter().any(|x| !a.contains(x))
}

fn have_all<T: PartialEq>(client: &[T], server: &[T]) -> bool {
    server.iter().all(|x| client.contains(x)) && client.iter().all(|x| server.contains(x))
}

impl<T1, T2, C> Pick for Select<T1, T2>
where
    T1: Negotiate<Capability = C>,
    T2: Negotiate<Capability = C>,
    C: CapabilitySet + Serialize + DeserializeOwned + Clone,
{
    type Picked = Either<T1, T2>;
    type Iter = Either<IterOnce<Offer>, Empty<Offer>>;

    fn pick(
        self,
        mut client_offers: HashMap<u64, Vec<Offer>>,
    ) -> Result<(Self::Picked, Self::Iter, HashMap<u64, Vec<Offer>>), Report> {
        if C::guid() == 0 {
            return Ok((Either::Left(self.0), Either::Right(empty()), client_offers));
        }

        let offers = match client_offers.remove(&C::guid()) {
            None => {
                return Err(eyre!(
                    "Missing offer for type {:?}",
                    std::any::type_name::<C>()
                ));
            }
            Some(o) => o,
        };

        trace!(
            cap_type = std::any::type_name::<C>(),
            offer = ?&offers,
            "deserializing offers"
        );

        let caps: Result<Vec<Vec<C>>, Report> = offers
            .iter()
            .map(|o| {
                let c: Vec<C> = bincode::deserialize(&o.available).wrap_err(eyre!(
                    "Could not deserialize capability set: {:?} to type {:?}",
                    o,
                    std::any::type_name::<C>()
                ))?;
                Ok(c)
            })
            .collect();
        let mut caps = caps?;

        debug!(
            cap_type = std::any::type_name::<C>(),
            offer = ?&caps,
            "considering offers"
        );

        caps.sort_by(|a, b| b.len().cmp(&a.len()));

        let t1 = T1::capabilities();
        let t2 = T2::capabilities();

        let univ = C::universe();
        if let Some(univ) = univ {
            // one-sided. Compare the sum of the two sets to the universe
            for caps_co in caps.iter() {
                if t1.len() <= t2.len() {
                    let mut co: Vec<C> = caps_co.clone();
                    co.extend_from_slice(&t1);
                    if !lacks(&co, &univ) {
                        return Ok((
                            Either::Left(self.0),
                            Either::Left(once(caps_co.into())),
                            client_offers,
                        ));
                    }

                    let mut co = caps_co.clone();
                    co.extend_from_slice(&t2);
                    if !lacks(&co, &univ) {
                        return Ok((
                            Either::Right(self.1),
                            Either::Left(once(caps_co.into())),
                            client_offers,
                        ));
                    }
                } else {
                    let mut co = caps_co.clone();
                    co.extend_from_slice(&t2);
                    if !lacks(&co, &univ) {
                        return Ok((
                            Either::Right(self.1),
                            Either::Left(once(caps_co.into())),
                            client_offers,
                        ));
                    }

                    let mut co = caps_co.clone();
                    co.extend_from_slice(&t1);
                    if !lacks(&co, &univ) {
                        return Ok((
                            Either::Left(self.0),
                            Either::Left(once(caps_co.into())),
                            client_offers,
                        ));
                    }
                }
            }
        } else {
            // both-sided. Try both T1 and T2.
            for caps_co in caps.iter() {
                if have_all(&t1, &caps_co) {
                    return Ok((
                        Either::Left(self.0),
                        Either::Left(once(caps_co.into())),
                        client_offers,
                    ));
                } else {
                    if have_all(&t2, &caps_co) {
                        return Ok((
                            Either::Right(self.1),
                            Either::Left(once(caps_co.into())),
                            client_offers,
                        ));
                    }
                }
            }
        }

        Err(eyre!(
            "Could not find satisfying client/server capability set for {:?}",
            std::any::type_name::<C>()
        ))
    }
}

fn group_offers(offers: Vec<Offer>) -> HashMap<u64, Vec<Offer>> {
    let mut map = HashMap::<_, Vec<Offer>>::new();
    for o in offers {
        map.entry(o.capability_guid).or_default().push(o);
    }

    map
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
    ClientOffer(Vec<Offer>),
    ServerReply(Result<Vec<Offer>, String>),
    ServerNonce { addr: Vec<u8>, picked: Vec<Offer> },
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
                        <<<Srv as Pick>::Picked as CxListReverse>::Reversed as Client<ClientInput<C, A>>>::Connection,
                        <<<Srv as Apply<(A, Vec<u8>)>>::Applied as CxListReverse>::Reversed as Client<ClientInput<C, A>>>::Connection,
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
    Srv: Pick + Apply<(A, Vec<u8>)> + GetOffers + Clone + Send + 'static,
// main-line branch: Pick on incoming negotiation handshake.
    <Srv as Pick>::Picked: CxListReverse + Send + 'static,
    <<Srv as Pick>::Picked as CxListReverse>::Reversed: NegotiatePicked + Client<ClientInput<C, A>> + Clone + Send + 'static,
    <<<Srv as Pick>::Picked as CxListReverse>::Reversed as Client<ClientInput<C, A>>>::Connection: Send + Sync + 'static,
    <<<Srv as Pick>::Picked as CxListReverse>::Reversed as Client<ClientInput<C, A>>>::Error: Into<Report> + Send + Sync + 'static,
    <Srv as Pick>::Iter: Send,
// nonce branch: Apply stack from nonce on indicated connections.
    <Srv as Apply<(A, Vec<u8>)>>::Applied: CxListReverse + Send + 'static,
    <<Srv as Apply<(A, Vec<u8>)>>::Applied as CxListReverse>::Reversed: Client<ClientInput<C, A>> + Clone + Send + 'static,
    <<<Srv as Apply<(A, Vec<u8>)>>::Applied as CxListReverse>::Reversed as Client<ClientInput<C, A>>>::Connection:
        Send + Sync + 'static,
    <<<Srv as Apply<(A, Vec<u8>)>>::Applied as CxListReverse>::Reversed as Client<ClientInput<C, A>>>::Error:
        Into<Report> + Send + Sync + 'static,
    A: Serialize + DeserializeOwned + Eq + std::hash::Hash + Debug + Send + Sync + 'static,
{
    async move {
        // 1. serve (A, Vec<u8>) connections.
        let st = raw_cn_st.map_err(Into::into); // stream of incoming Vec<u8> conns.
        let pending_negotiated_connections: Arc<Mutex<HashMap<A, Vec<Offer>>>> = Default::default();
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
    pending_negotiated_connections: Arc<Mutex<HashMap<A, Vec<Offer>>>>,
) -> Result<
    Option<Either<
        <<<Srv as Pick>::Picked as CxListReverse>::Reversed as Client<ClientInput<C, A>>>::Connection,
        <<<Srv as Apply<(A, Vec<u8>)>>::Applied as CxListReverse>::Reversed as Client<ClientInput<C, A>>>::Connection,
    >>,
    Report,
>
where
    C: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync + 'static,
    Srv: Pick + Apply<(A, Vec<u8>)> + GetOffers + Clone + Send + 'static,
// main-line branch: Pick on incoming negotiation handshake.
    <Srv as Pick>::Picked: CxListReverse + Send + 'static,
    <<Srv as Pick>::Picked as CxListReverse>::Reversed:
        NegotiatePicked + Client<ClientInput<C, A>> + Clone + Send + 'static,
    <<<Srv as Pick>::Picked as CxListReverse>::Reversed as Client<ClientInput<C, A>>>::Connection:
        Send + Sync + 'static,
    <<<Srv as Pick>::Picked as CxListReverse>::Reversed as Client<ClientInput<C, A>>>::Error:
        Into<Report> + Send + Sync + 'static,
    <Srv as Pick>::Iter: Send,
// nonce branch: Apply stack from nonce on indicated connections.
    <Srv as Apply<(A, Vec<u8>)>>::Applied: CxListReverse + Send + 'static,
    <<Srv as Apply<(A, Vec<u8>)>>::Applied as CxListReverse>::Reversed:
        Client<ClientInput<C, A>> + Clone + Send + 'static,
    <<<Srv as Apply<(A, Vec<u8>)>>::Applied as CxListReverse>::Reversed as Client<
        ClientInput<C, A>,
    >>::Connection: Send + Sync + 'static,
    <<<Srv as Apply<(A, Vec<u8>)>>::Applied as CxListReverse>::Reversed as Client<
        ClientInput<C, A>,
    >>::Error: Into<Report> + Send + Sync + 'static,
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
            let (stack, _) = stack
                .apply(group_offers(picked))
                .wrap_err("failed to apply semantics to client connection")?;
            let mut stack = stack.rev();
            let new_cn = stack.connect_wrap(cn).await.map_err(Into::into)?;

            debug!(addr = ?&a, "returning pre-negotiated connection");
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
                debug!(client_addr = ?&addr, nonce = ?&picked, "got nonce");
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
                cn.send((a, buf)).await?;

                if let Some(mut new_stack) = new_stack {
                    debug!("negotiation handshake done");

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
    pending_negotiated_connections: Arc<Mutex<HashMap<A, Vec<Offer>>>>,
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
                debug!(client_addr = ?&addr, nonce = ?&picked, "got nonce");
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
    client_offers: Vec<Offer>,
    from_addr: &A,
) -> Result<
    (
        <<Srv as Pick>::Picked as CxListReverse>::Reversed,
        Vec<Offer>,
    ),
    Report,
>
where
    Srv: Pick + GetOffers + Clone + Send + 'static,
    // main-line branch: Pick on incoming negotiation handshake.
    <Srv as Pick>::Picked: CxListReverse + Send + 'static,
    <<Srv as Pick>::Picked as CxListReverse>::Reversed: NegotiatePicked + Clone + Send + 'static,
    <Srv as Pick>::Iter: Send,
    A: Serialize + DeserializeOwned + Debug + Send + Sync + 'static,
{
    let client_offers_map = group_offers(client_offers);
    debug!(client_offers = ?&client_offers_map, ?from_addr, "received offer");
    // 3. monomorphize: transform the CxList<impl Serve/Select<impl Serve,
    //    impl Serve>> into a CxList<impl Serve>
    let (new_stack, picked_offers, _) = stack
        .pick(client_offers_map)
        .wrap_err(eyre!("error monomorphizing stack",))?;

    let picked_offers_vec: Vec<Offer> = picked_offers.collect();
    debug!(picked_client_offers = ?&picked_offers_vec, "monomorphized stack");
    // tell all the stack elements about the nonce = (client addr, chosen stack)
    let nonce = NegotiateMsg::ServerNonce {
        addr: bincode::serialize(from_addr)?,
        picked: picked_offers_vec.clone(),
    };
    let nonce_buf =
        bincode::serialize(&nonce).wrap_err("Failed to serialize (addr, chosen_stack) nonce")?;
    let mut new_stack = new_stack.rev();
    new_stack
        .call_negotiate_picked(&nonce_buf)
        .instrument(debug_span!("call_negotiate_picked"))
        .await;

    Ok((new_stack, picked_offers_vec))
}

pub trait Apply<D> {
    type Applied;
    fn apply(
        self,
        picked_offers: HashMap<u64, Vec<Offer>>,
    ) -> Result<(Self::Applied, HashMap<u64, Vec<Offer>>), Report>;
}

impl<N, D> Apply<D> for N
where
    N: Negotiate,
    <N as Negotiate>::Capability: DeserializeOwned + Ord,
{
    type Applied = Self;
    fn apply(
        self,
        mut picked_offers: HashMap<u64, Vec<Offer>>,
    ) -> Result<(Self::Applied, HashMap<u64, Vec<Offer>>), Report> {
        let cap_guid = N::Capability::guid();
        if cap_guid == 0 {
            return Ok((self, picked_offers));
        }

        let mut offers = match picked_offers.remove(&cap_guid) {
            None => {
                return Err(eyre!(
                    "Missing offer for type {:?}",
                    std::any::type_name::<N::Capability>()
                ));
            }
            Some(o) => o,
        };

        if offers.is_empty() {
            return Err(eyre!("Got empty offer set"));
        }

        let o = offers.pop().unwrap();
        let mut cs: Vec<N::Capability> = bincode::deserialize(&o.available)
            .wrap_err(eyre!("Failed deserializing offer capabilities"))?;
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

        Ok((self, picked_offers))
    }
}

impl<H, T, D> Apply<D> for CxList<H, T>
where
    H: Apply<D>,
    T: Apply<D>,
{
    type Applied = CxList<H::Applied, T::Applied>;

    fn apply(
        self,
        picked_offers: HashMap<u64, Vec<Offer>>,
    ) -> Result<(Self::Applied, HashMap<u64, Vec<Offer>>), Report> {
        let (head_pick, picked_offers) = self.head.apply(picked_offers)?;
        let (tail_pick, picked_offers) = self.tail.apply(picked_offers)?;
        Ok((
            CxList {
                head: head_pick,
                tail: tail_pick,
            },
            picked_offers,
        ))
    }
}

impl<T1, T2, D> Apply<D> for Select<T1, T2>
where
    T1: Apply<D>,
    T2: Apply<D>,
{
    type Applied = Either<<T1 as Apply<D>>::Applied, <T2 as Apply<D>>::Applied>;

    fn apply(
        self,
        picked_offers: HashMap<u64, Vec<Offer>>,
    ) -> Result<(Self::Applied, HashMap<u64, Vec<Offer>>), Report> {
        match self.0.apply(picked_offers.clone()) {
            Ok((t1_applied, r)) => Ok((Either::Left(t1_applied), r)),
            Err(e) => {
                debug!(t1 = std::any::type_name::<T1>(), err = ?e, "Select::T1 mismatched");
                let (t2_applied, r) = self.1.apply(picked_offers)?;
                Ok((Either::Right(t2_applied), r))
            }
        }
    }
}

pub type NegotiatedConn<C, A, S> =
    <<<S as Apply<(A, Vec<u8>)>>::Applied as CxListReverse>::Reversed as Client<C>>::Connection;

/// Return a connection with `stack`'s semantics, connecting to `a`.
pub fn negotiate_client<C, A, S>(
    stack: S,
    cn: C,
    addr: A,
) -> impl Future<Output = Result<NegotiatedConn<C, A, S>, Report>> + Send + 'static
where
    C: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync + 'static,
    S: Apply<(A, Vec<u8>)> + GetOffers + Clone + Send + 'static,
    <S as Apply<(A, Vec<u8>)>>::Applied: CxListReverse + Send + 'static,
    <<S as Apply<(A, Vec<u8>)>>::Applied as CxListReverse>::Reversed:
        Client<C> + Clone + Debug + Send + 'static,
    <<<S as Apply<(A, Vec<u8>)>>::Applied as CxListReverse>::Reversed as Client<C>>::Error:
        Into<Report> + Send + Sync + 'static,
    A: Send + Sync + 'static,
{
    async move {
        // 1. get Vec<u8> connection.
        debug!("got negotiation connection");

        // 2. send Vec<Offer>
        let offers = NegotiateMsg::ClientOffer(stack.offers().collect());
        let buf = bincode::serialize(&offers)?;
        debug!(offers = ?&offers, "sending offers");
        cn.send((addr, buf)).await?;

        // 3. receive Vec<Offer>
        let (_, buf) = cn.recv().await?;
        let resp: NegotiateMsg = bincode::deserialize(&buf)?;
        match resp {
            NegotiateMsg::ServerReply(Ok(picked)) => {
                debug!(picked = ?&picked, "received picked impls");

                // 4. monomorphize `stack`, picking received choices
                let (new_stack, _) = stack
                    .apply(group_offers(picked))
                    .wrap_err(eyre!("Could not apply received impls to stack"))?;
                let mut new_stack = new_stack.rev();

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
    use super::{
        negotiate_client, negotiate_server, CapabilitySet, GetOffers, Negotiate, NegotiateMsg,
        Offer, Select,
    };
    use crate::{
        chan_transport::Chan, ChunnelConnection, ChunnelConnector, ChunnelListener, Client, CxList,
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

            impl<D, InC> Client<InC> for $name
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
                }

                enumerate_enum!([<$name Cap>], *[<$name CapGuid>], A, B, C);
            }

            mock_serve_impl!(StructDef==>$name);

            paste::paste! {
            impl Negotiate for $name {
                type Capability = [<$name Cap>];
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

    #[test]
    fn serve_no_select() {
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
                let stack = CxList::from(ChunnelA).wrap(ChunnelB).wrap(ChunnelC);
                let srv_stack = stack.clone();

                let (s, r) = tokio::sync::oneshot::channel();
                tokio::spawn(
                    async move {
                        info!("starting");
                        let raw_st = srv.listen(()).await?;
                        let mut srv_stream = negotiate_server(srv_stack, raw_st).await?;
                        s.send(()).unwrap();
                        // shadow the original variable so it can't accidentally be used after the pin,
                        // making this safe
                        let mut srv_stream =
                            unsafe { std::pin::Pin::new_unchecked(&mut srv_stream) };
                        srv_stream
                            .next()
                            .await
                            .ok_or_else(|| eyre!("srv_stream returned none"))??;
                        Ok::<_, Report>(())
                    }
                    .instrument(debug_span!("server")),
                );

                r.await.unwrap();

                // make a Vec<u8> client
                let cn = cln.connect(()).await?;

                // send the raw Vec<Vec<Offer>>
                // [ [Offer{guid: A, vec:[]}], [Offer{guid: B, vec:[]}], [Offer{guid: C, vec:[]}] ]
                let offers = NegotiateMsg::ClientOffer(stack.offers().collect());
                debug!(offers = ?&offers, "starting negotiation handshake");
                let buf = bincode::serialize(&offers)?;
                cn.send(((), buf)).await?;

                let (_, resp) = cn.recv().await?;
                let resp: NegotiateMsg = bincode::deserialize(&resp)?;
                debug!(resp = ?&resp, "got negotiation response");

                let resp = match resp {
                    NegotiateMsg::ServerReply(Ok(os)) => os,
                    x => panic!("Bad negotiation server response: {:?}", x),
                };

                let expected: Vec<Offer> = match offers {
                    NegotiateMsg::ClientOffer(os) => os.into_iter().collect(),
                    _ => unreachable!(),
                };

                assert_eq!(resp, expected);
                info!("done");
                Ok::<_, Report>(())
            }
            .instrument(info_span!("serve_no_select")),
        )
        .unwrap();
    }

    #[allow(non_upper_case_globals)]
    macro_rules! mock_alt_impl {
        ($name:ident) => {
            paste::paste! {
            mock_serve_impl!(StructDef==>[< $name Alt >]);

            impl Negotiate for [< $name Alt >] {
                type Capability = [<$name Cap>];
                fn capabilities() -> Vec<Self::Capability> {
                    [<$name Cap>]::universe().unwrap()
                }
            }
            }
        };
    }

    mock_alt_impl!(ChunnelB);

    #[test]
    fn get_offers() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or_else(|_| ());

        let stack = CxList::from(ChunnelA)
            .wrap(Select(ChunnelB, ChunnelBAlt))
            .wrap(ChunnelC);
        let offers = super::group_offers(stack.offers().collect());
        info!(offers = ?&offers, "select_offers");

        assert!(matches!(offers.get(
            &<<ChunnelB as Negotiate>::Capability as CapabilitySet>::guid()),
            Some(x) if x.len() == 2,
        ));

        let stack1 = CxList::from(ChunnelA).wrap(ChunnelB).wrap(ChunnelC);
        let offers1 = super::group_offers(stack1.offers().collect());
        info!(offers = ?&offers1, "no_select_offers");

        assert!(matches!(offers1.get(
            &<<ChunnelB as Negotiate>::Capability as CapabilitySet>::guid()),
            Some(x) if x.len() == 1,
        ));

        assert_eq!(offers.len(), offers1.len());
    }

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
                    .wrap(Select(ChunnelB, ChunnelBAlt))
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
                    .wrap(Select(ChunnelB, ChunnelBAlt))
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
                //let stack = ChunnelA;
                let stack = CxList::from(ChunnelA).wrap(ChunnelD);
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
