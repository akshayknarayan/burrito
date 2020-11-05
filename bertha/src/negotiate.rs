//! Chunnel wrapper types to negotiate between multiple implementations.

use crate::{ChunnelConnection, Client, CxList, CxNil, Either, Serve};
use color_eyre::{
    eyre::{eyre, Report, WrapErr},
    Section,
};
use futures_util::{
    future::{select, FutureExt, Ready},
    stream::{Once, Stream, TryStreamExt},
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{atomic::AtomicBool, Arc, Mutex};
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tracing::{debug, debug_span, instrument, trace, warn};
use tracing_futures::Instrument;

/// A type that can list out the `universe()` of possible values it can have.
pub trait CapabilitySet: core::fmt::Debug + PartialEq + Sized {
    /// All possible values this type can have.
    // TODO make return an unordered collection
    fn universe() -> Vec<Self>;

    fn guid() -> u64;
}

impl CapabilitySet for () {
    fn universe() -> Vec<Self> {
        vec![()]
    }

    fn guid() -> u64 {
        0
    }
}

impl<T, U> CapabilitySet for (Vec<T>, Vec<U>)
where
    T: CapabilitySet + Clone,
    U: CapabilitySet,
{
    fn universe() -> Vec<Self> {
        vec![(T::universe(), U::universe())]
    }

    fn guid() -> u64 {
        T::guid() + U::guid()
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
            fn universe() -> Vec<Self> {
                vec![
                    $($name::$variant),+
                ]
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
/// Read: `Negotiate` *over* `Capability`.
///
/// TODO Add endedness to this trait: onesided_capabilities vs bothsided_capabilities
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
    fn capabilities() -> Vec<Self::Capability> {
        vec![]
    }
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Offer {
    capability_guid: u64,
    available: Vec<u8>,
}

impl<C> From<Vec<C>> for Offer
where
    C: CapabilitySet + Serialize,
{
    fn from(f: Vec<C>) -> Self {
        Offer {
            capability_guid: C::guid(),
            available: bincode::serialize(&f).unwrap(),
        }
    }
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
    fn offers(&self) -> Vec<Vec<Offer>>;
}

impl<H, T> GetOffers for CxList<H, T>
where
    H: GetOffers,
    T: GetOffers,
{
    fn offers(&self) -> Vec<Vec<Offer>> {
        let mut offers = self.head.offers();
        let rest = self.tail.offers();
        offers.extend(rest);
        offers
    }
}

impl<N> GetOffers for N
where
    N: Negotiate,
    <N as Negotiate>::Capability: Serialize + DeserializeOwned,
{
    fn offers(&self) -> Vec<Vec<Offer>> {
        if let Some(o) = get_offer::<N>() {
            vec![vec![o]]
        } else {
            Default::default()
        }
    }
}

/// Trait to monomorphize a CxList with possible `Select`s into something that impls Serve
pub trait Pick {
    type Picked;
    fn pick(
        self,
        client_offers: Vec<Vec<Offer>>,
    ) -> Result<(Self::Picked, Option<Vec<Offer>>, Vec<Vec<Offer>>), Report>;
}

impl<N> Pick for N
where
    N: Negotiate,
{
    type Picked = Self;
    fn pick(
        self,
        client_offers: Vec<Vec<Offer>>,
    ) -> Result<(Self::Picked, Option<Vec<Offer>>, Vec<Vec<Offer>>), Report> {
        if N::Capability::guid() == 0 {
            Ok((self, None, client_offers))
        } else {
            let mut o = client_offers.into_iter();
            Ok((self, o.next(), o.collect()))
        }
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
        client_offers: Vec<Vec<Offer>>,
    ) -> Result<(Self::Picked, Option<Vec<Offer>>, Vec<Vec<Offer>>), Report> {
        let (head_pick, cl_pick, client_offers) = self.head.pick(client_offers)?;
        let (tail_pick, rest_cl_pick, client_offers) = self.tail.pick(client_offers)?;
        let pick = match (cl_pick, rest_cl_pick) {
            (None, None) => None,
            (Some(a), None) | (None, Some(a)) => Some(a),
            (Some(mut a), Some(b)) => {
                a.extend(b);
                Some(a)
            }
        };
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

/// Negotiation type to choose between `T1` and `T2`.
#[derive(Clone)]
pub struct Select<T1, T2>(pub T1, pub T2);

impl<T1, T2> GetOffers for Select<T1, T2>
where
    T1: GetOffers,
    T2: GetOffers,
{
    fn offers(&self) -> Vec<Vec<Offer>> {
        // TODO won't work with nested select
        let t1 = self.0.offers();
        let t2 = self.1.offers();
        if t1.is_empty() {
            t2
        } else {
            let mut t1 = t1[0].clone();
            if !t2.is_empty() {
                let t2 = t2[0].clone();
                t1.extend(t2);
            }

            vec![t1]
        }
    }
}

//fn check_offers(offers: &[Vec<Offer>]) -> Result<(), Report> {
//    if !offers.iter().all(|l| {
//        let id = l[0].capability_guid;
//        l.iter().all(|o| o.capability_guid == id)
//    }) {
//        return Err(eyre!("Capability guid mismatch")).wrap_err_with(|| {
//            let mut idx = 0;
//            let mut ok = true;
//            for offer_set in offers.iter() {
//                let id = offer_set[0].capability_guid;
//                for o in offer_set.iter() {
//                    if o.capability_guid != id {
//                        ok = false;
//                        break;
//                    }
//                }
//
//                if !ok {
//                    return eyre!("layer {}: {:?}", idx, offer_set);
//                }
//
//                idx += 1;
//            }
//
//            unreachable!()
//        });
//    }
//
//    Ok(())
//}

fn lacking<T: PartialEq>(a: &[T], univ: Vec<T>) -> Vec<T> {
    univ.into_iter().filter(|x| !a.contains(x)).collect()
}

impl<T1, T2, C> Pick for Select<T1, T2>
where
    T1: Negotiate<Capability = C>,
    T2: Negotiate<Capability = C>,
    C: CapabilitySet + Serialize + DeserializeOwned + Clone,
{
    type Picked = Either<T1, T2>;
    fn pick(
        self,
        client_offers: Vec<Vec<Offer>>,
    ) -> Result<(Self::Picked, Option<Vec<Offer>>, Vec<Vec<Offer>>), Report> {
        if C::guid() == 0 {
            return Ok((Either::Left(self.0), None, client_offers));
        }

        if client_offers.is_empty() {
            return Err(eyre!("Not enough offers for stack"));
        }

        let mut offers = client_offers.into_iter();
        let offer = offers.next().unwrap();
        if offer[0].capability_guid != C::guid() {
            return Err(eyre!("Capability type mismatch"));
        }

        debug!(
            cap_type = std::any::type_name::<C>(),
            offer = ?&offer,
            "deserializing offers"
        );

        let caps: Result<Vec<Vec<C>>, Report> = offer
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

        for caps_co in caps.iter() {
            if t1.len() <= t2.len() {
                let mut co: Vec<C> = caps_co.clone();
                co.extend_from_slice(&t1);
                if lacking(&co, C::universe()).is_empty() {
                    return Ok((
                        Either::Left(self.0),
                        Some(vec![caps_co.clone().into()]),
                        offers.collect(),
                    ));
                }

                let mut co = caps_co.clone();
                co.extend_from_slice(&t2);
                if lacking(&co, C::universe()).is_empty() {
                    return Ok((
                        Either::Right(self.1),
                        Some(vec![caps_co.clone().into()]),
                        offers.collect(),
                    ));
                }
            } else {
                let mut co = caps_co.clone();
                co.extend_from_slice(&t2);
                if lacking(&co, C::universe()).is_empty() {
                    return Ok((
                        Either::Right(self.1),
                        Some(vec![caps_co.clone().into()]),
                        offers.collect(),
                    ));
                }

                let mut co = caps_co.clone();
                co.extend_from_slice(&t1);
                if lacking(&co, C::universe()).is_empty() {
                    return Ok((
                        Either::Left(self.0),
                        Some(vec![caps_co.clone().into()]),
                        offers.collect(),
                    ));
                }
            }
        }

        Err(eyre!(
            "Could not find satisfying client/server capability set for {:?}",
            std::any::type_name::<C>()
        ))
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
    ClientOffer(Vec<Vec<Offer>>),
    ServerReply(Vec<Offer>),
    ServerNonce { addr: Vec<u8>, picked: Vec<Offer> },
    ServerNonceAck,
}

type ServeInput<C, A> = Once<Ready<Result<InjectWithChannel<C, (A, Vec<u8>)>, Report>>>;
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
                        <<Srv as Pick>::Picked as Serve<ServeInput<C, A>>>::Connection,
                        <<Srv as Apply>::Applied as Serve<ServeInput<C, A>>>::Connection,
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
    Srv: Pick + Apply + GetOffers + Clone + Send + 'static,
    // main-line branch: Pick on incoming negotiation handshake.
    <Srv as Pick>::Picked: NegotiatePicked + Serve<ServeInput<C, A>> + Clone + Send + 'static,
    <<Srv as Pick>::Picked as Serve<ServeInput<C, A>>>::Connection: Send + Sync + 'static,
    <<Srv as Pick>::Picked as Serve<ServeInput<C, A>>>::Error: Into<Report> + Send + Sync + 'static,
    <<Srv as Pick>::Picked as Serve<ServeInput<C, A>>>::Stream: Unpin + Send + 'static,
    // nonce branch: Apply stack from nonce on indicated connections.
    <Srv as Apply>::Applied: Serve<ServeInput<C, A>> + Clone + Send + 'static,
    <<Srv as Apply>::Applied as Serve<ServeInput<C, A>>>::Connection: Send + Sync + 'static,
    <<Srv as Apply>::Applied as Serve<ServeInput<C, A>>>::Error:
        Into<Report> + Send + Sync + 'static,
    <<Srv as Apply>::Applied as Serve<ServeInput<C, A>>>::Stream: Unpin + Send + 'static,
    A: Serialize
        + DeserializeOwned
        + Eq
        + std::hash::Hash
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
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
                debug!("new connection");
                let (cn, s) = InjectWithChannel::make(cn);
                let stack = stack.clone();
                let pending_negotiated_connections = Arc::clone(&pending_negotiated_connections);
                async move {
                    // 2. on new connection, read off Vec<Vec<Offer>> from
                    //    client
                    let (a, buf): (_, Vec<u8>) = cn.recv().await?;
                    trace!("got offer pkt");

                    // if `a` is in pending_negotiated_connections, this is a post-negotiation
                    // message and we should return the applied connection.
                    let opt_picked = {
                        let guard = pending_negotiated_connections.lock().unwrap();
                        guard.get(&a).map(Clone::clone)
                    };
                    if let Some(picked) = opt_picked {
                        let ofs = stack.offers();
                        let p = picked.clone();
                        let (mut stack, _) = stack
                            .apply(picked)
                            .wrap_err("failed to apply semantics to client connection")
                            .note(format!("tried to apply: {:?}", p))
                            .note(format!("onto the stack: {:?}", ofs))?;
                        let cn_st = futures_util::stream::once(futures_util::future::ready(Ok(cn)));
                        let mut new_st = stack.serve(cn_st).await.map_err(Into::into)?;
                        let new_cn = new_st
                            .try_next()
                            .await // -> Result<Option<T>, E>
                            .map_err(Into::into)?
                            .ok_or_else(|| eyre!("No connection returned"))?;

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
                            let addr: A =
                                bincode::deserialize(&addr).wrap_err("mismatched addr types")?;
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
                            process_nonces_connection(
                                cn,
                                Arc::clone(&pending_negotiated_connections),
                            )
                            .await?;
                            unreachable!();
                        }
                        ClientOffer(client_offers) => {
                            debug!(client_offers = ?&client_offers, from = ?&a, "received offer");

                            //if let Err(e) = check_offers(&client_offers) {
                            //    warn!(err = ?e, "Received invalid offer set from client");
                            //    // TODO send error response
                            //    unimplemented!();
                            //}

                            // 3. monomorphize: transform the CxList<impl Serve/Select<impl Serve,
                            //    impl Serve>> into a CxList<impl Serve>
                            let (mut new_stack, picked_offers, leftover) = stack
                                .pick(client_offers)
                                .wrap_err(eyre!("error monomorphizing stack",))?;
                            if !leftover.is_empty() {
                                return Err(eyre!("Unmatched client offers"));
                            }

                            let picked_offers = picked_offers.unwrap_or_default();
                            debug!(picked_client_offers = ?&picked_offers, "monomorphized stack");
                            // tell all the stack elements about the nonce = (client addr, chosen stack)
                            let nonce = NegotiateMsg::ServerNonce {
                                addr: bincode::serialize(&a)?,
                                picked: picked_offers.clone(),
                            };
                            let nonce_buf = bincode::serialize(&nonce)
                                .wrap_err("Failed to serialize (addr, chosen_stack) nonce")?;
                            new_stack
                                .call_negotiate_picked(&nonce_buf)
                                .instrument(debug_span!("call_negotiate_picked"))
                                .await;

                            // 4. Respond to client with offer choice
                            let buf = bincode::serialize(&picked_offers)?;
                            cn.send((a, buf)).await?;

                            debug!("negotiation handshake done");

                            // 5. new_stack.serve(vec_u8_stream)
                            let cn_st =
                                futures_util::stream::once(futures_util::future::ready(Ok(cn)));
                            let mut new_st = new_stack.serve(cn_st).await.map_err(Into::into)?;
                            let new_cn = new_st
                                .try_next()
                                .await // -> Result<Option<T>, E>
                                .map_err(Into::into)?
                                .ok_or_else(|| eyre!("No connection returned"))?;

                            debug!("returning connection");
                            Ok(Some(Either::Left(new_cn)))
                        }
                        _ => unreachable!(),
                    }
                }
            })
            .try_filter_map(|v| futures_util::future::ready(Ok(v))))
    }
}

#[instrument(level = "debug", skip(cn, pending_negotiated_connections))]
async fn process_nonces_connection<A>(
    cn: impl ChunnelConnection<Data = (A, Vec<u8>)>,
    pending_negotiated_connections: Arc<Mutex<HashMap<A, Vec<Offer>>>>,
) -> Result<(), Report>
where
    A: Serialize
        + DeserializeOwned
        + Eq
        + std::hash::Hash
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
{
    loop {
        trace!("call recv()");
        let (a, buf): (_, Vec<u8>) = cn.recv().await?;
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

pub trait Apply {
    type Applied;
    fn apply(self, offers: Vec<Offer>) -> Result<(Self::Applied, Option<Vec<Offer>>), Report>;
}

impl<N> Apply for N
where
    N: Negotiate,
    <N as Negotiate>::Capability: DeserializeOwned + Ord,
{
    type Applied = Self;
    fn apply(self, o: Vec<Offer>) -> Result<(Self::Applied, Option<Vec<Offer>>), Report> {
        if N::Capability::guid() == 0 {
            return Ok((self, Some(o)));
        }

        if o.is_empty() {
            return Err(eyre!("Not enough offers for stack"));
        }

        let o = o.into_iter().next().unwrap();
        if o.capability_guid != N::Capability::guid() {
            return Err(eyre!(
                "Capability guid mismatch: offer={}, this={}",
                o.capability_guid,
                N::Capability::guid()
            ));
        }

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

        Ok((self, None))
    }
}

impl<H, T> Apply for CxList<H, T>
where
    H: Apply,
    T: Apply,
{
    type Applied = CxList<H::Applied, T::Applied>;
    fn apply(self, o: Vec<Offer>) -> Result<(Self::Applied, Option<Vec<Offer>>), Report> {
        let mut offers_iter = o.into_iter();
        let (head_pick, returned) = self
            .head
            .apply(offers_iter.next().map(|x| vec![x]).unwrap_or_default())?;

        let offers_iter = returned.into_iter().flatten().chain(offers_iter);
        let (tail_pick, returned) = self.tail.apply(offers_iter.collect())?;
        Ok((
            CxList {
                head: head_pick,
                tail: tail_pick,
            },
            returned,
        ))
    }
}

impl<T1, T2> Apply for Select<T1, T2>
where
    T1: Apply,
    T2: Apply,
{
    type Applied = Either<<T1 as Apply>::Applied, <T2 as Apply>::Applied>;
    fn apply(self, offers: Vec<Offer>) -> Result<(Self::Applied, Option<Vec<Offer>>), Report> {
        match self.0.apply(offers.clone()) {
            Ok((t1_applied, r)) => Ok((Either::Left(t1_applied), r)),
            Err(e) => {
                debug!(t1 = std::any::type_name::<T1>(), err = ?e, "Select::T1 mismatched");
                let (t2_applied, r) = self.1.apply(offers)?;
                Ok((Either::Right(t2_applied), r))
            }
        }
    }
}

/// Return a connection with `stack`'s semantics, connecting to `a`.
pub fn negotiate_client<C, A, S>(
    stack: S,
    cn: C,
    addr: A,
) -> impl Future<Output = Result<<<S as Apply>::Applied as Client<C>>::Connection, Report>>
       + Send
       + 'static
where
    C: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync + 'static,
    S: Apply + GetOffers + Clone + Send + 'static,
    <S as Apply>::Applied: Client<C> + Clone + std::fmt::Debug + Send + 'static,
    <<S as Apply>::Applied as Client<C>>::Error: Into<Report> + Send + Sync + 'static,
    A: Send + Sync + 'static,
{
    async move {
        // 1. get Vec<u8> connection.
        debug!("got negotiation connection");

        // 2. send Vec<Vec<Offer>>
        let offers = NegotiateMsg::ClientOffer(stack.offers());
        let buf = bincode::serialize(&offers)?;
        debug!(offers = ?&offers, "sending offers");
        cn.send((addr, buf)).await?;

        // 3. receive Vec<Offer>
        let (_, buf) = cn.recv().await?;
        let picked: Vec<Offer> = bincode::deserialize(&buf)?;
        debug!(picked = ?&picked, "received picked impls");

        // 4. monomorphize `stack`, picking received choices
        let p = picked.clone();
        let s = stack.clone();
        let (mut new_stack, leftover) = stack.apply(picked).wrap_err(eyre!(
            "Could not apply received impls to stack: picked = {:?}, stack = {:?}",
            &p,
            &s.offers()
        ))?;
        match leftover {
            Some(x) if x.is_empty() => (),
            _ => {
                return Err(eyre!("Did not use all picked offers"));
            }
        }

        debug!(applied = ?&new_stack, "applied to stack");

        // 5. return new_stack.connect_wrap(vec_u8_conn)
        new_stack.connect_wrap(cn).await.map_err(Into::into)
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
        Serve,
    };
    use color_eyre::eyre::{eyre, Report, WrapErr};
    use futures_util::{
        future::{ready, Ready},
        stream::{Stream, StreamExt},
    };
    use tracing::{debug, debug_span, info, info_span};
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[allow(non_upper_case_globals)]
    macro_rules! mock_serve_impl {
        ($name:ident) => {
            paste::paste! {
                lazy_static::lazy_static! {
                    static ref [<$name CapGuid>]: u64 = rand::random();
                }

                enumerate_enum!([<$name Cap>], *[<$name CapGuid>], A, B, C);
            }

            #[derive(Debug, Clone, Copy)]
            struct $name;

            impl<D, InS, InC, InE> Serve<InS> for $name
            where
                InS: Stream<Item = Result<InC, InE>> + Send + 'static,
                InC: ChunnelConnection<Data = D> + Send + Sync + 'static,
                InE: Send + Sync + 'static,
                D: Send + Sync + 'static,
            {
                type Future = Ready<Result<Self::Stream, Self::Error>>;
                type Connection = InC;
                type Error = InE;
                type Stream = InS;

                fn serve(&mut self, inner: InS) -> Self::Future {
                    ready(Ok(inner))
                }
            }

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

            paste::paste! {
            impl Negotiate for $name {
                type Capability = [<$name Cap>];
                fn capabilities() -> Vec<Self::Capability> {
                    [<$name Cap>]::universe()
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

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
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
                let offers = NegotiateMsg::ClientOffer(stack.offers());
                debug!(offers = ?&offers, "starting negotiation handshake");
                let buf = bincode::serialize(&offers)?;
                cn.send(((), buf)).await?;

                let (_, resp) = cn.recv().await?;
                let resp: Vec<Offer> = bincode::deserialize(&resp)?;
                debug!(resp = ?&resp, "got negotiation response");

                let expected: Vec<Offer> = match offers {
                    NegotiateMsg::ClientOffer(os) => os
                        .into_iter()
                        .map(|o| o.into_iter().next().unwrap())
                        .collect(),
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
            #[derive(Debug, Clone, Copy)]
            struct [< $name Alt >];

            impl<D, InS, InC, InE> Serve<InS> for [< $name Alt >]
            where
                InS: Stream<Item = Result<InC, InE>> + Send + 'static,
                InC: ChunnelConnection<Data = D> + Send + Sync + 'static,
                InE: Send + Sync + 'static,
                D: Send + Sync + 'static,
            {
                type Future = Ready<Result<Self::Stream, Self::Error>>;
                type Connection = InC;
                type Error = InE;
                type Stream = InS;

                fn serve(&mut self, inner: InS) -> Self::Future {
                    ready(Ok(inner))
                }
            }

            impl<D, InC> Client<InC> for [< $name Alt >]
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

            impl Negotiate for [< $name Alt >] {
                type Capability = [<$name Cap>];
                fn capabilities() -> Vec<Self::Capability> {
                    [<$name Cap>]::universe()
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
        let offers = stack.offers();
        info!(offers = ?&offers, "select_offers");

        let stack1 = CxList::from(ChunnelA).wrap(ChunnelB).wrap(ChunnelC);
        let offers1 = stack1.offers();
        info!(offers = ?&offers1, "no_select_offers");

        assert_eq!(offers.len(), offers1.len());
    }

    #[test]
    fn serve_select() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or_else(|_| ());

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
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
                        let srv_stream = negotiate_server(srv_stack, raw_st)
                            .await
                            .wrap_err("negotiate_server failed")?;
                        s.send(()).unwrap();
                        // shadow the original variable so it can't accidentally be used after the pin,
                        // making this safe
                        //let mut srv_stream =
                        //    unsafe { std::pin::Pin::new_unchecked(&mut srv_stream) };
                        tokio::pin!(srv_stream);
                        //let mut srv_stream = Box::pin(srv_stream);
                        srv_stream
                            .next()
                            .await
                            .ok_or_else(|| eyre!("srv_stream returned none"))?
                            .wrap_err("negotiation failed")?;
                        info!("done");
                        Ok::<_, Report>(())
                    }
                    .instrument(debug_span!("server")),
                );

                r.await.unwrap();

                // make a Vec<u8> client
                let cn = cln.connect(()).await?;

                // send the raw Vec<Vec<Offer>>
                // [ [Offer{guid: A, vec:[]}], [Offer{guid: B, vec:[]}], [Offer{guid: C, vec:[]}] ]
                let offers = NegotiateMsg::ClientOffer(stack.offers());
                info!(offers = ?&offers, "starting negotiation handshake");
                let buf = bincode::serialize(&offers)?;
                cn.send(((), buf)).await?;

                let (_, resp) = cn.recv().await?;
                let resp: Vec<Offer> = bincode::deserialize(&resp)?;
                info!(resp = ?&resp, "got negotiation response");

                let expected: Vec<Offer> = match offers {
                    NegotiateMsg::ClientOffer(os) => os
                        .into_iter()
                        .map(|o| o.into_iter().next().unwrap())
                        .collect(),
                    _ => unreachable!(),
                };

                assert_eq!(resp, expected);
                info!("done");
                Ok::<_, Report>(())
            }
            .instrument(info_span!("serve_select")),
        )
        .unwrap();
    }

    #[test]
    fn client_select() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or_else(|_| ());

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
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

                let (s, r) = tokio::sync::oneshot::channel();
                tokio::spawn(
                    async move {
                        info!("starting");
                        let mut st = srv.listen(()).await?;
                        s.send(()).unwrap();

                        let cn = st.next().await.unwrap()?;
                        let (_, buf) = cn.recv().await?;
                        let offers = bincode::deserialize(&buf)?;

                        // Pick something fake, the first idx for each of them
                        let picked: Vec<Offer> = match offers {
                            NegotiateMsg::ClientOffer(os) => os
                                .into_iter()
                                .map(|o| o.into_iter().next().unwrap())
                                .collect(),
                            _ => unreachable!(),
                        };

                        let buf = bincode::serialize(&picked)?;
                        cn.send(((), buf)).await?;

                        Ok::<_, Report>(())
                    }
                    .instrument(debug_span!("server")),
                );

                r.await.unwrap();

                let raw_cn = cln.connect(()).await?;
                let _cn = negotiate_client(stack, raw_cn, ())
                    .instrument(info_span!("negotiate_client"))
                    .await?;

                info!("done");

                Ok::<_, Report>(())
            }
            .instrument(info_span!("client_select")),
        )
        .unwrap();
    }

    #[test]
    fn both_select() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        color_eyre::install().unwrap_or_else(|_| ());

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
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

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
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
}
