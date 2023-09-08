//! Chunnel wrapper types to negotiate between multiple implementations.

use crate::{CxNil, Either};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;

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
    // TODO pass the peer address.
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

/// Negotiation type to choose between `T1` and `T2`.
#[derive(Clone, Debug)]
pub struct Select<T1, T2, Inner = Either<T1, T2>> {
    pub left: T1,
    pub right: T2,
    pub prefer: Either<(), ()>,
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

pub(crate) fn have_all(univ: &[Vec<u8>], joint: &[Vec<u8>]) -> bool {
    univ.iter().all(|x| joint.contains(x))
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct StackNonce(HashMap<u64, Offer>);

impl StackNonce {
    #[doc(hidden)]
    pub fn __from_inner(h: HashMap<u64, Offer>) -> StackNonce {
        StackNonce(h)
    }

    #[doc(hidden)]
    pub fn __into_inner(self) -> HashMap<u64, Offer> {
        self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NegotiateMsg {
    /// A list of stacks the client supports. Response from the server is a `ServerReply`.
    ClientOffer(Vec<StackNonce>),
    /// Can mean one of two things.
    /// 1. In response to a `ClientOffer`, a list of *client* stacks (a subset of those passed in
    ///    the `ClientOffer`) the server supports, given the one the server has chosen.
    /// 2. In response to a `ClientNonce`, a list of *server* stacks the server supports. In this
    ///    case the client can monomorphize to a working stack and try zero-rtt with `ClientNonce`
    ///    again, so the worst case is still one-rtt (the one that returned this `ServerReply`)
    ServerReply(Result<Vec<StackNonce>, String>),
    /// A specific stack the server should use on the given address.
    ServerNonce {
        addr: Vec<u8>,
        picked: StackNonce,
    },
    /// A nonce representing the stack the client wants to use.
    /// If it works, server sends a `ServerNonceAck`. Otherwise, a `ServerReply`
    ClientNonce(StackNonce),
    ServerNonceAck,
}

mod inject_with_channel;
pub use inject_with_channel::InjectWithChannel;
mod negotiate_picked;
pub use negotiate_picked::NegotiatePicked;
mod get_offers;
pub use get_offers::{GetOffers, Offer};
mod pick;
pub use pick::Pick;
use pick::PickResult;
mod apply;
pub use apply::check_apply;
pub use apply::Apply;
pub use apply::ApplyResult;
mod server;
pub use server::monomorphize;
pub use server::negotiate_server;
pub use server::negotiate_server_shared_state;
mod client;
pub use client::{
    negotiate_client, negotiate_client_fixed_stack, negotiate_client_nonce, ClientNegotiator,
    NegotiatedConn,
};
mod rendezvous;
pub use rendezvous::{
    negotiate_rendezvous, NegotiateRendezvousResult, RendezvousBackend, RendezvousEntry,
    StackUpgradeHandle, UpgradeEitherApply, UpgradeEitherConn, UpgradeHandle, UpgradeSelect,
};

#[allow(non_upper_case_globals)]
#[cfg(test)]
mod test {
    use super::{negotiate_client, negotiate_server, CapabilitySet, Negotiate, Select};
    use crate::test::COLOR_EYRE;
    use crate::udp::{UdpReqChunnel, UdpSkChunnel};
    use crate::{
        chan_transport::Chan, Chunnel, ChunnelConnection, ChunnelConnector, ChunnelListener, CxList,
    };
    use crate::{monomorphize, GetOffers};
    use color_eyre::eyre::{eyre, Report};
    use futures_util::TryStreamExt;
    use futures_util::{
        future::{ready, Ready},
        stream::StreamExt,
    };
    use std::net::ToSocketAddrs;
    use std::net::{Ipv4Addr, SocketAddr};
    use tracing::{debug, debug_span, info, info_span};
    use tracing_error::ErrorLayer;
    use tracing_futures::Instrument;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[allow(non_upper_case_globals)]
    #[macro_export]
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
    #[macro_export]
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
    fn stack_associativity() {
        let stack1 = Select::from((
            CxList::from(ChunnelB).wrap(ChunnelA),
            CxList::from(ChunnelC).wrap(ChunnelA),
        ));

        let stack2 = CxList::from(Select::from((ChunnelB, ChunnelC))).wrap(ChunnelA);

        let stack1_offers: Vec<_> = stack1.offers().collect();
        let stack2_offers: Vec<_> = stack2.offers().collect();

        assert_eq!(stack1_offers, stack2_offers);

        let a = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0);
        let _p = monomorphize(stack1.clone(), stack2_offers.clone(), &a)
            .expect("monomorphize Sel(A|>B, A|>C) with A|>Sel(B,C) offers");
        let _p = monomorphize(stack2.clone(), stack1_offers.clone(), &a)
            .expect("monomorphize A|>Sel(B,C) with Sel(A|>B, A|>C) offers");

        let left_only = CxList::from(ChunnelB).wrap(ChunnelA);
        let left_only_offers: Vec<_> = left_only.offers().collect();
        let _p = monomorphize(stack1.clone(), left_only_offers.clone(), &a)
            .expect("monomorphize Sel(A|>B, A|>C) with A|>B offers");
        let _p = monomorphize(stack2.clone(), left_only_offers.clone(), &a)
            .expect("monomorphize A|>Sel(B,C) with A|>B offers");

        let right_only = CxList::from(ChunnelC).wrap(ChunnelA);
        let right_only_offers: Vec<_> = right_only.offers().collect();
        let _p = monomorphize(stack1.clone(), right_only_offers.clone(), &a)
            .expect("monomorphize Sel(A|>B, A|>C) with A|>C offers");
        let _p = monomorphize(stack2.clone(), right_only_offers.clone(), &a)
            .expect("monomorphize A|>Sel(B,C) with A|>C offers");
    }

    #[test]
    fn both_select() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        COLOR_EYRE.call_once(|| color_eyre::install().unwrap_or(()));

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
                        let mut slot = [None];
                        let ms = cn.recv(&mut slot).await?;
                        let buf = ms[0].take().take().ok_or_else(|| eyre!("no message"))?;
                        cn.send(std::iter::once(buf)).await?;
                        Ok::<_, Report>(())
                    }
                    .instrument(debug_span!("server")),
                );

                r.await.unwrap();

                let raw_cn = cln.connect(()).await?;
                let cn = negotiate_client(stack, raw_cn, ())
                    .instrument(info_span!("negotiate_client"))
                    .await?;

                cn.send(std::iter::once(((), vec![1u8; 10]))).await?;
                let mut slot = [None];
                let ms = cn.recv(&mut slot).await?;
                let (_, buf) = ms[0].take().take().ok_or_else(|| eyre!("no message"))?;
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
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        COLOR_EYRE.call_once(|| color_eyre::install().unwrap_or(()));

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
                        let raw_st = UdpReqChunnel.listen(addr).await?;
                        let srv_stream = negotiate_server(srv_stack, raw_st).await?;
                        s.send(()).unwrap();
                        srv_stream
                            .try_for_each_concurrent(None, |cn| async move {
                                info!("got connection");
                                const EMPTY: Option<(SocketAddr, Vec<u8>)> = None;
                                let mut slots = [EMPTY; 4];
                                loop {
                                    let ms = cn.recv(&mut slots).await?;
                                    cn.send(ms.iter_mut().map_while(Option::take)).await?;
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
                let raw_cn = UdpSkChunnel.connect(addr).await?;
                let cn1 = negotiate_client(stack.clone(), raw_cn, addr)
                    .instrument(info_span!("negotiate_client"))
                    .await?;

                let raw_cn = UdpSkChunnel.connect(addr).await?;
                let cn2 = negotiate_client(stack, raw_cn, addr)
                    .instrument(info_span!("negotiate_client"))
                    .await?;

                for _ in 0..10 {
                    debug!("sending");
                    cn1.send(std::iter::once((addr, vec![1u8; 10]))).await?;
                    cn2.send(std::iter::once((addr, vec![2u8; 10]))).await?;
                    let mut slots = [None];
                    let ms = cn1.recv(&mut slots).await?;
                    let (_, buf1) = ms[0].take().expect("no message");
                    assert_eq!(buf1, vec![1u8; 10]);

                    let ms = cn2.recv(&mut slots).await?;
                    let (_, buf2) = ms[0].take().expect("no message");
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
    #[macro_export]
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
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        COLOR_EYRE.call_once(|| color_eyre::install().unwrap_or(()));

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
                        let raw_st = UdpReqChunnel.listen(addr).await?;
                        let srv_stream = negotiate_server(srv_stack, raw_st).await?;
                        s.send(()).unwrap();
                        srv_stream
                            .try_for_each_concurrent(None, |cn| async move {
                                info!("got connection");
                                const EMPTY: Option<(SocketAddr, Vec<u8>)> = None;
                                let mut slots = [EMPTY; 4];
                                loop {
                                    let ms = cn.recv(&mut slots).await?;
                                    cn.send(ms.iter_mut().map_while(Option::take)).await?;
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
                let cl_stack = ChunnelA;
                let raw_cn = UdpSkChunnel.connect(addr).await?;
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

    #[test]
    fn zero_rtt() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        COLOR_EYRE.call_once(|| color_eyre::install().unwrap_or(()));
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
                let addr = "127.0.0.1:52185".to_socket_addrs().unwrap().next().unwrap();
                let (s, r) = tokio::sync::oneshot::channel();
                tokio::spawn(
                    async move {
                        info!("starting");
                        let raw_st = UdpReqChunnel.listen(addr).await?;
                        let srv_stream = negotiate_server(srv_stack, raw_st).await?;
                        s.send(()).unwrap();
                        srv_stream
                            .try_for_each_concurrent(None, |cn| async move {
                                info!("got connection");
                                const EMPTY: Option<(SocketAddr, Vec<u8>)> = None;
                                let mut slots = [EMPTY; 4];
                                loop {
                                    let ms = cn.recv(&mut slots).await?;
                                    cn.send(ms.iter_mut().map_while(Option::take)).await?;
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
                let mut cl_neg = super::ClientNegotiator::default();
                async {
                    info!("starting client");
                    let raw_cn = UdpSkChunnel.connect(addr).await?;
                    let cn = cl_neg
                        .negotiate_fetch_nonce(stack.clone(), raw_cn, addr)
                        .instrument(info_span!("negotiate_client"))
                        .await?;

                    info!("sending");
                    cn.send(std::iter::once((addr, vec![1u8; 8]))).await?;
                    let mut slot = [None];
                    let b = cn.recv(&mut slot).await?;
                    let (a, r) = b[0].take().expect("no message received");
                    info!("received");
                    assert_eq!(a, addr, "Address mismatched");
                    assert_eq!(r, [1u8; 8], "Payload mismatched");
                    Ok::<_, Report>(())
                }
                .instrument(info_span!("normal client"))
                .await?;

                async {
                    info!("starting client");
                    let raw_cn = UdpSkChunnel.connect(addr).await?;
                    let cn = cl_neg
                        .negotiate_zero_rtt(stack.clone(), raw_cn, addr)
                        .instrument(info_span!("negotiate_client"))
                        .await?;

                    info!("sending");
                    cn.send(std::iter::once((addr, vec![2u8; 8]))).await?;
                    let mut slot = [None];
                    let b = cn.recv(&mut slot).await?;
                    let (a, r) = b[0].take().expect("no message received");
                    info!("recvd");
                    assert_eq!(a, addr, "Address mismatched");
                    assert_eq!(r, [2u8; 8], "Payload mismatched");
                    Ok::<_, Report>(())
                }
                .instrument(info_span!("zero-rtt client"))
                .await?;

                async {
                    use super::GetOffers;
                    // make a fake bad nonce
                    let cl_stack = ChunnelA;
                    let bad_nonce = cl_stack.offers().next().unwrap();
                    let raw_cn = UdpSkChunnel.connect(addr).await?;
                    let cn = super::client::negotiate_client_nonce(
                        cl_stack,
                        raw_cn.clone(),
                        bad_nonce,
                        addr,
                    )
                    .await?;
                    info!(attempt = 1, "sending");
                    cn.send(std::iter::once((addr, vec![3u8; 8]))).await?;

                    let mut slot = [None];
                    let e = cn.recv(&mut slot).await.unwrap_err();
                    info!("re-negotiate");
                    let cn = cl_neg.re_negotiate(stack.clone(), raw_cn, addr, e).await?;

                    info!(attempt = 2, "sending");
                    cn.send(std::iter::once((addr, vec![3u8; 8]))).await?;
                    let b = cn.recv(&mut slot).await?;
                    let (a, r) = b[0].take().expect("no message received");
                    info!(attempt = 2, "recvd");
                    assert_eq!(a, addr, "Address mismatched");
                    assert_eq!(r, [3u8; 8], "Payload mismatched");
                    Ok::<_, Report>(())
                }
                .instrument(info_span!("zero-rtt retry client"))
                .await?;

                Ok::<_, Report>(())
            }
            .instrument(info_span!("negotiate::zero_rtt")),
        )
        .unwrap();
    }
}
