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

fn have_all(univ: &[Vec<u8>], joint: &[Vec<u8>]) -> bool {
    univ.iter().all(|x| joint.contains(x))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StackNonce(HashMap<u64, Offer>);

impl StackNonce {
    #[doc(hidden)]
    pub fn __from_inner(h: HashMap<u64, Offer>) -> StackNonce {
        StackNonce(h)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NegotiateMsg {
    /// A list of stacks the client supports
    ClientOffer(Vec<StackNonce>),
    /// A list of stacks the server supports
    ServerReply(Result<Vec<StackNonce>, String>),
    /// A specific stack the server should use on the given address.
    ServerNonce {
        addr: Vec<u8>,
        picked: StackNonce,
    },
    ServerNonceAck,
}

mod inject_with_channel;
mod negotiate_picked;
pub use negotiate_picked::NegotiatePicked;
mod get_offers;
pub use get_offers::{GetOffers, Offer};
mod pick;
pub use pick::Pick;
use pick::PickResult;
mod apply;
pub use apply::Apply;
use apply::ApplyResult;
mod server;
use server::monomorphize;
pub use server::negotiate_server;
mod client;
pub use client::{negotiate_client, NegotiatedConn};
mod rendezvous;
pub use rendezvous::{
    negotiate_rendezvous, NegotiateRendezvousResult, RendezvousBackend, RendezvousEntry,
    SelectPolicy, UpgradeSelect,
};

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
        color_eyre::install().unwrap_or(());

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
        color_eyre::install().unwrap_or(());

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
        color_eyre::install().unwrap_or(());

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
