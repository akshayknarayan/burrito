//! Chunnel wrapper types to negotiate between multiple implementations.

use crate::{
    Address, ChunnelConnection, ChunnelConnector, ChunnelListener, Either, Endedness, Scope,
};
use eyre::{eyre, Report};
use futures_util::stream::Stream;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::convert::TryInto;
use std::future::Future;
use std::pin::Pin;
use tracing::debug;

// remote negotiation
// goal: need to pass a message describing what functionality goes where.
//
// "one-way handshake"
// client, in connect(), offers a set of functionality known to the chunnel
// server, in listen(), picks the right type out of {T1, T2, ..., Tn} given what the client said.
//   - no need to respond because the client has already sent what it is doing.
//
// problems:
//   - how to deal with arbitrary chunnel data types?
//    - impl Into<C::Connection::Data>? "bring your own serialization"
//
// solutions:
//   - how do we know what this functionality is?
//     - chunnels describe via trait method implementation a type for the functionality set (Vec of
//   enum)?

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

pub trait NegotiateDummy {}

/// Define an enum that implements the `CapabilitySet` trait.
///
/// Invoke with enum name (with optional `pub`) followed by variant names.
///
/// # Example
/// ```rust
/// # use bertha::enumerate_enum;
/// enumerate_enum!(pub Foo, A, B, C);
/// enumerate_enum!(Bar, A, B, C);
/// fn main() {
///     let f = Foo::B;
///     let b = Bar::C;
///     println!("{:?}, {:?}", f, b);
/// }
/// ```
#[macro_export]
macro_rules! enumerate_enum {
    (pub $name:ident, $($variant:ident),+) => {
        #[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
        pub enum $name {
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
                0xe898734df758d0c0 // TODO eventually we'll have an enum type that is not ShardFns
            }
        }

        impl $crate::negotiate::NegotiateDummy for $name {}
    };
    ($(keyw:ident)* $name:ident, $($variant:ident),+) => {
        #[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
        enum $name {
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
                0xe898734df758d0c0
            }
        }

        impl $crate::negotiate::NegotiateDummy for $name {}
    };
}

/// Expresses the ability to negotiate chunnel implementations over a set of capabilities enumerated
/// by the `Capability` type.
///
/// Read: `Negotiate` *over* `Capability`.
///
/// TODO Add endedness to this trait: onesided_capabilities vs bothsided_capabilities
pub trait Negotiate<Capability: CapabilitySet> {
    fn capabilities() -> Vec<Capability> {
        vec![]
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Offer {
    capability_guid: u64,
    available: Vec<u8>,
}

impl<T> Negotiate<()> for T {
    fn capabilities() -> Vec<()> {
        vec![()]
    }
}

pub struct Select<T1, T2, A, D, N>(pub T1, pub T2, std::marker::PhantomData<(A, D, N)>);

impl<T1, T2, A, D, N> Select<T1, T2, A, D, N> {
    pub fn new(t1: T1, t2: T2) -> Self {
        Select(t1, t2, Default::default())
    }
}

impl<T1, T2, A, D, N> From<(T1, T2)> for Select<T1, T2, A, D, N> {
    fn from(f: (T1, T2)) -> Self {
        Select::new(f.0, f.1)
    }
}

//impl<T1, T2, A, D, N> Select<T1, T2, A, D, N>
//where
//    A: Clone,
//    N: CapabilitySet + Serialize + DeserializeOwned,
//    T1: ChunnelConnector<D, Addr = A> + Negotiate<N>,
//    T2: ChunnelConnector<D, Addr = A> + Negotiate<N>,
//    <T1 as ChunnelConnector<D>>::Error: Into<eyre::Error> + Send + Sync + 'static,
//    <T2 as ChunnelConnector<D>>::Error: Into<eyre::Error> + Send + Sync + 'static,
//    <T1 as ChunnelConnector<D>>::Connection: ChunnelConnection<Data = D> + Send + 'static,
//    <T2 as ChunnelConnector<D>>::Connection: ChunnelConnection<Data = D> + Send + 'static,
//{
//    fn foo(&self) {}
//}

impl<T1, T2, A, D, N> ChunnelConnector<D> for Select<T1, T2, A, D, N>
where
    A: Address<Vec<u8>> + Clone + Send + Sync + 'static,
    <A as Address<Vec<u8>>>::Connector: Send + Sync + 'static,
    <<A as Address<Vec<u8>>>::Connector as ChunnelConnector<Vec<u8>>>::Connection:
        Send + Sync + 'static,
    <<A as Address<Vec<u8>>>::Connector as ChunnelConnector<Vec<u8>>>::Error:
        Into<eyre::Error> + Send + Sync + 'static,
    N: CapabilitySet + Serialize + DeserializeOwned + Send + Sync,
    T1: ChunnelConnector<D, Addr = A> + Negotiate<N>,
    T2: ChunnelConnector<D, Addr = A> + Negotiate<N>,
    <T1 as ChunnelConnector<D>>::Future: Unpin,
    <T2 as ChunnelConnector<D>>::Future: Unpin,
    <T1 as ChunnelConnector<D>>::Error: Into<eyre::Error> + Send + Sync + 'static,
    <T2 as ChunnelConnector<D>>::Error: Into<eyre::Error> + Send + Sync + 'static,
    <T1 as ChunnelConnector<D>>::Connection: ChunnelConnection<Data = D> + Send + 'static,
    <T2 as ChunnelConnector<D>>::Connection: ChunnelConnection<Data = D> + Send + 'static,
{
    type Addr = A;
    type Connection = Either<T1::Connection, T2::Connection>;
    type Error = Report;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn connect(&mut self, a: Self::Addr) -> Self::Future {
        let use_t1 = match (T1::scope(), T2::scope()) {
            (Scope::Application, _) => true,
            (_, Scope::Application) => false,
            (Scope::Host, _) => true,
            (_, Scope::Host) => false,
            (Scope::Local, _) => true,
            (_, Scope::Local) => false,
            (Scope::Global, _) => true,
        };

        /// Pick an implementaion (t1 or t2)
        ///
        /// The handshake is in two steps:
        /// 1. send the locally picked capability set (C).
        /// 2. receive a remote capability set.
        /// 3. pick a local set based on returned capabilities.
        /// return true if C, false if Calt.
        async fn handshake<A, C, Calt, N>(remote: A) -> Result<bool, Report>
        where
            A: Address<Vec<u8>>,
            C: Negotiate<N>,
            Calt: Negotiate<N>,
            N: CapabilitySet + Serialize + DeserializeOwned,
            <<A as Address<Vec<u8>>>::Connector as ChunnelConnector<Vec<u8>>>::Error:
                Into<eyre::Error> + Send + Sync + 'static,
        {
            // 0. negotiation connection.
            let mut ctr = remote.connector();
            let cn = ctr.connect(remote).await.map_err(|e| e.into())?;
            let mut buf = N::guid().to_be_bytes().to_vec();
            let orig_set = C::capabilities();
            let set = bincode::serialize(&orig_set)?;
            // 1. send the locally picked capability set.
            // (u64, Vec<enum>)
            buf.extend(set);
            debug!(
                guid = N::guid(),
                caps = ?C::capabilities(),
                "sending negotiation offer"
            );
            cn.send(buf).await?;

            // 2. get the remote capability set.
            let buf = cn.recv().await?;
            let id = u64::from_be_bytes(buf[0..8].try_into().unwrap());
            debug!("got negotiation response");
            if id != N::guid() {
                return Err(eyre!(
                    "Negotiation type mismatch: {:?} < {:?}",
                    id,
                    N::guid()
                ));
            }

            let recv_set: Vec<N> = bincode::deserialize(&buf[8..])?;
            // pick a local set from (C, Calt) depending on the returned capabilities.
            let alt_set = Calt::capabilities();

            if recv_set == N::universe() {
                // arbitrarily prefer client impls to server impls
                if orig_set == N::universe() {
                    Ok(true)
                } else if alt_set == N::universe() {
                    Ok(false)
                } else {
                    Ok(true)
                }
            } else {
                Ok(true)
            }
        }

        async fn decide<F1, C1, E1, F2, C2, E2, H>(
            mut fut1: F1,
            first_type: &'static str,
            fut2: F2,
            second_type: &'static str,
            mut hshake: H,
        ) -> Result<Either<C1, C2>, Report>
        where
            F1: Future<Output = Result<C1, E1>> + Unpin,
            F2: Future<Output = Result<C2, E2>>,
            H: Future<Output = Result<bool, Report>> + Unpin,
            E1: Into<eyre::Error> + Send + Sync + 'static,
            E2: Into<eyre::Error> + Send + Sync + 'static,
        {
            // 0. concurrently: negotiation.connect and fut1.connect
            // 1. if negotiation picks t1 or t2 and choice is same as ours or negotiation fails:
            //    return connection
            // 2. otherwise, other.connect() and return
            debug!(chunnel_type = first_type, "picking locally");
            enum SelectState<C, E>
            where
                E: Into<eyre::Error> + Send + Sync + 'static,
            {
                WaitForPrimary,
                WaitForHshake(C),
                UseAlt(Option<E>),
            }

            let mut state = tokio::select! {
                res = &mut fut1 => {
                    match res {
                        Ok(c) => {
                            SelectState::WaitForHshake(c)
                        }
                        Err(e) => {
                            SelectState::UseAlt(Some(e))
                        }
                    }
                }
                res = &mut hshake => {
                    match res {
                        Ok(true) => SelectState::WaitForPrimary,
                        Ok(false) => SelectState::UseAlt(None),
                        Err(e) => {
                            debug!(err = ?e, "negotiation handshake failed");
                            SelectState::WaitForPrimary
                        }
                    }
                }
            };

            match state {
                SelectState::WaitForPrimary => match fut1.await {
                    Ok(c) => return Ok(Either::Left(c)),
                    Err(e1) => {
                        state = SelectState::UseAlt(Some(e1));
                    }
                },
                SelectState::UseAlt(e1) => {
                    debug!(chunnel_type = second_type, "using fallback");
                    match fut2.await {
                        Ok(c) => return Ok(Either::Right(c)),
                        Err(right_e) => {
                            let mut err = right_e
                                .into()
                                .wrap_err(eyre!("Second-choice chunnel connect() failed"));
                            if let Some(e) = e1 {
                                err = err.wrap_err(
                                    e.into()
                                        .wrap_err(eyre!("First-choice chunnel connect() failed")),
                                );
                            }
                            return Err(err);
                        }
                    }
                }
                SelectState::WaitForHshake(c1) => match hshake.await {
                    Ok(false) => {
                        state = SelectState::UseAlt(None);
                    }
                    Ok(true) => return Ok(Either::Left(c1)),
                    Err(e) => {
                        debug!(err = ?e, "negotiation handshake failed");
                        return Ok(Either::Left(c1));
                    }
                },
            }

            debug!(chunnel_type = second_type, "using fallback");
            match state {
                SelectState::UseAlt(e1) => match fut2.await {
                    Ok(c) => return Ok(Either::Right(c)),
                    Err(right_e) => {
                        let mut err = right_e
                            .into()
                            .wrap_err(eyre!("Second-choice chunnel connect() failed"));
                        if let Some(e) = e1 {
                            err = err.wrap_err(
                                e.into()
                                    .wrap_err(eyre!("First-choice chunnel connect() failed")),
                            );
                        }
                        return Err(err);
                    }
                },
                _ => unreachable!(),
            }
        }

        let t1 = self.0.connect(a.clone());
        let t2 = self.1.connect(a.clone());

        Box::pin(async move {
            if use_t1 {
                decide(
                    t1,
                    std::any::type_name::<T1>(),
                    t2,
                    std::any::type_name::<T2>(),
                    Box::pin(handshake::<_, T1, T2, _>(a.clone())),
                )
                .await
            } else {
                decide(
                    t2,
                    std::any::type_name::<T2>(),
                    t1,
                    std::any::type_name::<T1>(),
                    Box::pin(handshake::<_, T2, T1, _>(a.clone())),
                )
                .await
                .map(|eith| eith.flip())
            }
        })
    }

    fn scope() -> Scope {
        unimplemented!()
    }
    fn endedness() -> Endedness {
        unimplemented!()
    }
    fn implementation_priority() -> usize {
        unimplemented!()
    }
}

impl<T1, T2, A, D, N> ChunnelListener<D> for Select<T1, T2, A, D, N>
where
    A: Clone,
    N: CapabilitySet + Serialize + DeserializeOwned,
    T1: ChunnelListener<D, Addr = A> + Negotiate<N>,
    T2: ChunnelListener<D, Addr = A> + Negotiate<N>,
    <T1 as ChunnelListener<D>>::Error: Into<eyre::Error> + Send + Sync + 'static,
    <T2 as ChunnelListener<D>>::Error: Into<eyre::Error> + Send + Sync + 'static,
    <T1 as ChunnelListener<D>>::Connection: ChunnelConnection<Data = D> + Send + 'static,
    <T2 as ChunnelListener<D>>::Connection: ChunnelConnection<Data = D> + Send + 'static,
{
    type Addr = A;
    type Connection = Either<T1::Connection, T2::Connection>;
    type Error = Report;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        let use_t1 = match (T1::scope(), T2::scope()) {
            (Scope::Application, _) => true,
            (_, Scope::Application) => false,
            (Scope::Host, _) => true,
            (_, Scope::Host) => false,
            (Scope::Local, _) => true,
            (_, Scope::Local) => false,
            (Scope::Global, _) => true,
        };

        use futures_util::TryStreamExt;

        let left_fut = self.0.listen(a.clone());
        let right_fut = self.1.listen(a);
        if use_t1 {
            debug!(chunnel_type = std::any::type_name::<T1>(), "picking");
            Box::pin(async move {
                match left_fut.await {
                    Ok(st) => Ok(
                        Box::pin(st.map_ok(|c| Either::Left(c)).map_err(|e| e.into()))
                            as Pin<
                                Box<
                                    dyn Stream<Item = Result<Self::Connection, Self::Error>>
                                        + Send
                                        + 'static,
                                >,
                            >,
                    ),
                    Err(left_e) => {
                        let left_e = left_e
                            .into()
                            .wrap_err(eyre!("First-choice chunnel listen() failed"));
                        match right_fut.await {
                            Ok(st) => Ok(Box::pin(
                                st.map_ok(|c| Either::Right(c)).map_err(|e| e.into()),
                            )
                                as Pin<
                                    Box<
                                        dyn Stream<Item = Result<Self::Connection, Self::Error>>
                                            + Send
                                            + 'static,
                                    >,
                                >),
                            Err(right_e) => Err(right_e
                                .into()
                                .wrap_err(eyre!("Second-choice chunnel listen() failed"))
                                .wrap_err(left_e)),
                        }
                    }
                }
            }) as _
        } else {
            debug!(chunnel_type = std::any::type_name::<T2>(), "picking");
            Box::pin(async move {
                match right_fut.await {
                    Ok(st) => Ok(
                        Box::pin(st.map_ok(|c| Either::Right(c)).map_err(|e| e.into()))
                            as Pin<
                                Box<
                                    dyn Stream<Item = Result<Self::Connection, Self::Error>>
                                        + Send
                                        + 'static,
                                >,
                            >,
                    ),
                    Err(right_e) => {
                        let right_e = right_e
                            .into()
                            .wrap_err(eyre!("First-choice chunnel listen() failed"));
                        match left_fut.await {
                            Ok(st) => Ok(Box::pin(
                                st.map_ok(|c| Either::Left(c)).map_err(|e| e.into()),
                            )
                                as Pin<
                                    Box<
                                        dyn Stream<Item = Result<Self::Connection, Self::Error>>
                                            + Send
                                            + 'static,
                                    >,
                                >),
                            Err(left_e) => Err(left_e
                                .into()
                                .wrap_err(eyre!("Second-choice chunnel listen() failed"))
                                .wrap_err(right_e)),
                        }
                    }
                }
            }) as _
        }
    }

    fn scope() -> Scope {
        unimplemented!()
    }
    fn endedness() -> Endedness {
        unimplemented!()
    }
    fn implementation_priority() -> usize {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use crate::{ChunnelConnector, ChunnelListener, Endedness, Scope};

    macro_rules! test_scope_impl {
        ($name:ident,$scope:expr) => {
            struct $name<C>(C);

            impl<C> ChunnelConnector for $name<C>
            where
                C: ChunnelConnector + Send + Sync + 'static,
            {
                type Addr = C::Addr;
                type Connection = C::Connection;
                type Future = C::Future;
                type Error = C::Error;

                fn connect(&mut self, a: Self::Addr) -> Self::Future {
                    self.0.connect(a)
                }

                fn scope() -> Scope {
                    $scope
                }
                fn endedness() -> Endedness {
                    C::endedness()
                }
                fn implementation_priority() -> usize {
                    C::implementation_priority()
                }
            }

            impl<C> ChunnelListener for $name<C>
            where
                C: ChunnelListener + Send + Sync + 'static,
            {
                type Addr = C::Addr;
                type Connection = C::Connection;
                type Future = C::Future;
                type Stream = C::Stream;
                type Error = C::Error;

                fn listen(&mut self, a: Self::Addr) -> Self::Future {
                    self.0.listen(a)
                }

                fn scope() -> Scope {
                    $scope
                }
                fn endedness() -> Endedness {
                    C::endedness()
                }
                fn implementation_priority() -> usize {
                    C::implementation_priority()
                }
            }
        };
    }

    test_scope_impl!(ImplA, Scope::Host);
    test_scope_impl!(ImplB, Scope::Local);

    use crate::bincode::SerializeChunnel;
    use crate::chan_transport::RendezvousChannel;
    use crate::util::{Never, OptionUnwrap};
    use crate::ChunnelConnection;
    use futures_util::TryStreamExt;
    use serde::{Deserialize, Serialize};
    use tracing::info;
    use tracing_futures::Instrument;

    #[test]
    fn negotiate() {
        let _guard = tracing_subscriber::fmt::try_init();
        color_eyre::install().unwrap_or_else(|_| ());

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();
        rt.block_on(
            async move {
                let (srv, cln) = RendezvousChannel::new(10).split();
                let mut srv = OptionUnwrap::from(srv);
                let (s, r) = tokio::sync::oneshot::channel();

                tokio::spawn(
                    async move {
                        let st = srv.listen(3u8).await.unwrap();
                        s.send(()).unwrap();
                        st.try_for_each_concurrent(None, |cn| async move {
                            let m = cn.recv().await?;
                            cn.send(m).await?;
                            Ok::<_, eyre::Report>(())
                        })
                        .await
                    }
                    .instrument(tracing::debug_span!("server")),
                );

                let mut cln: super::Select<_, _, _> =
                    (ImplA(cln.clone()), ImplB(Never::from(cln))).into();
                let _: () = r.await.unwrap();
                info!("connecting client");
                let cn = cln
                    .connect(3u8)
                    .instrument(tracing::debug_span!("connect"))
                    .await
                    .unwrap();

                cn.send(vec![1u8; 8])
                    .instrument(tracing::debug_span!("send"))
                    .await
                    .unwrap();
                let d = cn
                    .recv()
                    .instrument(tracing::debug_span!("recv"))
                    .await
                    .unwrap();
                assert_eq!(d, vec![1u8; 8]);
            }
            .instrument(tracing::debug_span!("negotiate")),
        );
    }

    #[derive(Copy, Clone, Debug, Serialize, Deserialize)]
    struct Foo(u8);

    #[test]
    fn negotiate_serialize() {
        let _guard = tracing_subscriber::fmt::try_init();
        color_eyre::install().unwrap_or_else(|_| ());

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();
        rt.block_on(
            async move {
                let (srv, cln) = RendezvousChannel::new(10).split();
                let mut srv = OptionUnwrap::from(srv);
                let (s, r) = tokio::sync::oneshot::channel();

                let srv = SerializeChunnel::<_, Foo>::from(srv);

                tokio::spawn(
                    async move {
                        let st = srv.listen(3u8).await.unwrap();
                        s.send(()).unwrap();
                        st.try_for_each_concurrent(None, |cn| async move {
                            let m = cn.recv().await?;
                            cn.send(m).await?;
                            Ok::<_, eyre::Report>(())
                        })
                        .await
                    }
                    .instrument(tracing::debug_span!("server")),
                );

                let cln = SerializeChunnel::<_, Foo>::from(cln);

                let mut cln: super::Select<_, _, _> =
                    (ImplA(cln.clone()), ImplB(Never::from(cln))).into();

                cln.foo();
                //let _: () = r.await.unwrap();
                //info!("connecting client");
                //let cn = cln
                //    .connect(3u8)
                //    .instrument(tracing::debug_span!("connect"))
                //    .await
                //    .unwrap();

                //cn.send(vec![1u8; 8])
                //    .instrument(tracing::debug_span!("send"))
                //    .await
                //    .unwrap();
                //let d = cn
                //    .recv()
                //    .instrument(tracing::debug_span!("recv"))
                //    .await
                //    .unwrap();
                //assert_eq!(d, vec![1u8; 8]);
            }
            .instrument(tracing::debug_span!("negotiate")),
        );
    }
}
