//! Chunnel wrapper types to negotiate between multiple implementations.

use crate::{
    ChunnelConnection, ChunnelConnector, ChunnelListener, Client, ConnectAddress, CxList, CxNil,
    Either, ListenAddress, Serve,
};
use color_eyre::eyre::{eyre, Report, WrapErr};
use futures_util::{
    future::Ready,
    stream::{Once, Stream, TryStreamExt},
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tracing::debug;

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

pub trait NegotiateDummy {}

/// Define an enum that implements the `CapabilitySet` trait.
///
/// Invoke with enum name (with optional `pub`) followed by variant names.
///
/// # Example
/// ```rust
/// # use bertha::enumerate_enum;
/// enumerate_enum!(pub Foo, 0xe1e3ca44a5ece5bb, A, B, C);
/// enumerate_enum!(Bar, 0x829233ff7c2ab87a, A, B, C);
/// fn main() {
///     let f = Foo::B;
///     let b = Bar::C;
///     println!("{:?}, {:?}", f, b);
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
}

impl Negotiate for CxNil {
    type Capability = ();
    fn capabilities() -> Vec<Self::Capability> {
        vec![()]
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

fn get_offer<T>() -> Offer
where
    T: Negotiate,
    <T as Negotiate>::Capability: Serialize + DeserializeOwned,
{
    Offer {
        capability_guid: T::Capability::guid(),
        available: bincode::serialize(&T::capabilities()).unwrap(),
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
        vec![vec![get_offer::<N>()]]
    }
}

/// Trait to monomorphize a CxList with possible `Select`s into something that impls Serve or Client.
pub trait Pick<I> {
    type Picked;
    fn pick(self, client_offers: Vec<Vec<Offer>>) -> Result<(Vec<Offer>, Self::Picked), Report>;
}

impl<N, I> Pick<I> for N
where
    N: Negotiate,
{
    type Picked = Self;
    fn pick(self, o: Vec<Vec<Offer>>) -> Result<(Vec<Offer>, Self::Picked), Report> {
        if o.is_empty() {
            return Err(eyre!("Not enough offers for stack"));
        }

        Ok((o.into_iter().next().unwrap(), self))
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
        let mut t1 = self.0.offers()[0].clone();
        let t2 = self.1.offers()[0].clone();
        t1.extend(t2);
        vec![t1]
    }
}

fn check_offers(offers: &[Vec<Offer>]) -> Result<(), Report> {
    if !offers.iter().all(|l| {
        let id = l[0].capability_guid;
        l.iter().all(|o| o.capability_guid == id)
    }) {
        return Err(eyre!("Capability guid mismatch"));
    }

    Ok(())
}

fn lacking<T: PartialEq>(a: &[T], univ: Vec<T>) -> Vec<T> {
    univ.into_iter().filter(|x| !a.contains(x)).collect()
}

impl<T1, T2, I, D, C> Pick<I> for Select<T1, T2>
where
    T1: Serve<I> + Negotiate<Capability = C>,
    T2: Serve<I> + Negotiate<Capability = C>,
    C: CapabilitySet + Serialize + DeserializeOwned + Clone,
    <T1 as Serve<I>>::Connection: ChunnelConnection<Data = D>,
    <T2 as Serve<I>>::Connection: ChunnelConnection<Data = D>,
{
    type Picked = Either<T1, T2>;
    fn pick(self, offers: Vec<Vec<Offer>>) -> Result<(Vec<Offer>, Self::Picked), Report> {
        if offers.is_empty() {
            return Err(eyre!("Not enough offers for stack"));
        }

        let offer = offers.into_iter().next().unwrap();
        if offer[0].capability_guid != C::guid() {
            return Err(eyre!("Capability type mismatch"));
        }

        let caps: Result<Vec<Vec<C>>, Report> = offer
            .iter()
            .map(|o| {
                let c: Vec<C> = bincode::deserialize(&o.available)
                    .wrap_err(eyre!("Deserializing capability set"))?;
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

        for idx in 0..caps.len() {
            if t1.len() <= t2.len() {
                let mut co: Vec<C> = caps[idx].clone();
                co.extend_from_slice(&t1);
                if lacking(&co, C::universe()).is_empty() {
                    return Ok((vec![caps[idx].clone().into()], Either::Left(self.0)));
                }

                let mut co = caps[idx].clone();
                co.extend_from_slice(&t2);
                if lacking(&co, C::universe()).is_empty() {
                    return Ok((vec![caps[idx].clone().into()], Either::Right(self.1)));
                }
            } else {
                let mut co = caps[idx].clone();
                co.extend_from_slice(&t2);
                if lacking(&co, C::universe()).is_empty() {
                    return Ok((vec![caps[idx].clone().into()], Either::Right(self.1)));
                }

                let mut co = caps[idx].clone();
                co.extend_from_slice(&t1);
                if lacking(&co, C::universe()).is_empty() {
                    return Ok((vec![caps[idx].clone().into()], Either::Left(self.0)));
                }
            }
        }

        return Err(eyre!(
            "Could not find satisfying client/server capability set for {:?}",
            std::any::type_name::<C>()
        ));
    }
}

impl<H, T, I> Pick<I> for CxList<H, T>
where
    H: Pick<I>,
    T: Pick<I>,
{
    type Picked = CxList<H::Picked, T::Picked>;
    fn pick(self, offers: Vec<Vec<Offer>>) -> Result<(Vec<Offer>, Self::Picked), Report> {
        if offers.is_empty() {
            return Err(eyre!("Not enough offers for stack"));
        }

        let mut offers_iter = offers.into_iter();
        let (mut cl_pick, head_pick) = self.head.pick(vec![offers_iter.next().unwrap()])?;
        let (rest_cl_pick, tail_pick) = self.tail.pick(offers_iter.collect())?;
        cl_pick.extend(rest_cl_pick);
        Ok((
            cl_pick,
            CxList {
                head: head_pick,
                tail: tail_pick,
            },
        ))
    }
}

macro_rules! addr_conntype {
    (srv, $a:ty) => {
        Once<Ready<Result<<<$a as ListenAddress>::Listener as ChunnelListener>::Connection, Report>>>
    };
    (cln, $a:ty) => {
        <<$a as ConnectAddress>::Connector as ChunnelConnector>::Connection
    };
}

/// Return a stream of connections with `stack`'s semantics, listening on `a`.
pub async fn negotiate_server<H, T, A>(
    stack: CxList<H, T>,
    a: A,
) -> Result<
    impl Stream<Item = Result<<<CxList<H, T> as Pick<addr_conntype!(srv, A)>>::Picked as Serve<addr_conntype!(srv, A)>>::Connection, Report>>,
    Report,
>
where
    A: ListenAddress,
    <<A as ListenAddress>::Listener as ChunnelListener>::Connection:
        ChunnelConnection<Data = Vec<u8>>,
    <<A as ListenAddress>::Listener as ChunnelListener>::Error:
        Into<Report> + Send + Sync + 'static,
    CxList<H, T>: Pick<addr_conntype!(srv, A)> + Clone + 'static,
    <CxList<H, T> as Pick<addr_conntype!(srv, A)>>::Picked: Serve<addr_conntype!(srv, A)> + Clone,
    <<CxList<H, T> as Pick<addr_conntype!(srv, A)>>::Picked as Serve<addr_conntype!(srv, A)>>::Error:
        Into<Report> + Send + Sync + 'static,
    <<CxList<H, T> as Pick<addr_conntype!(srv, A)>>::Picked as Serve<addr_conntype!(srv, A)>>::Stream:
        Unpin + Send  + 'static,
    CxList<H, T>: GetOffers,
{
    // 1. serve Vec<u8> connections.
    let mut listener = a.listener();
    let st: Result<
        <<A as ListenAddress>::Listener as ChunnelListener>::Stream,
        <<A as ListenAddress>::Listener as ChunnelListener>::Error,
    > = listener.listen(a).await;
    let st = st.map_err(Into::into)?; // stream of incoming Vec<u8> conns.

    Ok(st.map_err(Into::into).and_then(move |cn| {
        debug!("new connection");
        let stack = stack.clone();
        async move {
            // 2. on new connection, read off Vec<Vec<Offer>> from
            //    client
            let buf: Vec<u8> = cn.recv().await?;
            let client_offers: Vec<Vec<Offer>> = bincode::deserialize(&buf)?;
            debug!(client_offers = ?&client_offers, "received offer");

            if let Err(e) = check_offers(&client_offers) {
                debug!(err = ?e, "Received invalid offer set from client");
                // TODO send error response
                unimplemented!();
            }

            // 3. monomorphize: transform the CxList<impl Serve/Select<impl Serve, impl Serve>>
            // into a CxList<impl Serve>
            let (picked_offers, mut new_stack) = stack.pick(client_offers)?;
            debug!(picked_client_offers = ?&picked_offers, "monomorphized stack");

            // 4. Respond to client with offer choice
            let buf = bincode::serialize(&picked_offers)?;
            cn.send(buf).await?;

            debug!("negotiation handshake done");

            // 5. new_stack.serve(vec_u8_stream)
            let cn_st = futures_util::stream::once(futures_util::future::ready(Ok(cn)));
            let mut new_st = new_stack.serve(cn_st).await.map_err(Into::into)?;
            let new_cn = new_st
                .try_next()
                .await // -> Result<Option<T>, E>
                .map_err(Into::into)?
                .ok_or_else(|| eyre!("No connection returned"))?;

            debug!("returning connection");
            Ok(new_cn)
        }
    }))
}

pub trait Apply<I> {
    type Applied;
    fn apply(self, offers: Vec<Offer>) -> Result<Self::Applied, Report>;
}

impl<N, I> Apply<I> for N
where
    N: Negotiate,
    <N as Negotiate>::Capability: DeserializeOwned + Ord,
{
    type Applied = Self;
    fn apply(self, o: Vec<Offer>) -> Result<Self::Applied, Report> {
        if o.is_empty() {
            return Err(eyre!("Not enough offers for stack"));
        }

        let o = o.into_iter().next().unwrap();
        if o.capability_guid != N::Capability::guid() {
            return Err(eyre!(
                "Capability mismatch: offer={}, this={}",
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
                "Capability mismatch: offer={:?}, this={:?}",
                cs,
                this
            ));
        }

        Ok(self)
    }
}

impl<H, T, I> Apply<I> for CxList<H, T>
where
    H: Apply<I>,
    T: Apply<I>,
{
    type Applied = CxList<H::Applied, T::Applied>;
    fn apply(self, offers: Vec<Offer>) -> Result<Self::Applied, Report> {
        if offers.is_empty() {
            return Err(eyre!("Not enough offers for stack"));
        }

        let mut offers_iter = offers.into_iter();
        let head_pick = self.head.apply(vec![offers_iter.next().unwrap()])?;
        let tail_pick = self.tail.apply(offers_iter.collect())?;
        Ok(CxList {
            head: head_pick,
            tail: tail_pick,
        })
    }
}

impl<I, D, T1, T2> Apply<I> for Select<T1, T2>
where
    T1: Apply<I>,
    T2: Apply<I>,
    <T1 as Apply<I>>::Applied: Client<I>,
    <T2 as Apply<I>>::Applied: Client<I>,
    <<T1 as Apply<I>>::Applied as Client<I>>::Connection: ChunnelConnection<Data = D>,
    <<T2 as Apply<I>>::Applied as Client<I>>::Connection: ChunnelConnection<Data = D>,
{
    type Applied = Either<<T1 as Apply<I>>::Applied, <T2 as Apply<I>>::Applied>;
    fn apply(self, offers: Vec<Offer>) -> Result<Self::Applied, Report> {
        match self.0.apply(offers.clone()) {
            Ok(t1_applied) => Ok(Either::Left(t1_applied)),
            Err(e) => {
                debug!(t1 = std::any::type_name::<T1>(), err = ?e, "Select::T1 mismatched");
                Ok(Either::Right(self.1.apply(offers)?))
            }
        }
    }
}

/// Return a connection with `stack`'s semantics, connecting to `a`.
pub async fn negotiate_client<H, T, A>(stack: CxList<H, T>, a: A) -> Result<<<CxList<H, T> as Apply<addr_conntype!(cln, A)>>::Applied as Client<addr_conntype!(cln, A)>>::Connection, Report>
where
    A: ConnectAddress,
    <<A as ConnectAddress>::Connector as ChunnelConnector>::Connection:
        ChunnelConnection<Data = Vec<u8>>,
    <<A as ConnectAddress>::Connector as ChunnelConnector>::Error:
        Into<Report> + Send + Sync + 'static,
    CxList<H, T>: Apply<addr_conntype!(cln, A)> + Clone + 'static,
    <CxList<H, T> as Apply<addr_conntype!(cln, A)>>::Applied: Client<addr_conntype!(cln, A)> + Clone,
    <<CxList<H, T> as Apply<addr_conntype!(cln, A)>>::Applied as Client<addr_conntype!(cln, A)>>::Error:
        Into<Report> + Send + Sync + 'static,
    CxList<H, T>: GetOffers,
{
    // 1. get Vec<u8> connection.
    let cn = a.connector().connect(a).await.map_err(Into::into)?;
    debug!("got negotiation connection");

    // 2. send Vec<Vec<Offer>>
    let offers = stack.offers();
    let buf = bincode::serialize(&offers)?;
    debug!(offers = ?&offers, "sending offers");
    cn.send(buf).await?;

    // 3. receive Vec<Offer>
    let buf = cn.recv().await?;
    let picked: Vec<Offer> = bincode::deserialize(&buf)?;
    debug!(picked = ?&picked, "received picked impls");

    // 4. monomorphize `stack`, picking received choices
    let mut new_stack = stack.apply(picked)?;

    // 5. return new_stack.connect_wrap(vec_u8_conn)
    new_stack.connect_wrap(cn).await.map_err(Into::into)
}

#[allow(non_upper_case_globals)]
#[cfg(test)]
mod test {
    use super::{
        negotiate_client, negotiate_server, CapabilitySet, GetOffers, Negotiate, Offer, Select,
    };
    use crate::{
        chan_transport::{Chan, ChanAddr},
        ChunnelConnection, ChunnelConnector, ChunnelListener, Client, ConnectAddress, CxList,
        ListenAddress, Serve,
    };
    use color_eyre::eyre::{eyre, Report};
    use futures_util::{
        future::{ready, Ready},
        stream::{Stream, StreamExt},
    };
    use tracing::{debug, info};
    use tracing_futures::Instrument;

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
        let _guard = tracing_subscriber::fmt::try_init();
        color_eyre::install().unwrap_or_else(|_| ());

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                info!("starting");
                let a: ChanAddr<Vec<u8>> = Chan::default().into();
                let cl_a = a.clone();
                let stack = CxList::from(ChunnelA).wrap(ChunnelB).wrap(ChunnelC);
                let srv_stack = stack.clone();

                let (s, r) = tokio::sync::oneshot::channel();
                tokio::spawn(
                    async move {
                        info!("starting");
                        let mut srv_stream = negotiate_server(srv_stack, a).await?;
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
                    .instrument(tracing::debug_span!("server")),
                );

                r.await.unwrap();

                // make a Vec<u8> client
                let a = cl_a;
                let cn = a.connector().connect(a).await?;

                // send the raw Vec<Vec<Offer>>
                // [ [Offer{guid: A, vec:[]}], [Offer{guid: B, vec:[]}], [Offer{guid: C, vec:[]}] ]
                let offers: Vec<Vec<Offer>> = stack.offers();
                debug!(offers = ?&offers, "starting negotiation handshake");
                let buf = bincode::serialize(&offers)?;
                cn.send(buf).await?;

                let resp = cn.recv().await?;
                let resp: Vec<Offer> = bincode::deserialize(&resp)?;
                debug!(resp = ?&resp, "got negotiation response");

                let expected: Vec<Offer> = offers
                    .into_iter()
                    .map(|o| o.into_iter().next().unwrap())
                    .collect();
                assert_eq!(resp, expected);

                info!("done");

                Ok::<_, Report>(())
            }
            .instrument(tracing::info_span!("serve_no_select")),
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
        let _guard = tracing_subscriber::fmt::try_init();
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
        let _guard = tracing_subscriber::fmt::try_init();
        color_eyre::install().unwrap_or_else(|_| ());

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                info!("starting");
                let a: ChanAddr<Vec<u8>> = Chan::default().into();
                let cl_a = a.clone();
                let stack = CxList::from(ChunnelA)
                    .wrap(Select(ChunnelB, ChunnelBAlt))
                    .wrap(ChunnelC);
                let srv_stack = stack.clone();

                let (s, r) = tokio::sync::oneshot::channel();
                tokio::spawn(
                    async move {
                        info!("starting");
                        let srv_stream = negotiate_server(srv_stack, a).await?;
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
                            .ok_or_else(|| eyre!("srv_stream returned none"))??;
                        Ok::<_, Report>(())
                    }
                    .instrument(tracing::debug_span!("server")),
                );

                r.await.unwrap();

                // make a Vec<u8> client
                let a = cl_a;
                let cn = a.connector().connect(a).await?;

                // send the raw Vec<Vec<Offer>>
                // [ [Offer{guid: A, vec:[]}], [Offer{guid: B, vec:[]}], [Offer{guid: C, vec:[]}] ]
                let offers: Vec<Vec<Offer>> = stack.offers();
                info!(offers = ?&offers, "starting negotiation handshake");
                let buf = bincode::serialize(&offers)?;
                cn.send(buf).await?;

                let resp = cn.recv().await?;
                let resp: Vec<Offer> = bincode::deserialize(&resp)?;
                info!(resp = ?&resp, "got negotiation response");

                let expected: Vec<Offer> = offers
                    .into_iter()
                    .map(|o| o.into_iter().next().unwrap())
                    .collect();
                assert_eq!(resp, expected);

                info!("done");

                Ok::<_, Report>(())
            }
            .instrument(tracing::info_span!("serve_select")),
        )
        .unwrap();
    }

    #[test]
    fn client_select() {
        let _guard = tracing_subscriber::fmt::try_init();
        color_eyre::install().unwrap_or_else(|_| ());

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                info!("starting");
                let a: ChanAddr<Vec<u8>> = Chan::default().into();
                let cl_a = a.clone();
                let stack = CxList::from(ChunnelA)
                    .wrap(Select(ChunnelB, ChunnelBAlt))
                    .wrap(ChunnelC);

                let (s, r) = tokio::sync::oneshot::channel();
                tokio::spawn(
                    async move {
                        info!("starting");
                        let mut st = a.listener().listen(a).await?;
                        s.send(()).unwrap();

                        let cn = st.next().await.unwrap()?;
                        let buf = cn.recv().await?;
                        let offers: Vec<Vec<Offer>> = bincode::deserialize(&buf)?;

                        // Pick something fake, the first idx for each of them
                        let picked: Vec<Offer> = offers
                            .into_iter()
                            .map(|v| v.into_iter().next().unwrap())
                            .collect();

                        let buf = bincode::serialize(&picked)?;
                        cn.send(buf).await?;

                        Ok::<_, Report>(())
                    }
                    .instrument(tracing::debug_span!("server")),
                );

                r.await.unwrap();

                let _cn = negotiate_client(stack, cl_a)
                    .instrument(tracing::info_span!("negotiate_client"))
                    .await?;

                info!("done");

                Ok::<_, Report>(())
            }
            .instrument(tracing::info_span!("client_select")),
        )
        .unwrap();
    }

    #[test]
    fn both_select() {
        let _guard = tracing_subscriber::fmt::try_init();
        color_eyre::install().unwrap_or_else(|_| ());

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(
            async move {
                info!("starting");
                let a: ChanAddr<Vec<u8>> = Chan::default().into();
                let cl_a = a.clone();
                let stack = CxList::from(ChunnelA)
                    .wrap(Select(ChunnelB, ChunnelBAlt))
                    .wrap(ChunnelC);
                let srv_stack = stack.clone();

                let (s, r) = tokio::sync::oneshot::channel();
                tokio::spawn(
                    async move {
                        info!("starting");
                        let srv_stream = negotiate_server(srv_stack, a).await?;
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
                    .instrument(tracing::debug_span!("server")),
                );

                r.await.unwrap();

                let cn = negotiate_client(stack, cl_a)
                    .instrument(tracing::info_span!("negotiate_client"))
                    .await?;

                cn.send(vec![1u8; 10]).await?;
                let buf = cn.recv().await?;

                assert_eq!(buf, vec![1u8; 10]);

                info!("done");

                Ok::<_, Report>(())
            }
            .instrument(tracing::info_span!("both_select")),
        )
        .unwrap();
    }
}
