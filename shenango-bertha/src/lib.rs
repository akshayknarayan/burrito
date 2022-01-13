//! Bertha logic adapted to work inside a shenango runtime
// we will only use the things needed for the kvstore application.

mod base_traits {
    use bertha::{CxList, CxNil};
    use color_eyre::eyre::Report;

    pub trait Chunnel<I> {
        type Connection: ChunnelConnection;
        type Error: Send + Sync + 'static;

        fn connect_wrap(&mut self, inner: I) -> Result<Self::Connection, Self::Error>;
    }

    impl<C> Chunnel<C> for CxNil
    where
        C: ChunnelConnection,
    {
        type Connection = C;
        type Error = Report;

        fn connect_wrap(&mut self, inner: C) -> Result<Self::Connection, Self::Error> {
            Ok(inner)
        }
    }

    impl<H, T, I> Chunnel<I> for CxList<H, T>
    where
        H: Chunnel<I>,
        T: Chunnel<<H as Chunnel<I>>::Connection>,
        <T as Chunnel<<H as Chunnel<I>>::Connection>>::Error: From<<H as Chunnel<I>>::Error>,
    {
        type Connection = T::Connection;
        type Error = T::Error;

        fn connect_wrap(&mut self, inner: I) -> Result<Self::Connection, Self::Error> {
            let cn = self.head.connect_wrap(inner)?;
            self.tail.connect_wrap(cn)
        }
    }

    pub trait ChunnelConnection {
        type Data;
        /// Send a message
        fn send(&self, data: Self::Data) -> Result<(), Report>;
        /// Retrieve next incoming message.
        fn recv(&self) -> Result<Self::Data, Report>;
    }

    pub trait ChunnelConnector {
        type Addr;
        type Connection;
        type Error;
        fn connect(&mut self, a: Self::Addr) -> Result<Self::Connection, Self::Error>;
    }
}

mod udp {
    use crate::base_traits::ChunnelConnection;
    use color_eyre::eyre::Report;
    pub use shenango::udp::udp_accept;
    pub use shenango::udp::UdpConnection;
    use std::net::SocketAddrV4;
    use std::rc::Rc;
    use std::sync::{atomic::AtomicBool, Mutex};

    #[derive(Clone)]
    pub struct UdpChunnelConnection(pub UdpConnection);

    impl ChunnelConnection for UdpChunnelConnection {
        type Data = (SocketAddrV4, Vec<u8>);
        /// Send a message
        fn send(&self, (addr, buf): Self::Data) -> Result<(), Report> {
            self.0.write_to(&buf, addr)?;
            Ok(())
        }
        /// Retrieve next incoming message.
        fn recv(&self) -> Result<Self::Data, Report> {
            let from = self.0.remote_addr();
            let mut buf = [0u8; 1024];
            let len = self.0.recv(&mut buf)?;
            Ok((from, buf[..len].to_vec()))
        }
    }

    pub struct InjectOne<C, D>(C, AtomicBool, Rc<Mutex<Option<D>>>);

    impl<C, D> InjectOne<C, D> {
        pub fn new(inner: C) -> (Self, Rc<Mutex<Option<D>>>) {
            let mtx = Default::default();
            (Self(inner, Default::default(), Rc::clone(&mtx)), mtx)
        }
    }

    impl<C: ChunnelConnection<Data = D>, D> ChunnelConnection for InjectOne<C, D> {
        type Data = D;

        fn send(&self, d: Self::Data) -> Result<(), Report> {
            self.0.send(d)
        }

        fn recv(&self) -> Result<Self::Data, Report> {
            if self.1.load(std::sync::atomic::Ordering::SeqCst) {
                self.0.recv()
            } else {
                self.1.store(true, std::sync::atomic::Ordering::SeqCst);
                let d = self.2.lock().unwrap().take().unwrap();
                Ok(d)
            }
        }
    }

    pub struct UdpConnector;
    impl super::base_traits::ChunnelConnector for UdpConnector {
        type Addr = SocketAddrV4;
        type Connection = UdpChunnelConnection;
        type Error = Report;

        fn connect(&mut self, a: Self::Addr) -> Result<Self::Connection, Self::Error> {
            let cn = UdpConnection::dial(SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, 0), a)?;
            Ok(UdpChunnelConnection(cn))
        }
    }
}

pub use negotiate::{negotiate_client, ClientNegotiator};
pub use negotiate::{negotiate_server, Negotiate};
mod negotiate {
    use crate::base_traits::{Chunnel, ChunnelConnection};
    use crate::udp::{self, InjectOne, UdpChunnelConnection};
    use bertha::{
        negotiate::{monomorphize, Apply, ApplyResult, GetOffers, Pick},
        CxList, DataEither, Either,
    };
    use color_eyre::eyre::{bail, eyre, Report, WrapErr};
    use std::collections::HashMap;
    use std::fmt::Debug;
    use std::net::SocketAddrV4;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    };
    use tracing::{debug, trace, warn};

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct StackNonce(HashMap<u64, bertha::Offer>, HashMap<u64, Option<Vec<u8>>>);

    impl StackNonce {
        fn set_ext_info(&mut self, h: HashMap<u64, Option<Vec<u8>>>) {
            self.1 = h
        }
    }

    impl Into<bertha::StackNonce> for StackNonce {
        fn into(self) -> bertha::StackNonce {
            bertha::StackNonce::__from_inner(self.0)
        }
    }
    impl From<bertha::StackNonce> for StackNonce {
        fn from(s: bertha::StackNonce) -> Self {
            Self(s.__into_inner(), Default::default())
        }
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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

    pub trait Negotiate: bertha::Negotiate {
        fn picked_blocking(&mut self, _nonce: &[u8]) {}
        fn ext_info(&self) -> Option<Vec<u8>> {
            None
        }

        fn ext_info_callback(&mut self, _info: Vec<u8>) {}
    }

    pub trait NegotiatePicked {
        fn call_negotiate_picked(&mut self, nonce: &[u8]);
        fn collect_ext_info(&self) -> HashMap<u64, Option<Vec<u8>>>;
        fn distribute_ext_info(&mut self, info: &mut HashMap<u64, Option<Vec<u8>>>);
    }

    impl<N> NegotiatePicked for N
    where
        N: Negotiate,
    {
        fn call_negotiate_picked(&mut self, nonce: &[u8]) {
            self.picked_blocking(nonce)
        }

        fn collect_ext_info(&self) -> HashMap<u64, Option<Vec<u8>>> {
            std::iter::once((N::guid(), self.ext_info())).collect()
        }

        fn distribute_ext_info(&mut self, info: &mut HashMap<u64, Option<Vec<u8>>>) {
            if let Some(Some(i)) = info.remove(&N::guid()) {
                self.ext_info_callback(i);
            }
        }
    }

    impl<H, T> NegotiatePicked for CxList<H, T>
    where
        H: NegotiatePicked,
        T: NegotiatePicked,
    {
        fn call_negotiate_picked(&mut self, nonce: &[u8]) {
            self.head.call_negotiate_picked(nonce);
            self.tail.call_negotiate_picked(nonce);
        }

        fn collect_ext_info(&self) -> HashMap<u64, Option<Vec<u8>>> {
            self.head
                .collect_ext_info()
                .into_iter()
                .chain(self.tail.collect_ext_info().into_iter())
                .collect()
        }

        fn distribute_ext_info(&mut self, info: &mut HashMap<u64, Option<Vec<u8>>>) {
            self.head.distribute_ext_info(info);
            self.tail.distribute_ext_info(info);
        }
    }

    impl<L, R> NegotiatePicked for DataEither<L, R>
    where
        L: NegotiatePicked,
        R: NegotiatePicked,
    {
        fn call_negotiate_picked(&mut self, nonce: &[u8]) {
            match self {
                DataEither::Left(l) => l.call_negotiate_picked(nonce),
                DataEither::Right(r) => r.call_negotiate_picked(nonce),
            }
        }

        fn collect_ext_info(&self) -> HashMap<u64, Option<Vec<u8>>> {
            match self {
                DataEither::Left(l) => l.collect_ext_info(),
                DataEither::Right(r) => r.collect_ext_info(),
            }
        }

        fn distribute_ext_info(&mut self, info: &mut HashMap<u64, Option<Vec<u8>>>) {
            match self {
                DataEither::Left(l) => l.distribute_ext_info(info),
                DataEither::Right(r) => r.distribute_ext_info(info),
            }
        }
    }

    impl<L, R> NegotiatePicked for Either<L, R>
    where
        L: NegotiatePicked,
        R: NegotiatePicked,
    {
        fn call_negotiate_picked(&mut self, nonce: &[u8]) {
            match self {
                Either::Left(l) => l.call_negotiate_picked(nonce),
                Either::Right(r) => r.call_negotiate_picked(nonce),
            }
        }

        fn collect_ext_info(&self) -> HashMap<u64, Option<Vec<u8>>> {
            match self {
                Either::Left(l) => l.collect_ext_info(),
                Either::Right(r) => r.collect_ext_info(),
            }
        }

        fn distribute_ext_info(&mut self, info: &mut HashMap<u64, Option<Vec<u8>>>) {
            match self {
                Either::Left(l) => l.distribute_ext_info(info),
                Either::Right(r) => r.distribute_ext_info(info),
            }
        }
    }

    pub fn negotiate_server<Srv, F>(
        stack: Srv,
        listen_addr: SocketAddrV4,
        per_conn: F,
    ) -> Result<(), Report>
    where
        Srv: Pick + Apply + GetOffers + Clone + Debug + Send + Sync + 'static,
        // main-line branch: Pick on incoming negotiation handshake.
        <Srv as Pick>::Picked:
            NegotiatePicked + Chunnel<UdpChunnelConnection> + Clone + Debug + Send + 'static,
        <<Srv as Pick>::Picked as Chunnel<UdpChunnelConnection>>::Connection: Send + Sync + 'static,
        <<Srv as Pick>::Picked as Chunnel<UdpChunnelConnection>>::Error:
            Into<Report> + Send + Sync + 'static,
        // nonce branch: Apply stack from nonce on indicated connections.
        <Srv as Apply>::Applied: Chunnel<ApplyInput> + Clone + Debug + Send + 'static,
        <<Srv as Apply>::Applied as Chunnel<ApplyInput>>::Connection: Send + Sync + 'static,
        <<Srv as Apply>::Applied as Chunnel<ApplyInput>>::Error:
            Into<Report> + Send + Sync + 'static,
        F: Fn(
                Either<
                    <<Srv as Pick>::Picked as Chunnel<UdpChunnelConnection>>::Connection,
                    <<Srv as Apply>::Applied as Chunnel<ApplyInput>>::Connection,
                >,
            ) + Clone
            + Send
            + Sync
            + 'static,
    {
        let pending_negotiated_connections: Arc<Mutex<HashMap<SocketAddrV4, StackNonce>>> =
            Default::default();
        udp::udp_accept(listen_addr, move |cn| {
            let stack = stack.clone();
            let pending_negotiated_connections = Arc::clone(&pending_negotiated_connections);
            match negotiate_server_connection(cn, stack, pending_negotiated_connections) {
                Ok(Some(cn)) => per_conn(cn),
                _ => (),
            }
        })?;
        Ok(())
    }

    type ApplyInput = InjectOne<UdpChunnelConnection, (SocketAddrV4, Vec<u8>)>;

    fn negotiate_server_connection<Srv>(
        cn: udp::UdpConnection,
        stack: Srv,
        pending_negotiated_connections: Arc<Mutex<HashMap<SocketAddrV4, StackNonce>>>,
    ) -> Result<
        Option<
            Either<
                <<Srv as Pick>::Picked as Chunnel<UdpChunnelConnection>>::Connection,
                <<Srv as Apply>::Applied as Chunnel<ApplyInput>>::Connection,
            >,
        >,
        Report,
    >
    where
        Srv: Pick + Apply + GetOffers + Clone + Debug + Send + Sync + 'static,
        // main-line branch: Pick on incoming negotiation handshake.
        <Srv as Pick>::Picked:
            NegotiatePicked + Chunnel<UdpChunnelConnection> + Clone + Debug + Send + 'static,
        <<Srv as Pick>::Picked as Chunnel<UdpChunnelConnection>>::Connection: Send + Sync + 'static,
        <<Srv as Pick>::Picked as Chunnel<UdpChunnelConnection>>::Error:
            Into<Report> + Send + Sync + 'static,
        // nonce branch: Apply stack from nonce on indicated connections.
        <Srv as Apply>::Applied: Chunnel<ApplyInput> + Clone + Debug + Send + 'static,
        <<Srv as Apply>::Applied as Chunnel<ApplyInput>>::Connection: Send + Sync + 'static,
        <<Srv as Apply>::Applied as Chunnel<ApplyInput>>::Error:
            Into<Report> + Send + Sync + 'static,
    {
        debug!("new connection");
        let a = cn.remote_addr();
        let cn = UdpChunnelConnection(cn);
        loop {
            trace!("listening for potential negotiation pkt");
            let (_, buf) = cn.recv()?;
            trace!("got potential negotiation pkt");

            // if `a` is in pending_negotiated_connections, this is a post-negotiation message and we
            // should return the applied connection.
            let opt_picked = {
                let guard = pending_negotiated_connections.lock().unwrap();
                guard.get(&a).map(Clone::clone)
            };

            if let Some(picked) = opt_picked {
                let (cn, recirculate_mtx) = InjectOne::new(cn);
                let ApplyResult {
                    applied: mut stack, ..
                } = stack
                    .apply(picked.into()) // use check_apply or no?
                    .wrap_err("failed to apply semantics to client connection")?;
                let new_cn = stack.connect_wrap(cn).map_err(Into::into)?;

                debug!(addr = ?&a, stack = ?&stack, "returning pre-negotiated connection");
                *recirculate_mtx.lock().unwrap() = Some((a, buf));
                return Ok(Some(Either::Right(new_cn)));
            }

            debug!(client_addr = ?&a, "address not already negotiated, doing negotiation");

            // else, do negotiation
            let negotiate_msg: NegotiateMsg =
                match bincode::deserialize(&buf).wrap_err("offer deserialize failed") {
                    Ok(m) => m,
                    Err(e) => {
                        debug!(err = %format!("{:#?}", e), ?buf, "Discarding message");
                        continue;
                    }
                };

            use NegotiateMsg::*;
            match negotiate_msg {
                ServerNonce { addr, picked } => {
                    let addr = bincode::deserialize(&addr).wrap_err("mismatched addr types")?;
                    trace!(client_addr = ?&addr, nonce = ?&picked, "got nonce");
                    pending_negotiated_connections
                        .lock()
                        .unwrap()
                        .insert(addr, picked.clone());

                    // send ack
                    let ack = bincode::serialize(&NegotiateMsg::ServerNonceAck).unwrap();
                    cn.send((a, ack))?;
                    debug!("sent nonce ack");

                    // need to loop on this connection, processing nonces
                    if let Err(e) =
                        process_nonces_connection(cn, Arc::clone(&pending_negotiated_connections))
                            .wrap_err("process_nonces_connection")
                    {
                        debug!(err = %format!("{:#}", e), "process_nonces_connection exited");
                        return Ok(None);
                    }

                    unreachable!();
                }
                // one-rtt case
                ClientOffer(client_offers) => {
                    let s = stack.clone();
                    let (new_stack, client_resp) = match monomorphize(
                        s,
                        client_offers.into_iter().map(Into::into).collect(),
                        &a,
                    ) {
                        Ok((mut new_stack, nonce, picked_offers)) => {
                            let nonce_buf = bincode::serialize(&nonce)
                                .wrap_err("Failed to serialize (addr, chosen_stack) nonce")?;

                            new_stack.call_negotiate_picked(&nonce_buf);
                            let ext_info = new_stack.collect_ext_info();
                            let picked_offers = picked_offers
                                .into_iter()
                                .map(|o| {
                                    let mut o: StackNonce = o.into();
                                    o.set_ext_info(ext_info.clone());
                                    o
                                })
                                .collect();

                            (
                                Some(new_stack),
                                NegotiateMsg::ServerReply(Ok(picked_offers)),
                            )
                        }
                        Err(e) => {
                            debug!(err = %format!("{:#}", &e), "negotiation handshake failed");
                            (None, NegotiateMsg::ServerReply(Err(e.to_string())))
                        }
                    };

                    let buf = bincode::serialize(&client_resp)?;
                    assert!(buf.len() < 1500); // response has to fit in a packet
                    cn.send((a, buf))?;
                    debug!("sent client response");
                    if let Some(mut new_stack) = new_stack {
                        debug!(stack = ?&new_stack, "handshake done, picked stack");
                        let new_cn = new_stack.connect_wrap(cn).map_err(Into::into)?;
                        debug!("returning connection");
                        return Ok(Some(Either::Left(new_cn)));
                    } else {
                        continue;
                    }
                }
                // zero-rtt case
                ClientNonce(client_offer) => {
                    let s = stack.clone();
                    let (new_stack, client_resp) =
                        match bertha::negotiate::monomorphize(s, vec![client_offer.into()], &a) {
                            Ok((mut new_stack, nonce, _)) => {
                                let nonce_buf = bincode::serialize(&nonce)
                                    .wrap_err("Failed to serialize (addr, chosen_stack) nonce")?;

                                new_stack.call_negotiate_picked(&nonce_buf);
                                (Some(new_stack), NegotiateMsg::ServerNonceAck)
                            }
                            Err(e) => {
                                debug!(err = %format!("{:#}", &e), "negotiation handshake failed");
                                (
                                    None,
                                    NegotiateMsg::ServerReply(Ok(stack
                                        .offers()
                                        .map(Into::into)
                                        .collect())),
                                )
                            }
                        };

                    let client_resp_buf = bincode::serialize(&client_resp).unwrap();
                    assert!(client_resp_buf.len() < 1500);
                    cn.send((a, client_resp_buf))?;
                    debug!("sent client response");
                    if let Some(mut new_stack) = new_stack {
                        debug!(stack = ?&new_stack, "zero-rtt handshake done, picked stack");
                        let new_cn = new_stack.connect_wrap(cn).map_err(Into::into)?;
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

    fn process_nonces_connection(
        cn: impl ChunnelConnection<Data = (SocketAddrV4, Vec<u8>)>,
        pending_negotiated_connections: Arc<Mutex<HashMap<SocketAddrV4, StackNonce>>>,
    ) -> Result<(), Report> {
        loop {
            trace!("call recv()");
            let (a, buf): (_, Vec<u8>) = cn.recv().wrap_err("conn recv")?;
            let negotiate_msg: NegotiateMsg =
                bincode::deserialize(&buf).wrap_err("offer deserialize failed")?;

            use NegotiateMsg::*;
            match negotiate_msg {
                ServerNonce { addr, picked } => {
                    let addr = bincode::deserialize(&addr).wrap_err("mismatched addr types")?;
                    trace!(client_addr = ?&addr, nonce = ?&picked, "got nonce");
                    pending_negotiated_connections
                        .lock()
                        .unwrap()
                        .insert(addr, picked.clone());

                    // send ack
                    let ack = bincode::serialize(&NegotiateMsg::ServerNonceAck).unwrap();
                    cn.send((a, ack))?;
                    debug!("sent nonce ack");
                }
                x => warn!(msg = ?x, "expected server nonce, got different message"),
            }
        }
    }

    pub type NegotiatedConn<S> =
        <<S as Apply>::Applied as Chunnel<UdpChunnelConnection>>::Connection;

    pub struct ClientNegotiator {
        nonces: HashMap<SocketAddrV4, StackNonce>,
    }

    impl Default for ClientNegotiator {
        fn default() -> Self {
            Self {
                nonces: HashMap::new(),
            }
        }
    }

    impl ClientNegotiator {
        pub fn negotiate_fetch_nonce<S>(
            &mut self,
            stack: S,
            cn: UdpChunnelConnection,
            addr: SocketAddrV4,
        ) -> Result<NegotiatedConn<S>, Report>
        where
            S: Apply + GetOffers + Clone + Send + 'static,
            <S as Apply>::Applied:
                Chunnel<UdpChunnelConnection> + NegotiatePicked + Clone + Debug + Send + 'static,
            <<S as Apply>::Applied as Chunnel<UdpChunnelConnection>>::Error:
                Into<Report> + Send + Sync + 'static,
        {
            let (cn, nonce) = negotiate_client_fetch_nonce(stack, cn, addr)?;
            // the insert could be replacing something here.
            self.nonces.insert(addr, nonce);
            Ok(cn)
        }

        pub fn negotiate_zero_rtt<S>(
            &mut self,
            stack: S,
            cn: UdpChunnelConnection,
            addr: SocketAddrV4,
        ) -> Result<
            <<S as Apply>::Applied as Chunnel<
                CheckZeroRttNegotiationReply<UdpChunnelConnection>,
            >>::Connection,
            Report,
        >
        where
            S: Apply + Clone + Send + 'static,
            <S as Apply>::Applied: Chunnel<CheckZeroRttNegotiationReply<UdpChunnelConnection>>
                + GetOffers
                + NegotiatePicked
                + Clone
                + Debug
                + Send
                + 'static,
            <<S as Apply>::Applied as Chunnel<
                CheckZeroRttNegotiationReply<UdpChunnelConnection>,
            >>::Connection: Send,
            <<S as Apply>::Applied as Chunnel<
                CheckZeroRttNegotiationReply<UdpChunnelConnection>,
            >>::Error: Into<Report> + Send + Sync + 'static,
        {
            let nonce = self
                .nonces
                .get(&addr)
                .ok_or_else(|| eyre!("No nonce found for addr"))?;
            negotiate_client_nonce(stack, cn, nonce.clone(), addr)
        }

        pub fn re_negotiate<S>(
            &mut self,
            stack: S,
            cn: UdpChunnelConnection,
            addr: SocketAddrV4,
            returned_error: Report,
        ) -> Result<
            <<S as Apply>::Applied as Chunnel<
                CheckZeroRttNegotiationReply<UdpChunnelConnection>,
            >>::Connection,
            Report,
        >
        where
            S: Apply + Pick + GetOffers + Clone + Debug + Send + 'static,
            <S as Apply>::Applied: Chunnel<CheckZeroRttNegotiationReply<UdpChunnelConnection>>
                + GetOffers
                + NegotiatePicked
                + Clone
                + Debug
                + Send
                + 'static,
            <<S as Apply>::Applied as Chunnel<
                CheckZeroRttNegotiationReply<UdpChunnelConnection>,
            >>::Connection: Send,
            <<S as Apply>::Applied as Chunnel<
                CheckZeroRttNegotiationReply<UdpChunnelConnection>,
            >>::Error: Into<Report> + Send + Sync + 'static,
            <S as Pick>::Picked: Debug,
        {
            let returned_error: ZeroRttNegotiationError = returned_error.downcast().wrap_err(eyre!("Renegotation only works with an error returned from a connection returned by negotiate_zero_rtt"))?;
            let nonces = match returned_error {
                ZeroRttNegotiationError::NotAccepted(nonces) => nonces,
                e => bail!("Non-negotiation error: {}", e),
            };

            let picked = client_monomorphize(&stack, nonces)?;
            self.nonces.insert(addr.clone(), picked.clone());
            negotiate_client_nonce(stack, cn, picked, addr)
        }
    }

    /// Return a connection with `stack`'s semantics, connecting to `a`.
    ///
    /// This is the traditional "one-rtt" version. It will block until the remote end completes the
    /// negotiation handshake.
    #[allow(clippy::manual_async_fn)] // we need the + 'static which async fn does not do.
    pub fn negotiate_client<S>(
        stack: S,
        cn: UdpChunnelConnection,
        addr: SocketAddrV4,
    ) -> Result<NegotiatedConn<S>, Report>
    where
        S: Apply + GetOffers + Clone + Send + 'static,
        <S as Apply>::Applied:
            Chunnel<UdpChunnelConnection> + NegotiatePicked + Clone + Debug + Send + 'static,
        <<S as Apply>::Applied as Chunnel<UdpChunnelConnection>>::Error:
            Into<Report> + Send + Sync + 'static,
    {
        let (cn, _) = negotiate_client_fetch_nonce(stack, cn, addr)?;
        Ok(cn)
    }

    /// Same as [`negotiate_client`], but also return the [`StackNonce`].
    pub fn negotiate_client_fetch_nonce<S>(
        stack: S,
        cn: UdpChunnelConnection,
        addr: SocketAddrV4,
    ) -> Result<(NegotiatedConn<S>, StackNonce), Report>
    where
        S: Apply + GetOffers + Clone + Send + 'static,
        <S as Apply>::Applied:
            Chunnel<UdpChunnelConnection> + NegotiatePicked + Clone + Debug + Send + 'static,
        <<S as Apply>::Applied as Chunnel<UdpChunnelConnection>>::Error:
            Into<Report> + Send + Sync + 'static,
    {
        debug!(?addr, "client negotiation starting");
        let offers = NegotiateMsg::ClientOffer(stack.offers().map(Into::into).collect());
        let resp = try_negotiate_offer_loop(cn.clone(), addr.clone(), offers)?;
        match resp {
            NegotiateMsg::ServerReply(Ok(picked)) => {
                // 4. monomorphize `stack`, picking received choices
                trace!(?picked, "received server pairs, applying");
                let mut sc = 0;
                let mut applied = None;
                let mut apply_err = eyre!("Apply error");
                for p in picked {
                    let p2 = p.clone();
                    match bertha::negotiate::check_apply(stack.clone(), p.into())
                        .wrap_err(eyre!("Could not apply received impls to stack"))
                    {
                        Ok(ApplyResult {
                            applied: ns,
                            score: p_sc,
                            ..
                        }) => {
                            // TODO what if two options are tied? This will arbitrarily pick the first.
                            if p_sc > sc || applied.is_none() {
                                sc = p_sc;
                                applied = Some((ns, p2));
                            }
                        }
                        Err(e) => {
                            debug!(err = %format!("{:#}", e), "Apply attempt failed");
                            apply_err = apply_err.wrap_err(e);
                            continue;
                        }
                    }
                }

                let (mut applied, nonce) = applied.ok_or_else(|| {
                    apply_err.wrap_err(eyre!("All received options failed to apply"))
                })?;
                debug!(?applied, "applied to stack");
                let inform_picked_nonce_buf = bincode::serialize(&NegotiateMsg::ServerNonce {
                    addr: bincode::serialize(&addr)?,
                    picked: nonce.clone(),
                })?;
                applied.call_negotiate_picked(&inform_picked_nonce_buf);

                let mut inform_extinfo = nonce.1.clone();
                applied.distribute_ext_info(&mut inform_extinfo);

                // 5. return applied.connect_wrap(vec_u8_conn)
                Ok((applied.connect_wrap(cn).map_err(Into::into)?, nonce))
            }
            NegotiateMsg::ServerReply(Err(errmsg)) => Err(eyre!("{:?}", errmsg)),
            _ => Err(eyre!("Received unknown message type")),
        }
    }

    /// Similar to `negotiate_client_nonce`, but the nonce is implicit.
    ///
    /// The nonce in this case is determined by inspecting the provided `stack`. The stack must be
    /// fully-determined, i.e., it cannot have any [`Select`]s. If it does, this will error.
    pub fn negotiate_client_fixed_stack<S>(
        mut stack: S,
        cn: UdpChunnelConnection,
        addr: SocketAddrV4,
    ) -> Result<
        <S as Chunnel<CheckZeroRttNegotiationReply<UdpChunnelConnection>>>::Connection,
        Report,
    >
    where
        S: Chunnel<CheckZeroRttNegotiationReply<UdpChunnelConnection>>
            + GetOffers
            + NegotiatePicked
            + Clone
            + Debug
            + Send
            + 'static,
        <S as Chunnel<CheckZeroRttNegotiationReply<UdpChunnelConnection>>>::Error:
            Into<Report> + Send + Sync + 'static,
    {
        let nonce = {
            let mut offers = stack.offers();
            let nonce: StackNonce = offers
                .next()
                .ok_or_else(|| eyre!("No StackNonce available for {:?}", stack))
                .map(Into::into)?;
            if offers.next().is_some() {
                bail!("Stack should not have Selects: {:?}", stack);
            }

            nonce
        };

        let msg = NegotiateMsg::ClientNonce(nonce.clone());
        let buf = bincode::serialize(&msg)?;
        cn.send((addr, buf))?;

        let picked = NegotiateMsg::ServerNonce {
            addr: bincode::serialize(&addr)?,
            picked: nonce,
        };
        let inform_picked_nonce_buf = bincode::serialize(&picked)?;
        stack.call_negotiate_picked(&inform_picked_nonce_buf);

        stack
            .connect_wrap(CheckZeroRttNegotiationReply::from(cn))
            .map_err(Into::into)
    }

    /// Pass an existing `nonce` to get a "zero-rtt" negotiated connection that be be used immediately.
    ///
    /// The connection might return an error later if `nonce` was incompatible with the other side.
    /// This error will be `.downcast`-able to a [`ZeroRttNegotiationError`], which will contain the
    /// list of valid remote nonces.
    /// From this, callers can monomorphize a new nonce and try again with this function.
    pub fn negotiate_client_nonce<S>(
        stack: S,
        cn: UdpChunnelConnection,
        nonce: StackNonce,
        addr: SocketAddrV4,
    ) -> Result<
        <<S as Apply>::Applied as Chunnel<CheckZeroRttNegotiationReply<UdpChunnelConnection>>>::Connection,
        Report,
    >
    where
        S: Apply + Clone + Send + 'static,
        <S as Apply>::Applied: Chunnel<CheckZeroRttNegotiationReply<UdpChunnelConnection>>
            + GetOffers
            + NegotiatePicked
            + Clone
            + Debug
            + Send
            + 'static,
        <<S as Apply>::Applied as Chunnel<CheckZeroRttNegotiationReply<UdpChunnelConnection>>>::Connection: Send,
        <<S as Apply>::Applied as Chunnel<CheckZeroRttNegotiationReply<UdpChunnelConnection>>>::Error:
            Into<Report> + Send + Sync + 'static,
    {
        let ApplyResult { applied, .. } = stack.apply(nonce.into())?;
        negotiate_client_fixed_stack(applied, cn, addr)
    }

    /// Pick a nonce from a stack and `server_offers` which is returned in [`ZeroRttNegotiationError`].
    pub fn client_monomorphize<S>(
        stack: &S,
        server_offers: Vec<StackNonce>,
    ) -> Result<StackNonce, Report>
    where
        S: Pick + GetOffers + Clone + Debug + Send + 'static,
        <S as Pick>::Picked: Debug,
    {
        match monomorphize(
            stack.clone(),
            server_offers.into_iter().map(Into::into).collect(),
            &String::new(),
        )? {
            (_, bertha::NegotiateMsg::ServerNonce { picked, .. }, _) => Ok(picked.into()),
            _ => unreachable!(),
        }
    }

    fn try_negotiate_offer_loop(
        cn: UdpChunnelConnection,
        addr: SocketAddrV4,
        offer: NegotiateMsg,
    ) -> Result<NegotiateMsg, Report> {
        use shenango::poll::PollWaiter;
        let buf = bincode::serialize(&offer)?;
        loop {
            let mut poll = PollWaiter::new();
            let sleep_trigger = poll.trigger(Box::new(Either::Left(())));
            let send_trigger = poll.trigger(Box::new(Either::Right(())));

            shenango::thread::spawn_detached(move || {
                let _sleep_trigger = sleep_trigger;
                shenango::sleep(std::time::Duration::from_millis(2_000));
                // drop sleep_trigger
            });

            let cn_try = cn.clone();
            let b = buf.clone();
            let work = shenango::thread::spawn(move || {
                let _send_trigger = send_trigger;
                let x = try_once(cn_try, addr, b);
                x // drop send_trigger
            });

            match *poll.wait() {
                Either::Right(_) => {
                    return work.join().unwrap();
                }
                Either::Left(_) => {
                    debug!("negotiate offer timed out");
                    continue;
                }
            }
        }

        fn try_once(
            cn: UdpChunnelConnection,
            addr: SocketAddrV4,
            buf: Vec<u8>,
        ) -> Result<NegotiateMsg, Report> {
            // 2. send offers
            cn.send((addr, buf))?;

            // 3. receive picked
            let (_, rbuf) = cn.recv()?;
            bincode::deserialize(&rbuf).wrap_err(eyre!(
                "Could not deserialize negotiate_server response: {:?}",
                rbuf
            ))
        }
    }

    /// Ensures safety for zero-rtt negotiation.
    ///
    /// On recv, errors if the received message (a response to something that was sent) is a
    /// negotiation response. This only would happen in the error case, so we just return the error
    /// [`ZeroRttNegotiationError`] - can downcast the [`Report`] to get it.
    pub struct CheckZeroRttNegotiationReply<C> {
        inner: C,
        success: Arc<AtomicBool>,
    }

    impl<C> From<C> for CheckZeroRttNegotiationReply<C> {
        fn from(inner: C) -> Self {
            Self {
                inner,
                success: Default::default(),
            }
        }
    }

    impl<C> ChunnelConnection for CheckZeroRttNegotiationReply<C>
    where
        C: ChunnelConnection<Data = (SocketAddrV4, Vec<u8>)>,
    {
        type Data = (SocketAddrV4, Vec<u8>);

        fn send(&self, data: Self::Data) -> Result<(), Report> {
            self.inner.send(data)
        }

        fn recv(&self) -> Result<Self::Data, Report> {
            if !self.success.load(Ordering::SeqCst) {
                let (addr, data) = self.inner.recv()?;
                // try parsing a NegotiateMsg::ServerReply
                let m: Result<NegotiateMsg, _> = bincode::deserialize(&data);
                match m {
                    Ok(NegotiateMsg::ServerNonceAck) => {
                        trace!("zero-rtt negotiation succeeded");
                        self.success.as_ref().store(true, Ordering::SeqCst);
                        // the next recv will get application data.
                        // this is safe because we set success to true, so a subsequent call
                        // to this function would skip negotiaton logic anyway
                        return self.inner.recv();
                    }
                    Err(e) => {
                        // do NOT set success because we still need to listen for the
                        // ServerNonceAck, but getting a non-`NegotiateMsg` means that negotiaton
                        // succeeded but there was some reordering.
                        //
                        // This is because the negotiaton server will only return a connection to
                        // the application if negotiaton succeeded, and if it did that then it will
                        // have sent the ServerNonceAck.
                        //
                        // This assumes that:
                        // 1. client and server both implement the negotiaton protocol correctly.
                        // 2. the error `e` is actually a deserialization error and not some other
                        //    error from the base connection.
                        //    TODO check the error type
                        trace!(err = %format!("{:#}", e), ?data, "return reordered message");
                        Ok((addr, data))
                    }
                    Ok(NegotiateMsg::ServerReply(Ok(options))) => {
                        bail!(ZeroRttNegotiationError::NotAccepted(options))
                    }
                    Ok(NegotiateMsg::ServerReply(Err(s))) => {
                        bail!(ZeroRttNegotiationError::UnexpectedError(s))
                    }
                    Ok(m) => bail!(ZeroRttNegotiationError::UnexpectedResponse(m)),
                }
            } else {
                self.inner.recv()
            }
        }
    }

    #[derive(Debug, Clone)]
    pub enum ZeroRttNegotiationError {
        NotAccepted(Vec<StackNonce>),
        UnexpectedResponse(NegotiateMsg),
        UnexpectedError(String),
    }

    impl std::fmt::Display for ZeroRttNegotiationError {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
            use ZeroRttNegotiationError::*;
            match self {
                NotAccepted(_) => f.write_str("Negotiation nonce was not accepted"),
                UnexpectedError(m) => {
                    let e = format!("Unexpected negotiation response: {:?}", m);
                    f.write_str(&e)
                }
                UnexpectedResponse(m) => {
                    let e = format!("Unexpected negotiation response: {:?}", m);
                    f.write_str(&e)
                }
            }
        }
    }
}

pub use chunnels::{
    ClientShardChunnelClient, ClientShardClientConnection, ShardCanonicalServer,
    ShardCanonicalServerConnection,
};
pub use chunnels::{KvReliability, KvReliabilityChunnel, KvReliabilityServerChunnel};
pub use chunnels::{SerializeChunnelProject, SerializeProject};
mod chunnels {
    use super::base_traits::{Chunnel, ChunnelConnection};
    use crate::negotiate::StackNonce;
    use color_eyre::eyre::{eyre, Report, WrapErr};
    use shenango::poll::PollWaiter;
    use shenango::sync::Mutex;
    use std::fmt::Debug;
    use std::net::SocketAddrV4;
    use std::sync::Arc;
    use std::time::Duration;
    use tracing::{debug, trace, warn};

    #[derive(Debug, Clone)]
    pub struct SerializeChunnelProject<D> {
        _data: std::marker::PhantomData<D>,
    }

    impl<D> bertha::Negotiate for SerializeChunnelProject<D> {
        type Capability = ();

        fn guid() -> u64 {
            0xd5263bf239e761c3
        }
    }

    impl<D> super::negotiate::Negotiate for SerializeChunnelProject<D> {}

    impl<D> Default for SerializeChunnelProject<D> {
        fn default() -> Self {
            SerializeChunnelProject {
                _data: Default::default(),
            }
        }
    }

    impl<A, D, InC> Chunnel<InC> for SerializeChunnelProject<D>
    where
        InC: ChunnelConnection<Data = (A, Vec<u8>)>,
        D: serde::Serialize + serde::de::DeserializeOwned,
    {
        type Connection = SerializeProject<A, D, InC>;
        type Error = std::convert::Infallible;

        fn connect_wrap(&mut self, cn: InC) -> Result<Self::Connection, Self::Error> {
            Ok(SerializeProject::from(cn))
        }
    }

    #[derive(Default, Debug, Clone)]
    pub struct SerializeProject<A, D, C> {
        inner: C,
        _data: std::marker::PhantomData<(A, D)>,
    }

    impl<Cx, A, D> From<Cx> for SerializeProject<A, D, Cx> {
        fn from(inner: Cx) -> SerializeProject<A, D, Cx> {
            debug!(data = ?std::any::type_name::<D>(), "make serialize chunnel");
            SerializeProject {
                inner,
                _data: Default::default(),
            }
        }
    }

    impl<A, C, D> ChunnelConnection for SerializeProject<A, D, C>
    where
        C: ChunnelConnection<Data = (A, Vec<u8>)>,
        D: serde::Serialize + serde::de::DeserializeOwned,
    {
        type Data = (A, D);

        fn send(&self, data: Self::Data) -> Result<(), Report> {
            let buf = bincode::serialize(&data.1).wrap_err("serialize failed")?;
            self.inner.send((data.0, buf))
        }

        fn recv(&self) -> Result<Self::Data, Report> {
            let (a, buf) = self.inner.recv()?;
            trace!("recvd");
            let data = bincode::deserialize(&buf[..]).wrap_err(eyre!(
                "deserialize failed: {:?} -> {:?}",
                buf,
                std::any::type_name::<D>()
            ))?;
            Ok((a, data))
        }
    }

    use bertha::reliable::ReliabilityNeg;

    #[derive(Debug, Clone)]
    pub struct KvReliabilityChunnel {
        timeout: Duration,
    }

    impl bertha::Negotiate for KvReliabilityChunnel {
        type Capability = ReliabilityNeg;

        fn guid() -> u64 {
            0xa84943c6f0ce1b78
        }

        fn capabilities() -> Vec<Self::Capability> {
            vec![ReliabilityNeg::Reliability, ReliabilityNeg::Ordering]
        }
    }

    impl super::Negotiate for KvReliabilityChunnel {}

    impl Default for KvReliabilityChunnel {
        fn default() -> Self {
            KvReliabilityChunnel {
                timeout: Duration::from_millis(500),
            }
        }
    }

    impl KvReliabilityChunnel {
        pub fn set_timeout(&mut self, to: Duration) -> &mut Self {
            self.timeout = to;
            self
        }
    }

    impl<A, InC, D> Chunnel<InC> for KvReliabilityChunnel
    where
        InC: ChunnelConnection<Data = (A, D)> + Send + Sync + 'static,
        A: Clone + Eq + std::hash::Hash + std::fmt::Debug + 'static,
        D: MsgId + Clone + 'static,
    {
        type Connection = KvReliability<InC, A, D>;
        type Error = std::convert::Infallible;

        fn connect_wrap(&mut self, cn: InC) -> Result<Self::Connection, Self::Error> {
            Ok(KvReliability::new(cn, self.timeout))
        }
    }

    #[derive(Debug, Clone, Default)]
    pub struct KvReliabilityServerChunnel;

    impl bertha::Negotiate for KvReliabilityServerChunnel {
        type Capability = ReliabilityNeg;

        fn guid() -> u64 {
            0xa84943c6f0ce1b78
        }

        fn capabilities() -> Vec<Self::Capability> {
            vec![ReliabilityNeg::Reliability, ReliabilityNeg::Ordering]
        }
    }

    impl super::Negotiate for KvReliabilityServerChunnel {}

    impl<D, InC> Chunnel<InC> for KvReliabilityServerChunnel
    where
        InC: ChunnelConnection<Data = D>,
    {
        type Connection = InC;
        type Error = std::convert::Infallible;

        fn connect_wrap(&mut self, cn: InC) -> Result<Self::Connection, Self::Error> {
            Ok(cn)
        }
    }

    #[derive(Clone)]
    pub struct KvReliability<C, A, D> {
        inner: Arc<C>,
        timeout: Duration,
        sends: Arc<dashmap::DashSet<usize>>,
        recvs: Arc<Mutex<Vec<(A, D)>>>,
    }

    impl<C, A, D> KvReliability<C, A, D> {
        fn new(inner: C, timeout: Duration) -> Self {
            KvReliability {
                inner: Arc::new(inner),
                timeout,
                sends: Default::default(),
                recvs: Default::default(),
            }
        }
    }

    use bertha::util::MsgId;
    impl<A, D, C> ChunnelConnection for KvReliability<C, A, D>
    where
        C: ChunnelConnection<Data = (A, D)> + Send + Sync + 'static,
        A: Clone + 'static,
        D: MsgId + Clone + 'static,
    {
        type Data = (A, D);

        fn send(&self, data: Self::Data) -> Result<(), Report> {
            enum PollBranch {
                Timeout,
                Snd,
                Rcv,
            }

            let msg_id = data.1.id();
            let to = self.timeout;
            self.sends.insert(msg_id);

            // blocks, but will finish fast.
            self.inner.send(data.clone())?;

            // there are 3 tasks. when this function returns, all 3 of them *must* exit (or have
            // already exited) shortly afterwards.
            let mut poll = PollWaiter::new();
            // 1. wait for the send to finish. this is what lets us return
            let sendcomplete_trigger = poll.trigger(Box::new(PollBranch::Snd));
            // 2. if a timeout happens, retransmit.
            let timeout_trigger = poll.trigger(Box::new(PollBranch::Timeout));
            // 3. handle receives and notify the corresponding senders.
            let recv_trigger = poll.trigger(Box::new(PollBranch::Rcv));

            let sends = Arc::clone(&self.sends);
            shenango::thread::spawn(move || {
                let _send = sendcomplete_trigger;
                loop {
                    match sends.get(&msg_id) {
                        None => break,
                        _ => shenango::thread::thread_yield(),
                    }
                }
            });

            let recv_cn = Arc::clone(&self.inner);
            let recvs = Arc::clone(&self.recvs);
            let sends = Arc::clone(&self.sends);
            let recv_jh = shenango::thread::spawn(move || {
                let _recv = recv_trigger;
                loop {
                    let resp = recv_cn.recv().wrap_err("kvreliability recv acks")?;
                    let recv_msg_id = resp.1.id();

                    // signal recv
                    recvs.lock().push(resp);
                    sends.remove(&recv_msg_id);

                    if recv_msg_id == msg_id || sends.remove(&msg_id).is_none() {
                        return Ok(());
                    }
                }
            });

            shenango::thread::spawn_detached(move || {
                let _to = timeout_trigger;
                shenango::sleep(to);
                // drop sleep_trigger
            });

            loop {
                match *poll.wait() {
                    PollBranch::Timeout => {
                        let timeout_trigger = poll.trigger(Box::new(PollBranch::Timeout));
                        shenango::thread::spawn_detached(move || {
                            let _to = timeout_trigger;
                            shenango::sleep(to);
                            // drop sleep_trigger
                        });

                        continue;
                    }
                    PollBranch::Rcv => {
                        return recv_jh.join().unwrap();
                    }
                    PollBranch::Snd => {
                        return Ok(());
                    }
                }
            }
        }

        fn recv(&self) -> Result<Self::Data, Report> {
            loop {
                if let Some(d) = self.recvs.lock().pop() {
                    return Ok(d);
                } else {
                    shenango::thread::thread_yield();
                }
            }
        }
    }

    use super::base_traits::ChunnelConnector;
    use super::negotiate::NegotiateMsg;

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    pub struct ShardInfo {
        pub canonical_addr: SocketAddrV4,
        pub shard_addrs: Vec<SocketAddrV4>,
    }

    #[derive(Clone)]
    pub struct ShardCanonicalServer<S, Ss, D> {
        addr: ShardInfo,
        internal_addr: Vec<SocketAddrV4>,
        shards_inner: S,
        shards_inner_stack: Ss,
        shards_extern_nonce: StackNonce,
        _phantom: std::marker::PhantomData<D>,
    }

    impl<S, Ss, D> std::fmt::Debug for ShardCanonicalServer<S, Ss, D> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ShardCanonicalServer")
                .field("addr", &self.addr)
                .finish()
        }
    }

    impl<S, Ss, D> ShardCanonicalServer<S, Ss, D> {
        /// Inner is a chunnel for the external connection.
        /// Shards is a chunnel for an internal connection to the shards.
        pub fn new(
            addr: ShardInfo,
            internal_addr: Option<Vec<SocketAddrV4>>,
            shards_inner: S,
            shards_inner_stack: Ss,
            shards_extern_nonce: StackNonce,
        ) -> Result<Self, Report> {
            let internal_addr = internal_addr.unwrap_or_else(|| addr.shard_addrs.clone());
            if internal_addr.len() != addr.shard_addrs.len() {
                return Err(eyre!(
                    "Shard addresses mismatched between internal and external: {:?} != {:?}",
                    internal_addr,
                    addr
                ));
            }

            Ok(ShardCanonicalServer {
                addr,
                internal_addr,
                shards_inner,
                shards_inner_stack,
                shards_extern_nonce,
                _phantom: Default::default(),
            })
        }
    }

    impl<S, Ss, D> bertha::Negotiate for ShardCanonicalServer<S, Ss, D>
    where
        S: ChunnelConnector<Addr = SocketAddrV4> + Clone + Send + Sync + 'static,
        <S as ChunnelConnector>::Error: Into<Report>,
        S::Connection: ChunnelConnection<Data = (SocketAddrV4, Vec<u8>)> + Send + Sync + 'static,
    {
        type Capability = ShardFns;

        fn guid() -> u64 {
            0xe91d00534cb2b98f
        }

        fn capabilities() -> Vec<ShardFns> {
            vec![ShardFns::Sharding]
        }
    }

    impl<S, Ss, D> super::Negotiate for ShardCanonicalServer<S, Ss, D>
    where
        S: ChunnelConnector<Addr = SocketAddrV4> + Clone + Send + Sync + 'static,
        <S as ChunnelConnector>::Error: Into<Report>,
        S::Connection: ChunnelConnection<Data = (SocketAddrV4, Vec<u8>)> + Send + Sync + 'static,
    {
        fn picked_blocking(&mut self, nonce: &[u8]) {
            let offer = self.shards_extern_nonce.clone();
            let msg: NegotiateMsg = match bincode::deserialize(nonce) {
                Err(e) => {
                    warn!(err = ?e, "deserialize failed");
                    return;
                }
                Ok(m) => m,
            };

            let msg = match msg {
                NegotiateMsg::ServerNonce { addr, .. } => NegotiateMsg::ServerNonce {
                    addr,
                    picked: offer,
                },
                _ => {
                    warn!("malformed nonce");
                    return;
                }
            };

            let buf = match bincode::serialize(&msg) {
                Err(e) => {
                    warn!(err = ?e, "serialize failed");
                    return;
                }
                Ok(m) => m,
            };

            let wg = shenango::sync::WaitGroup::new();
            for shard in &self.internal_addr {
                let mut ctr = self.shards_inner.clone();
                let shard = shard.clone();
                let buf = buf.clone();
                wg.add(1);
                let wg = wg.clone();
                shenango::thread::spawn(move || {
                    let cn = match ctr.connect(shard.clone()) {
                        Ok(c) => c,
                        Err(e) => {
                            let e = e.into();
                            warn!(err = ?e, "failed making connection to shard");
                            wg.done();
                            return;
                        }
                    };

                    if let Err(e) = cn.send((shard.clone(), buf.clone())) {
                        warn!(err = ?e, "failed sending negotiation nonce to shard");

                        wg.done();
                        return;
                    }

                    trace!("wait for nonce ack");
                    match cn.recv() {
                        Ok((a, buf)) => match bincode::deserialize(&buf) {
                            Ok(NegotiateMsg::ServerNonceAck) => {
                                // TODO collect received addresses since we could receive acks from
                                // any shard
                                if a != shard.clone() {
                                    warn!(addr = ?a, expected = ?shard.clone(), "received from unexpected address");
                                }

                                trace!("got nonce ack");
                            }
                            Ok(m) => {
                                warn!(msg = ?m, shard = ?shard.clone(), "got unexpected response to nonce");
                            }
                            Err(e) => {
                                warn!(err = ?e, shard = ?shard.clone(), "failed deserializing nonce ack");
                            }
                        },
                        Err(e) => {
                            warn!(err = ?e, shard = ?shard.clone(), "failed waiting for nonce ack");
                        }
                    }

                    wg.done();
                });
            }

            wg.wait();
        }

        fn ext_info(&self) -> Option<Vec<u8>> {
            let shard_info_bytes = bincode::serialize(&self.addr).unwrap();
            Some(shard_info_bytes)
        }
    }

    use super::udp::UdpChunnelConnection;
    use bertha::negotiate::{Apply, GetOffers};
    use burrito_shard_ctl::Kv;
    const FNV1_64_INIT: u64 = 0xcbf29ce484222325u64;
    const FNV_64_PRIME: u64 = 0x100000001b3u64;

    pub type NegotiatedConn<C, S> = <<S as Apply>::Applied as Chunnel<C>>::Connection;
    impl<I, S, Ss, D, E> Chunnel<I> for ShardCanonicalServer<S, Ss, D>
    where
        I: ChunnelConnection<Data = D> + Send + Sync + 'static,
        S: ChunnelConnector<Addr = SocketAddrV4, Error = E, Connection = UdpChunnelConnection>
            + Clone
            + Send
            + Sync
            + 'static,
        Ss: Apply + GetOffers + Clone + Send + Sync + 'static,
        <Ss as Apply>::Applied: Chunnel<UdpChunnelConnection>
            + super::negotiate::NegotiatePicked
            + Clone
            + Debug
            + Send
            + 'static,
        <<Ss as Apply>::Applied as Chunnel<UdpChunnelConnection>>::Connection:
            ChunnelConnection<Data = D> + Send + Sync + 'static,
        <<Ss as Apply>::Applied as Chunnel<UdpChunnelConnection>>::Error:
            Into<Report> + Send + Sync + 'static,
        D: Kv + Send + Sync + 'static,
        <D as Kv>::Key: AsRef<str>,
        E: Into<Report> + Send + Sync + 'static,
    {
        type Connection =
            ShardCanonicalServerConnection<I, NegotiatedConn<UdpChunnelConnection, Ss>, D>;
        type Error = Report;

        fn connect_wrap(&mut self, conn: I) -> Result<Self::Connection, Self::Error> {
            let addr = self.addr.clone();
            let internal_addr = self.internal_addr.clone();
            let shards_inner = self.shards_inner.clone();
            let shards_inner_stack = self.shards_inner_stack.clone();
            let num_shards = addr.shard_addrs.len();

            // connect to shards
            let conns: Vec<Arc<_>> = internal_addr
                .into_iter()
                .map(|a| {
                    debug!(?a, "connecting to shard");
                    let a = a;
                    let cn = shards_inner
                        .clone()
                        .connect(a.clone())
                        .map_err(Into::into)
                        .wrap_err(eyre!("Could not connect to {}", a.clone()))?;
                    let cn = super::negotiate_client(shards_inner_stack.clone(), cn, a.clone())
                        .wrap_err("negotiate_client failed")?;
                    Ok::<_, Report>(Arc::new(cn))
                })
                .collect::<Result<_, Report>>()?;
            trace!("connected to shards");

            // serve canonical address
            Ok(ShardCanonicalServerConnection {
                inner: Arc::new(conn),
                shard_conns: conns,
                shard_fn: Arc::new(move |d: &D| {
                    let mut hash = FNV1_64_INIT;
                    for b in d.key().as_ref().as_bytes()[0..4].iter() {
                        hash ^= *b as u64;
                        hash = u64::wrapping_mul(hash, FNV_64_PRIME);
                    }

                    hash as usize % num_shards
                }),
            })
        }
    }

    pub struct ShardCanonicalServerConnection<C, S, D> {
        inner: Arc<C>,
        shard_conns: Vec<Arc<S>>,
        shard_fn: Arc<dyn Fn(&D) -> usize + Send + Sync + 'static>,
    }

    impl<C, S, D> ChunnelConnection for ShardCanonicalServerConnection<C, S, D>
    where
        C: ChunnelConnection<Data = D> + Send + Sync + 'static,
        S: ChunnelConnection<Data = D> + Send + Sync + 'static,
        D: Send + Sync + 'static,
    {
        type Data = ();

        fn recv(&self) -> Result<Self::Data, Report> {
            // received a packet on the canonical_addr.
            // need to
            // 1. evaluate the hash fn
            // 2. forward to the right shard,
            //    preserving the src addr so the response goes back to the client

            // 0. receive the packet.
            let data = self.inner.recv()?;
            trace!("got request");

            // 1. evaluate the hash fn to determine where to forward to.
            let shard_idx = (self.shard_fn)(&data);
            let conn = &self.shard_conns[shard_idx];
            trace!(shard_idx, "checked shard");

            // 2. Forward to the shard.
            // TODO this assumes no reordering.
            conn.send(data).wrap_err("Forward to shard")?;
            trace!(shard_idx, "wait for shard response");

            // 3. Get response from the shard, and send back to client.
            let resp = conn.recv().wrap_err("Receive from shard")?;
            trace!(shard_idx, "got shard response");
            self.inner.send(resp)?;
            trace!(shard_idx, "send response");
            Ok(())
        }

        fn send(&self, _data: Self::Data) -> Result<(), Report> {
            warn!("Called ShardCanonicalServerConnection.send(), a useless function");
            Ok(())
        }
    }

    /// Client-side sharding chunnel implementation.
    ///
    /// Contacts shard-ctl for sharding information, and does client-side sharding.
    #[derive(Clone)]
    pub struct ClientShardChunnelClient {
        addr: SocketAddrV4,
        shard_info: Option<Vec<SocketAddrV4>>,
    }

    impl std::fmt::Debug for ClientShardChunnelClient {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ClientShardChunnelClient")
                .field("addr", &self.addr)
                .finish()
        }
    }

    impl ClientShardChunnelClient {
        pub fn new(addr: SocketAddrV4) -> Result<Self, Report> {
            Ok(ClientShardChunnelClient {
                addr,
                shard_info: None,
            })
        }
    }

    use burrito_shard_ctl::ShardFns;

    impl bertha::Negotiate for ClientShardChunnelClient {
        type Capability = ShardFns;

        fn guid() -> u64 {
            0xafb32251f0697831
        }

        fn capabilities() -> Vec<ShardFns> {
            vec![ShardFns::Sharding]
        }
    }

    impl super::Negotiate for ClientShardChunnelClient {
        fn ext_info_callback(&mut self, info: Vec<u8>) {
            //let redis_conn = Arc::clone(&self.redis_listen_connection);
            //trace!("query redis");
            //// query redis for si
            //let si = redis_util::redis_query(&a, redis_conn.lock().await)
            //    .await
            //    .wrap_err("redis query failed")?;
            //
            //    self.shard_info = Some(si);
            //let addrs = match si {
            //    None => vec![a.into()],
            //    Some(si) => {
            //        let mut addrs = vec![si.canonical_addr.into()];
            //        addrs.extend(si.shard_addrs.into_iter().map(Into::into));
            //        addrs
            //    }
            //};

            let si: ShardInfo = match bincode::deserialize(&info) {
                Ok(si) => si,
                Err(e) => {
                    warn!(err = ?e, "Could not deserialize extinfo");
                    return;
                }
            };

            let mut addrs = vec![si.canonical_addr.into()];
            addrs.extend(si.shard_addrs.into_iter());

            debug!(addrs = ?&addrs, "Decided sharding plan");
            self.shard_info = Some(addrs);
        }
    }

    impl<I, D> Chunnel<I> for ClientShardChunnelClient
    where
        I: ChunnelConnection<Data = (SocketAddrV4, D)> + Send + Sync + 'static,
        D: Kv + Send + Sync + 'static,
        <D as Kv>::Key: AsRef<str>,
    {
        type Connection = ClientShardClientConnection<D, I>;
        type Error = Report;

        // implementing this is tricky - we have been given one connection with some semantics,
        // but we need n connections, to each shard.
        // 1. ignore the connection (to the canonical_addr) to establish our own, to the shards.
        // 2. force the connection to have (addr, data) semantics, so we can do routing without
        //    establishing our own connections
        //
        //  -> pick #2
        fn connect_wrap(&mut self, inner: I) -> Result<Self::Connection, Self::Error> {
            let addrs = self
                .shard_info
                .clone()
                .ok_or_else(|| eyre!("Shard info was not set"))?;
            let num_shards = addrs.len() - 1;
            Ok(ClientShardClientConnection::new(inner, addrs, move |d| {
                if num_shards == 0 {
                    return 0;
                }

                let mut hash = FNV1_64_INIT;
                for b in d.key().as_ref().as_bytes()[0..4].iter() {
                    hash ^= *b as u64;
                    hash = u64::wrapping_mul(hash, FNV_64_PRIME);
                }

                (hash as usize % num_shards) + 1
            }))
        }
    }

    /// `ChunnelConnection` type for ClientShardChunnelClient.
    pub struct ClientShardClientConnection<D, C>
    where
        C: ChunnelConnection<Data = (SocketAddrV4, D)>,
    {
        inner: Arc<C>,
        shard_addrs: Vec<SocketAddrV4>,
        shard_fn: Arc<dyn Fn(&D) -> usize + Send + Sync + 'static>,
    }

    impl<C, D> ClientShardClientConnection<D, C>
    where
        C: ChunnelConnection<Data = (SocketAddrV4, D)>,
    {
        pub fn new(
            inner: C,
            shard_addrs: Vec<SocketAddrV4>, // canonical_addr is the first
            shard_fn: impl Fn(&D) -> usize + Send + Sync + 'static,
        ) -> Self {
            ClientShardClientConnection {
                inner: Arc::new(inner),
                shard_addrs,
                shard_fn: Arc::new(shard_fn),
            }
        }
    }

    impl<C, D> ChunnelConnection for ClientShardClientConnection<D, C>
    where
        C: ChunnelConnection<Data = (SocketAddrV4, D)> + Send + Sync + 'static,
        D: Kv + Send + Sync + 'static,
    {
        type Data = (SocketAddrV4, D);

        fn send(&self, data: Self::Data) -> Result<(), Report> {
            // figure out which shard to send to.
            let shard_idx = (self.shard_fn)(&data.1);
            let a = self.shard_addrs[shard_idx];
            let _ = &data;
            self.inner.send((a, data.1))
        }

        fn recv(&self) -> Result<Self::Data, Report> {
            let p = self.inner.recv()?;
            Ok(p)
        }
    }
}
