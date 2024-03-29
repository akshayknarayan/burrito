use super::{
    server::monomorphize, Apply, ApplyResult, GetOffers, NegotiateMsg, NegotiatePicked, Pick,
    StackNonce,
};
use crate::{Chunnel, ChunnelConnection};
use color_eyre::eyre::{bail, eyre, Report, WrapErr};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::{future::Future, pin::Pin};
use tracing::{debug, info, trace};

pub type NegotiatedConn<C, S> = <<S as Apply>::Applied as Chunnel<C>>::Connection;

pub struct ClientNegotiator<A> {
    nonces: HashMap<A, StackNonce>,
}

impl<A> Default for ClientNegotiator<A> {
    fn default() -> Self {
        Self {
            nonces: HashMap::new(),
        }
    }
}

impl<A: Serialize + DeserializeOwned + Clone + Debug + Eq + Hash + Send + Sync + 'static>
    ClientNegotiator<A>
{
    pub async fn negotiate_fetch_nonce<'c, C, S>(
        &mut self,
        stack: S,
        cn: C,
        addr: A,
    ) -> Result<NegotiatedConn<C, S>, Report>
    where
        C: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync + 'static,
        S: Apply + GetOffers + Clone + Send + 'static,
        <S as Apply>::Applied: Chunnel<C> + NegotiatePicked + Clone + Debug + Send + 'static,
        <<S as Apply>::Applied as Chunnel<C>>::Error: Into<Report> + Send + Sync + 'static,
    {
        let (cn, nonce) = negotiate_client_fetch_nonce(stack, cn, addr.clone()).await?;
        // the insert could be replacing something here.
        self.nonces.insert(addr, nonce);
        Ok(cn)
    }

    pub async fn negotiate_zero_rtt<'c, C, S>(
        &mut self,
        stack: S,
        cn: C,
        addr: A,
    ) -> Result<
        <<S as Apply>::Applied as Chunnel<CheckZeroRttNegotiationReply<C>>>::Connection,
        Report,
    >
    where
        C: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync + 'static,
        S: Apply + Clone + Send + 'static,
        <S as Apply>::Applied: Chunnel<CheckZeroRttNegotiationReply<C>>
            + GetOffers
            + NegotiatePicked
            + Clone
            + Debug
            + Send
            + 'static,
        <<S as Apply>::Applied as Chunnel<CheckZeroRttNegotiationReply<C>>>::Connection: Send,
        <<S as Apply>::Applied as Chunnel<CheckZeroRttNegotiationReply<C>>>::Error:
            Into<Report> + Send + Sync + 'static,
    {
        let nonce = self
            .nonces
            .get(&addr)
            .ok_or_else(|| eyre!("No nonce found for addr"))?;
        negotiate_client_nonce(stack, cn, nonce.clone(), addr).await
    }

    pub async fn re_negotiate<'c, C, S>(
        &mut self,
        stack: S,
        cn: C,
        addr: A,
        returned_error: Report,
    ) -> Result<
        <<S as Apply>::Applied as Chunnel<CheckZeroRttNegotiationReply<C>>>::Connection,
        Report,
    >
    where
        C: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync + 'static,
        S: Apply + Pick + GetOffers + Clone + Debug + Send + 'static,
        <S as Apply>::Applied: Chunnel<CheckZeroRttNegotiationReply<C>>
            + GetOffers
            + NegotiatePicked
            + Clone
            + Debug
            + Send
            + 'static,
        <<S as Apply>::Applied as Chunnel<CheckZeroRttNegotiationReply<C>>>::Connection: Send,
        <<S as Apply>::Applied as Chunnel<CheckZeroRttNegotiationReply<C>>>::Error:
            Into<Report> + Send + Sync + 'static,
        <S as Pick>::Picked: Debug,
    {
        let returned_error: ZeroRttNegotiationError = returned_error.downcast().wrap_err("Renegotation only works with an error returned from a connection returned by negotiate_zero_rtt")?;
        let nonces = match returned_error {
            ZeroRttNegotiationError::NotAccepted(nonces) => nonces,
            e => bail!(eyre!("Non-negotiation error: {}", e)),
        };

        let picked = client_monomorphize(&stack, nonces)?;
        self.nonces.insert(addr.clone(), picked.clone());
        negotiate_client_nonce(stack, cn, picked, addr).await
    }
}

/// Return a connection with `stack`'s semantics, connecting to `a`.
///
/// This is the traditional "one-rtt" version. It will block until the remote end completes the
/// negotiation handshake.
pub async fn negotiate_client<C, A, S>(
    stack: S,
    cn: C,
    addr: A,
) -> Result<NegotiatedConn<C, S>, Report>
where
    C: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync + 'static,
    S: Apply + GetOffers + Clone + Send + 'static,
    <S as Apply>::Applied: Chunnel<C> + NegotiatePicked + Clone + Debug + Send + 'static,
    <<S as Apply>::Applied as Chunnel<C>>::Error: Into<Report> + Send + Sync + 'static,
    A: Serialize + DeserializeOwned + Clone + Debug + Send + Sync + 'static,
{
    let (cn, _) = negotiate_client_fetch_nonce(stack, cn, addr).await?;
    Ok(cn)
}

/// Same as [`negotiate_client`], but also return the [`StackNonce`].
pub async fn negotiate_client_fetch_nonce<C, A, S>(
    stack: S,
    cn: C,
    addr: A,
) -> Result<(NegotiatedConn<C, S>, StackNonce), Report>
where
    C: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync + 'static,
    S: Apply + GetOffers + Clone + Send + 'static,
    <S as Apply>::Applied: Chunnel<C> + NegotiatePicked + Clone + Debug + Send + 'static,
    <<S as Apply>::Applied as Chunnel<C>>::Error: Into<Report> + Send + Sync + 'static,
    A: Serialize + DeserializeOwned + Clone + Debug + Send + Sync + 'static,
{
    let offers = NegotiateMsg::ClientOffer(stack.offers().collect());
    debug!(?addr, ?offers, "client negotiation starting");
    let resp = try_negotiate_offer_loop(&cn, addr.clone(), offers).await?;
    match resp {
        NegotiateMsg::ServerReply(Ok(mut picked)) => {
            // 4. monomorphize `stack`, picking received choices
            trace!(?picked, "received server pairs, applying");
            let mut sc = 0;
            let mut applied = None;
            let mut apply_err = eyre!("Apply error");
            if picked.is_empty() {
                picked.push(Default::default());
            }

            for p in picked {
                let p2 = p.clone();
                match super::apply::check_apply(stack.clone(), p)
                    .wrap_err("Could not apply received impls to stack")
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

            let (mut applied, nonce) = applied
                .ok_or_else(|| apply_err.wrap_err("All received options failed to apply"))?;
            info!(?applied, ?addr, "negotiated stack");
            let inform_picked_nonce_buf = bincode::serialize(&NegotiateMsg::ServerNonce {
                addr: bincode::serialize(&addr)?,
                picked: nonce.clone(),
            })?;
            applied
                .call_negotiate_picked(&inform_picked_nonce_buf)
                .await;

            // 5. return applied.connect_wrap(vec_u8_conn)
            Ok((applied.connect_wrap(cn).await.map_err(Into::into)?, nonce))
        }
        NegotiateMsg::ServerReply(Err(errmsg)) => Err(eyre!("{:?}", errmsg)),
        _ => Err(eyre!("Received unknown message type")),
    }
}

/// Similar to `negotiate_client_nonce`, but the nonce is implicit.
///
/// The nonce in this case is determined by inspecting the provided `stack`. The stack must be
/// fully-determined, i.e., it cannot have any [`Select`]s. If it does, this will error.
pub async fn negotiate_client_fixed_stack<'c, C, A, S>(
    mut stack: S,
    cn: C,
    addr: A,
) -> Result<<S as Chunnel<CheckZeroRttNegotiationReply<C>>>::Connection, Report>
where
    C: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync + 'static,
    A: Serialize + DeserializeOwned + Clone + Debug + Send + Sync + 'static,
    S: Chunnel<CheckZeroRttNegotiationReply<C>>
        + GetOffers
        + NegotiatePicked
        + Clone
        + Debug
        + Send
        + 'static,
    <S as Chunnel<CheckZeroRttNegotiationReply<C>>>::Error: Into<Report> + Send + Sync + 'static,
{
    let nonce = {
        let mut offers = stack.offers();
        let nonce: StackNonce = offers
            .next()
            .ok_or_else(|| eyre!("No StackNonce available for {:?}", stack))?;
        if offers.next().is_some() {
            bail!("Stack should not have Selects: {:?}", stack);
        }

        nonce
    };

    let msg = NegotiateMsg::ClientNonce(nonce.clone());
    let buf = bincode::serialize(&msg)?;
    cn.send(std::iter::once((addr.clone(), buf))).await?;

    let picked = NegotiateMsg::ServerNonce {
        addr: bincode::serialize(&addr)?,
        picked: nonce,
    };
    let inform_picked_nonce_buf = bincode::serialize(&picked)?;
    stack.call_negotiate_picked(&inform_picked_nonce_buf).await;

    stack
        .connect_wrap(CheckZeroRttNegotiationReply::from(cn))
        .await
        .map_err(Into::into)
}

/// Pass an existing `nonce` to get a "zero-rtt" negotiated connection that be be used immediately.
///
/// The connection might return an error later if `nonce` was incompatible with the other side.
/// This error will be `.downcast`-able to a [`ZeroRttNegotiationError`], which will contain the
/// list of valid remote nonces.
/// From this, callers can monomorphize a new nonce and try again with this function.
pub async fn negotiate_client_nonce<C, A, S>(
    stack: S,
    cn: C,
    nonce: StackNonce,
    addr: A,
) -> Result<<<S as Apply>::Applied as Chunnel<CheckZeroRttNegotiationReply<C>>>::Connection, Report>
where
    C: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync + 'static,
    S: Apply + Clone + Send + 'static,
    <S as Apply>::Applied: Chunnel<CheckZeroRttNegotiationReply<C>>
        + GetOffers
        + NegotiatePicked
        + Clone
        + Debug
        + Send
        + 'static,
    <<S as Apply>::Applied as Chunnel<CheckZeroRttNegotiationReply<C>>>::Connection: Send,
    <<S as Apply>::Applied as Chunnel<CheckZeroRttNegotiationReply<C>>>::Error:
        Into<Report> + Send + Sync + 'static,
    A: Serialize + DeserializeOwned + Clone + Debug + Send + Sync + 'static,
{
    let super::ApplyResult { applied, .. } = stack.apply(nonce)?;
    negotiate_client_fixed_stack(applied, cn, addr).await
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
    match monomorphize(stack.clone(), server_offers, &String::new())? {
        (_, NegotiateMsg::ServerNonce { picked, .. }, _) => Ok(picked),
        _ => unreachable!(),
    }
}

#[tracing::instrument(skip(cn, offer), err)]
async fn try_negotiate_offer_loop<A, C>(
    cn: &C,
    addr: A,
    offer: NegotiateMsg,
) -> Result<NegotiateMsg, Report>
where
    A: Clone + Debug + Send + Sync + 'static,
    C: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync + 'static,
{
    use tokio::time;
    let buf = bincode::serialize(&offer)?;
    loop {
        match time::timeout(
            std::time::Duration::from_millis(2_000),
            try_once(cn, addr.clone(), buf.clone()),
        )
        .await
        {
            Ok(Ok(r)) => return Ok(r),
            Ok(e) => return e,
            Err(time::error::Elapsed { .. }) => {
                debug!("negotiate offer timed out");
                continue;
            }
        }
    }

    async fn try_once<A, C>(cn: &C, addr: A, buf: Vec<u8>) -> Result<NegotiateMsg, Report>
    where
        A: Send + Sync + 'static,
        C: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync + 'static,
    {
        // 2. send offers
        cn.send(std::iter::once((addr, buf))).await?;

        // 3. receive picked
        let mut slot = [None];
        let rbuf = loop {
            let ms = cn.recv(&mut slot).await?;
            if ms.len() == 0 || ms[0].is_none() {
                continue;
            }

            break ms[0].take().unwrap().1;
        };
        if rbuf.is_empty() {
            Ok(NegotiateMsg::ServerReply(Ok(vec![])))
        } else {
            bincode::deserialize(&rbuf).wrap_err_with(|| {
                eyre!(
                    "Could not deserialize negotiate_server response: {:?}",
                    rbuf
                )
            })
        }
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

impl<C, A> ChunnelConnection for CheckZeroRttNegotiationReply<C>
where
    C: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync,
    A: Send + 'static,
{
    type Data = (A, Vec<u8>);

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        self.inner.send(burst)
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        if !self.success.load(Ordering::SeqCst) {
            Box::pin(async move {
                let mut slot = [None];
                let (addr, data) = loop {
                    let ms = self.inner.recv(&mut slot).await?;
                    if ms.is_empty() || ms[0].is_none() {
                        continue;
                    }

                    break ms[0].take().unwrap();
                };

                // try parsing a NegotiateMsg::ServerReply
                let m: Result<NegotiateMsg, _> = bincode::deserialize(&data);
                match m {
                    Ok(NegotiateMsg::ServerNonceAck) => {
                        debug!("zero-rtt negotiation succeeded");
                        self.success.as_ref().store(true, Ordering::SeqCst);
                        // the next recv will get application data.
                        // this is safe because we set success to true, so a subsequent call
                        // to this function would skip negotiaton logic anyway
                        self.inner.recv(msgs_buf).await
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
                        msgs_buf[0] = Some((addr, data));
                        Ok(&mut msgs_buf[0..1])
                    }
                    Ok(NegotiateMsg::ServerReply(Ok(options))) => {
                        bail!(ZeroRttNegotiationError::NotAccepted(options))
                    }
                    Ok(NegotiateMsg::ServerReply(Err(s))) => {
                        bail!(ZeroRttNegotiationError::UnexpectedError(s))
                    }
                    Ok(m) => bail!(ZeroRttNegotiationError::UnexpectedResponse(m)),
                }
            })
        } else {
            self.inner.recv(msgs_buf)
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
