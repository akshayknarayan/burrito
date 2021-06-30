use super::{
    have_all, Apply, ApplyResult, GetOffers, NegotiateMsg, NegotiatePicked, Offer, Pick, PickResult,
};
use crate::{and_then_concurrent::TryStreamExtExt, Chunnel, ChunnelConnection, Either};
use color_eyre::eyre::{eyre, Report, WrapErr};
use futures_util::{
    future::{select, FutureExt},
    stream::{Stream, TryStreamExt},
};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::{atomic::AtomicBool, Arc, Mutex};
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tracing::{debug, debug_span, instrument, trace, warn};
use tracing_futures::Instrument;

/// Return a stream of connections with `stack`'s semantics, listening on `raw_cn_st`.
#[allow(clippy::manual_async_fn)] // we need the + 'static which async fn does not do.
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
            let ApplyResult {
                applied: mut stack, ..
            } = stack
                .apply(picked) // use check_apply or no?
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
                let (new_stack, client_resp) = match monomorphize(s, client_offers, &a) {
                    Ok((mut new_stack, nonce, picked_offers)) => {
                        let nonce_buf = bincode::serialize(&nonce)
                            .wrap_err("Failed to serialize (addr, chosen_stack) nonce")?;

                        new_stack
                            .call_negotiate_picked(&nonce_buf)
                            .instrument(debug_span!("call_negotiate_picked"))
                            .await;
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

pub(crate) fn monomorphize<Srv, A>(
    stack: Srv,
    client_offers: Vec<HashMap<u64, Offer>>,
    from_addr: &A,
) -> Result<
    (
        <Srv as Pick>::Picked,
        NegotiateMsg,
        Vec<HashMap<u64, Offer>>,
    ),
    Report,
>
where
    Srv: Pick + GetOffers + Clone + Debug + Send + 'static,
    // main-line branch: Pick on incoming negotiation handshake.
    <Srv as Pick>::Picked: Clone + Debug + Send + 'static,
    A: Serialize + DeserializeOwned + Debug + Send + Sync + 'static,
{
    // enumerate possible offer groups from `stack`.
    let possibilities: Vec<_> = stack.offers().collect();
    let saved_possibilities = possibilities.clone();
    let valid_pairs = compare_offers(client_offers, possibilities);
    // 3. monomorphize: transform the CxList<impl Serve/Select<impl Serve,
    //    impl Serve>> into a CxList<impl Serve>
    let PickResult {
        stack: new_stack,
        filtered_pairs,
        ..
    } = super::pick::check_touched(stack, valid_pairs)
        .wrap_err(eyre!("error monomorphizing stack"))?;
    assert!(!filtered_pairs.is_empty());
    let (client_choices, mut server_choices): (Vec<_>, Vec<_>) = filtered_pairs.into_iter().unzip();
    let server_choice = server_choices.pop().unwrap();

    fn check_possibilities(
        picked: HashMap<u64, Offer>,
        choices: Vec<HashMap<u64, Offer>>,
    ) -> HashMap<u64, Offer> {
        choices
            .into_iter()
            .find(|option| {
                option.iter().all(|(cap_guid, offer)| {
                    if let Some(picked_offer) = picked.get(&cap_guid) {
                        picked_offer.available == offer.available
                    } else {
                        false
                    }
                })
            })
            .expect("picked must be in choices")
    }

    let server_choice = check_possibilities(server_choice, saved_possibilities);

    // tell all the stack elements about the nonce = (client addr, chosen stack)
    let nonce = NegotiateMsg::ServerNonce {
        addr: bincode::serialize(from_addr)?,
        picked: server_choice,
    };

    Ok((new_stack, nonce, client_choices))
}

type ClientInput<C, A> = InjectWithChannel<C, (A, Vec<u8>)>;
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
