use super::{
    have_all, inject_with_channel::InjectWithChannel, Apply, ApplyResult, GetOffers, NegotiateMsg,
    NegotiatePicked, Offer, Pick, PickResult, StackNonce,
};
use crate::{and_then_concurrent::TryStreamExtExt, Chunnel, ChunnelConnection, Either};
use color_eyre::eyre::{eyre, Report, WrapErr};
use futures_util::stream::{Stream, TryStreamExt};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::sync::{Arc, Mutex};
use tracing::{debug, debug_span, instrument, trace, trace_span, warn};
use tracing_futures::Instrument;

/// Return a stream of connections with `stack`'s semantics, listening on `raw_cn_st`.
#[allow(clippy::manual_async_fn)] // we need the + 'static which async fn does not do.
pub fn negotiate_server<'c, Srv, Sc, Se, C, A>(
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
        let pending_negotiated_connections: Arc<Mutex<HashMap<A, StackNonce>>> = Default::default();
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

#[instrument(skip(cn, stack, pending_negotiated_connections), level = "debug", err)]
async fn negotiate_server_connection<'c, C, A, Srv>(
    cn: C,
    stack: Srv,
    pending_negotiated_connections: Arc<Mutex<HashMap<A, StackNonce>>>,
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
    let mut slot = [None];
    loop {
        trace!("listening for potential negotiation pkt");
        let ms = cn.recv(&mut slot).await?;
        if ms.is_empty() {
            continue;
        }

        let (a, buf): (_, Vec<u8>) = if let Some(b) = ms[0].take() {
            b
        } else {
            continue;
        };
        trace!("got potential negotiation pkt");

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
                let addr: A = bincode::deserialize(&addr).wrap_err("mismatched addr types")?;
                trace!(client_addr = ?&addr, nonce = ?&picked, "got nonce");
                pending_negotiated_connections
                    .lock()
                    .unwrap()
                    .insert(addr, picked.clone());

                // send ack
                let ack = bincode::serialize(&NegotiateMsg::ServerNonceAck).unwrap();
                cn.send(std::iter::once((a, ack))).await?;
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
            // one-rtt case
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

                let buf = bincode::serialize(&client_resp)?;
                assert!(buf.len() < 1500); // response has to fit in a packet
                let a_str = format!("{:?}", &a);
                cn.send(std::iter::once((a, buf))).await?;
                debug!(?a_str, "sent client response");
                if let Some(mut new_stack) = new_stack {
                    debug!(stack = ?&new_stack, "handshake done, picked stack");
                    let new_cn = new_stack.connect_wrap(cn).await.map_err(Into::into)?;
                    debug!(?a_str, "returning connection");
                    return Ok(Some(Either::Left(new_cn)));
                } else {
                    continue;
                }
            }
            // zero-rtt case
            ClientNonce(client_offer) => {
                let s = stack.clone();
                let (new_stack, client_resp) = match monomorphize(s, vec![client_offer], &a) {
                    Ok((mut new_stack, nonce, _)) => {
                        let nonce_buf = bincode::serialize(&nonce)
                            .wrap_err("Failed to serialize (addr, chosen_stack) nonce")?;

                        new_stack
                            .call_negotiate_picked(&nonce_buf)
                            .instrument(debug_span!("call_negotiate_picked"))
                            .await;
                        (Some(new_stack), NegotiateMsg::ServerNonceAck)
                    }
                    Err(e) => {
                        debug!(err = %format!("{:#}", &e), "negotiation handshake failed");
                        (
                            None,
                            NegotiateMsg::ServerReply(Ok(stack.offers().collect())),
                        )
                    }
                };

                let client_resp_buf = bincode::serialize(&client_resp).unwrap();
                assert!(client_resp_buf.len() < 1500);
                cn.send(std::iter::once((a, client_resp_buf))).await?;
                debug!("sent client response");
                if let Some(mut new_stack) = new_stack {
                    debug!(stack = ?&new_stack, "zero-rtt handshake done, picked stack");
                    let new_cn = new_stack.connect_wrap(cn).await.map_err(Into::into)?;
                    debug!("returning connection");
                    return Ok(Some(Either::Left(new_cn)));
                } else {
                    continue;
                }
            }
            m => {
                // there has been some serialization error that led this message to deserialize
                // successfully, but to a message type that is nonsensical. treat this as a
                // deserialization error.
                debug!(?m, "Discarding nonsensical message");
                continue;
            }
        }
    }
}

#[instrument(level = "debug", skip(cn, pending_negotiated_connections))]
async fn process_nonces_connection<A>(
    cn: impl ChunnelConnection<Data = (A, Vec<u8>)>,
    pending_negotiated_connections: Arc<Mutex<HashMap<A, StackNonce>>>,
) -> Result<(), Report>
where
    A: Serialize + DeserializeOwned + Eq + std::hash::Hash + Debug + Send + Sync + 'static,
{
    let mut slot = [None];
    loop {
        trace!("call recv()");
        let ms = tokio::time::timeout(std::time::Duration::from_millis(500), async {
            cn.recv(&mut slot) // ShardCanonicalServerConnection is recv-only
                .instrument(trace_span!("recv"))
                .await
                .wrap_err("negotiate/server/process_nonces_connection: Error receiving")
        })
        .await??;

        if ms.is_empty() {
            continue;
        }

        let (a, buf): (_, Vec<u8>) = if let Some(b) = ms[0].take() {
            b
        } else {
            continue;
        };

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
                cn.send(std::iter::once((a, ack))).await?;
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
            if let Some(srv_offer) = server.get(guid) {
                joint.extend(srv_offer.available.clone());
            }

            if !have_all(univ, &joint) {
                return false;
            }
        } else {
            // two-sided, they must be equal
            if let Some(srv_offer) = server.get(guid) {
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
pub(crate) fn compare_offers(
    client: Vec<StackNonce>,
    server: Vec<StackNonce>,
) -> Vec<(StackNonce, StackNonce)> {
    let mut valid_pairs = vec![];
    for StackNonce(ref client_stack_candidate) in client.iter() {
        for StackNonce(ref server_stack_candidate) in server.iter() {
            if stack_pair_valid(client_stack_candidate, server_stack_candidate) {
                valid_pairs.push((
                    StackNonce(client_stack_candidate.clone()),
                    StackNonce(server_stack_candidate.clone()),
                ));
            }
        }
    }

    valid_pairs
}

pub fn monomorphize<Srv, A>(
    stack: Srv,
    client_offers: Vec<StackNonce>,
    from_addr: &A,
) -> Result<(<Srv as Pick>::Picked, NegotiateMsg, Vec<StackNonce>), Report>
where
    Srv: Pick + GetOffers,
    // main-line branch: Pick on incoming negotiation handshake.
    <Srv as Pick>::Picked: Debug,
    A: Serialize + DeserializeOwned,
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
    } = super::pick::check_touched(stack, valid_pairs).wrap_err("error monomorphizing stack")?;
    assert!(!filtered_pairs.is_empty());
    let (client_choices, mut server_choices): (Vec<_>, Vec<_>) = filtered_pairs.into_iter().unzip();
    let server_choice = server_choices.pop().unwrap();

    fn check_possibilities(StackNonce(picked): StackNonce, choices: Vec<StackNonce>) -> StackNonce {
        choices
            .into_iter()
            .find(|option| {
                option.0.iter().all(|(cap_guid, offer)| {
                    if let Some(picked_offer) = picked.get(cap_guid) {
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
