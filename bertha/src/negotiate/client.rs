use super::{Apply, ApplyResult, GetOffers, NegotiateMsg, NegotiatePicked};
use crate::{Chunnel, ChunnelConnection};
use color_eyre::eyre::{eyre, Report, WrapErr};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::future::Future;
use tracing::{debug, trace};

pub type NegotiatedConn<C, S> = <<S as Apply>::Applied as Chunnel<C>>::Connection;

/// Return a connection with `stack`'s semantics, connecting to `a`.
#[allow(clippy::manual_async_fn)] // we need the + 'static which async fn does not do.
pub fn negotiate_client<C, A, S>(
    stack: S,
    cn: C,
    addr: A,
) -> impl Future<Output = Result<NegotiatedConn<C, S>, Report>> + Send + 'static
where
    C: ChunnelConnection<Data = (A, Vec<u8>)> + Send + Sync + 'static,
    S: Apply + GetOffers + Clone + Send + 'static,
    <S as Apply>::Applied: Chunnel<C> + NegotiatePicked + Clone + Debug + Send + 'static,
    <<S as Apply>::Applied as Chunnel<C>>::Error: Into<Report> + Send + Sync + 'static,
    A: Serialize + DeserializeOwned + Clone + Debug + Send + Sync + 'static,
{
    async move {
        debug!(?addr, "client negotiation starting");
        let offers = NegotiateMsg::ClientOffer(stack.offers().collect());
        let resp = try_negotiate_offer_loop(&cn, addr.clone(), offers).await?;
        match resp {
            NegotiateMsg::ServerReply(Ok(picked)) => {
                // 4. monomorphize `stack`, picking received choices
                trace!(?picked, "received server pairs, applying");
                let mut sc = 0;
                let mut new_stack = None;
                let mut apply_err = eyre!("Apply error");
                for p in picked {
                    let p2 = p.clone();
                    match super::apply::check_apply(stack.clone(), p)
                        .wrap_err(eyre!("Could not apply received impls to stack"))
                    {
                        Ok(ApplyResult {
                            applied: ns,
                            score: p_sc,
                            ..
                        }) => {
                            // TODO what if two options are tied? This will arbitrarily pick the first.
                            if p_sc > sc || new_stack.is_none() {
                                sc = p_sc;
                                new_stack = Some((ns, p2));
                            }
                        }
                        Err(e) => {
                            debug!(err = %format!("{:#}", e), "Apply attempt failed");
                            apply_err = apply_err.wrap_err(e);
                            continue;
                        }
                    }
                }

                let (mut new_stack, nonce) = new_stack.ok_or_else(|| {
                    apply_err.wrap_err(eyre!("All received options failed to apply"))
                })?;
                debug!(applied = ?&new_stack, "applied to stack");
                let nonce = bincode::serialize(&NegotiateMsg::ServerNonce {
                    addr: bincode::serialize(&addr)?,
                    picked: nonce,
                })?;
                new_stack.call_negotiate_picked(&nonce).await;

                // 5. return new_stack.connect_wrap(vec_u8_conn)
                new_stack.connect_wrap(cn).await.map_err(Into::into)
            }
            NegotiateMsg::ServerReply(Err(errmsg)) => Err(eyre!("{:?}", errmsg)),
            _ => Err(eyre!("Received unknown message type")),
        }
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
        cn.send((addr, buf)).await?;

        // 3. receive picked
        let (_, rbuf) = cn.recv().await?;
        bincode::deserialize(&rbuf).wrap_err(eyre!(
            "Could not deserialize negotiate_server response: {:?}",
            rbuf
        ))
    }
}
