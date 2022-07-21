use crate::kv::Store;
use crate::msg::Msg;
use crate::reliability::KvReliabilityServerChunnel;
use and_then_concurrent::TryStreamAndThenExt;
use bertha::Chunnel;
use bertha::{
    bincode::SerializeChunnel, chan_transport::RendezvousChannel, negotiate::StackNonce,
    select::SelectListener, ChunnelConnection, ChunnelListener, CxList, GetOffers,
};
use burrito_shard_ctl::{ShardInfo, SimpleShardPolicy};
use color_eyre::eyre::{Report, WrapErr};
use futures_util::stream::{Stream, TryStreamExt};
use std::fmt::Debug;
use std::net::{IpAddr, SocketAddr};
use std::sync::{atomic::AtomicUsize, Arc};
use tracing::{debug, debug_span, info, info_span, trace, warn};
use tracing_futures::Instrument;

/// Start and serve a single shard.
///
/// Will not spawn any threads via either `tokio::spawn` or `std::thread::spawn`.
///
/// `addr`: Public address to listen on
/// `raw_listener`: Chunnel to receive public messages from clients on.
/// `internal_addr`: Address to listen for forwarded messages from `ShardCanonicalServer` on. If
/// `None`, same as `addr`. Note: be careful of trying to bind twice to the same address.
/// `internal_srv`: Chunnel to receive messages from `ShardCanonicalServer` on.
/// `s`: Will send the offers on this channel when ready to listen for connections. This is needed
/// so that `ShardCanonicalServer` can open negotiation with us if it hears from the client first,
/// since we expect a negotiation handshake here.
pub async fn single_shard(
    addr: SocketAddr,
    raw_listener: impl ChunnelListener<
            Addr = SocketAddr,
            Connection = impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
            Error = impl Into<Report> + Send + Sync + 'static,
        > + Send
        + 'static,
    internal_addr: Option<SocketAddr>,
    internal_srv: Option<
        impl ChunnelListener<
                Addr = SocketAddr,
                Connection = impl ChunnelConnection<Data = (SocketAddr, Vec<u8>)>
                                 + Send
                                 + Sync
                                 + 'static,
                Error = impl Into<Report> + Send + Sync + 'static,
            > + Clone
            + Send
            + Sync
            + 'static,
    >,
    need_address_embedding: bool,
    s: impl Into<Option<tokio::sync::oneshot::Sender<Vec<StackNonce>>>>,
    no_negotiation: bool,
) {
    let s = s.into();
    let internal_addr = internal_addr.unwrap_or(addr);
    info!(?addr, ?internal_addr, "listening");

    macro_rules! serve_stack {
        ($st_make: path, $stack: expr, $st: expr) => {{
            let offers = $stack.clone().offers().collect();
            let st = $st_make($stack, $st) // bertha::negotiate::negotiate_server
                .await
                .unwrap();

            if let Some(s) = s {
                s.send(offers).unwrap();
            }

            // initialize the kv store.
            let store = Store::default();
            let idx = Arc::new(AtomicUsize::new(0));

            tokio::pin!(st);
            st.try_for_each_concurrent(None, |cn| async move {
                let idx = idx.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let store = store.clone();
                // TODO deduplicate possible spurious retxs by req id
                serve_one(cn, store, idx, 16)
                    .instrument(debug_span!("shard_connection", idx = ?idx)).await
            }).await?;
            unreachable!()
        }}
    }

    if let Some(internal_srv) = internal_srv {
        if need_address_embedding {
            let st = SelectListener::new(raw_listener, udp_to_shard::UdpToShard(internal_srv))
                .separate_addresses()
                .listen((addr, internal_addr))
                .await
                .map_err::<Report, _>(Into::into)
                .unwrap();
            let external = CxList::from(KvReliabilityServerChunnel::default())
                .wrap(SerializeChunnel::default());
            serve_stack!(bertha::negotiate::negotiate_server, external, st)
        } else {
            let st = SelectListener::new(raw_listener, internal_srv)
                .separate_addresses()
                .listen((addr, internal_addr))
                .await
                .map_err::<Report, _>(Into::into)
                .unwrap();
            if no_negotiation {
                let external = SerializeChunnel::default();
                serve_stack!(make_cn, external, st)
            } else {
                let external = CxList::from(KvReliabilityServerChunnel::default())
                    .wrap(SerializeChunnel::default());
                serve_stack!(bertha::negotiate::negotiate_server, external, st)
            }
        }
    } else {
        let st = raw_listener.listen(addr).await?;
        if no_negotiation {
            let external = SerializeChunnel::default();
            serve_stack!(make_cn, external, st)
        } else {
            let external = CxList::from(KvReliabilityServerChunnel::default())
                .wrap(SerializeChunnel::default());
            serve_stack!(bertha::negotiate::negotiate_server, external, st)
        }
    }
}

async fn serve_one(
    cn: impl ChunnelConnection<Data = (SocketAddr, Msg)> + Send + Sync + 'static,
    store: Store,
    idx: usize,
    batch_size: usize,
) {
    let mut slots: Vec<_> = (0..batch_size).map(|_| None).collect();
    debug!("new");
    loop {
        trace!("call recv");
        let msgs = match cn.recv(&mut slots).await {
            Ok(ms) => ms,
            Err(e) => {
                warn!(err = ?e, ?idx, "exiting on recv error");
                break;
            }
        };

        trace!(sz = ?msgs.iter().map_while(|x| x.as_ref().map(|_| 1)).sum::<usize>(), "got batch");

        match cn
            .send(
                msgs.into_iter()
                    .map_while(Option::take)
                    .map(|(a, msg @ Msg { .. })| {
                        let rsp = store.call(msg);
                        (a, rsp)
                    }),
            )
            .await
        {
            Ok(_) => (),
            Err(e) => {
                warn!(err = ?e, ?idx, "exiting on send error");
                break;
            }
        }
    }
}

/// A version of `negotiate_server` that skips negotiation.
#[allow(clippy::manual_async_fn)] // we need the + 'static which async fn does not do.
fn make_cn<Srv, Sc, Se, C>(
    stack: Srv,
    raw_cn_st: Sc,
) -> impl std::future::Future<
    Output = Result<
        impl Stream<Item = Result<<Srv as Chunnel<C>>::Connection, Report>> + Send + 'static,
        Report,
    >,
> + Send
       + 'static
where
    Sc: Stream<Item = Result<C, Se>> + Send + 'static,
    Se: Into<Report> + Send + Sync + 'static,
    C: ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
    Srv: Chunnel<C> + Clone + Debug + Send + 'static,
    <Srv as Chunnel<C>>::Connection: Send + Sync + 'static,
    <Srv as Chunnel<C>>::Error: Into<Report> + Send + Sync + 'static,
{
    async move {
        // 1. serve (A, Vec<u8>) connections.
        let st = raw_cn_st.map_err(Into::into); // stream of incoming Vec<u8> conns.
        Ok(st
            .map_err(Into::into)
            .and_then_concurrent(move |cn| {
                debug!("make_cn: new cn");
                let mut stack = stack.clone();
                async move { Ok(Some(stack.connect_wrap(cn).await.map_err(Into::into)?)) }
            })
            .try_filter_map(|v| futures_util::future::ready(Ok(v))))
    }
}
