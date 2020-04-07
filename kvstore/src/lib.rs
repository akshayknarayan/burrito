use async_bincode::AsyncBincodeStream;
use core::task::{Context, Poll};
use futures_util::{
    future::Ready,
    never::Never,
    sink::SinkExt,
    stream::{Stream, StreamExt},
};
use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{oneshot, Mutex};
use tokio_tower::pipeline;
use tower_service::Service;
use tracing::{debug, error, info, span, trace, warn, Level};
use tracing_futures::Instrument;

type StdError = Box<dyn Error + Send + Sync + 'static>;

mod kv;
mod msg;

pub use kv::Kv;
pub use msg::Msg;

#[derive(Debug, Default)]
pub struct Store {
    inner: kv::Kv,
}

impl From<kv::Kv> for Store {
    fn from(inner: kv::Kv) -> Self {
        Self { inner }
    }
}

impl tower_service::Service<msg::Msg> for Store {
    type Response = msg::Msg;
    type Error = Never;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: msg::Msg) -> Self::Future {
        let resp = match req.op() {
            msg::Op::Get => {
                let val = self.inner.get(req.key()).map(str::to_string);
                req.resp(val)
            }
            msg::Op::Put => {
                let old = self.inner.put(req.key(), req.val().map(|s| s.to_string()));
                req.resp(old)
            }
        };

        futures_util::future::ready(Ok(resp))
    }
}

/// Serve Store shards on `shard_listeners`.
///
/// The first shard_listener is the sharder, which uses shard_fn to steer requests to the correct
/// shard.
pub async fn shard_server_udp(
    shard_listeners: impl IntoIterator<Item = tokio::net::UdpSocket>,
    shard_fn: impl Fn(&Msg) -> usize + 'static,
) -> Result<(), StdError> {
    let mut shard_listeners = shard_listeners.into_iter();
    // the first shard_listener is the sharder
    let sharder_listen = shard_listeners
        .next()
        .ok_or_else(|| String::from("must provide at least one listener"))?;

    async fn serve_one_udp<S>(
        sk: &mut tokio::net::UdpSocket,
        srv: &mut S,
        mut buf: &mut [u8],
    ) -> Result<(), StdError>
    where
        S: tower_service::Service<msg::Msg, Response = msg::Msg>,
        S::Error: std::fmt::Debug,
    {
        let (len, from_addr) = sk.recv_from(&mut buf).await?;
        if len > buf.len() {
            Err(format!("Message too big: {} > {}", len, buf.len()))?;
        }

        let msg = &buf[..len];
        // deserialize
        let msg: msg::Msg = bincode::deserialize(msg)?;

        trace!(req = ?&msg.id, "serviceing request");

        futures_util::future::poll_fn(|cx| srv.poll_ready(cx))
            .await
            .map_err(|e| format!("Poll service err: {:?}", e))?;
        let resp = srv
            .call(msg)
            .await
            .map_err(|e| format!("Call service err: {:?}", e))?;

        let msg = bincode::serialize(&resp)?;
        sk.send_to(&msg, from_addr).await?;
        Ok(())
    }

    // start the shards
    let (shards, ctrs): (Vec<_>, Vec<_>) = shard_listeners
        .map(move |mut sk| {
            // The channel size passed to new() should be >= the maximum num. of concurrent requests.
            let mut srv = tower_buffer::Buffer::new(Store::default(), 100_000);
            let shard_srv = srv.clone();
            let ctr = Arc::new(AtomicUsize::new(0));
            let shard_ctr = ctr.clone();
            let a = sk.local_addr().expect("sk local addr").port();

            // serve srv on listener
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                loop {
                    if let Err(e) = serve_one_udp(&mut sk, &mut srv, &mut buf[..]).await {
                        warn!(err = ?e, "Error serving request");
                    }

                    ctr.fetch_add(1, Ordering::SeqCst);
                }
            });

            (shard_srv, (a, shard_ctr))
        })
        .unzip();

    let mut sk = sharder_listen;
    let mut buf = [0u8; 1024];
    // if shards.len() == 0, then there can only be one shard: sharder_listen. So, we just serve directly and
    // ignore `shard_fn`.
    if shards.is_empty() {
        let mut srv = Store::default();
        info!("Serving one shard");
        loop {
            if let Err(e) = serve_one_udp(&mut sk, &mut srv, &mut buf[..]).await {
                warn!(err = ?e, "Error serving request");
            }
        }
    }

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            for (a, ctr) in ctrs.iter() {
                let count = ctr.swap(0, Ordering::Relaxed);
                info!(port = ?a, count, "kvserver stats");
            }
        }
    });

    let mut shards = shards;
    loop {
        let (len, from_addr) = sk.recv_from(&mut buf).await?;
        let msg = &buf[..len];
        // deserialize
        let msg: msg::Msg = bincode::deserialize(msg)?;

        let srv = &mut shards[shard_fn(&msg)];
        futures_util::future::poll_fn(|cx| srv.poll_ready(cx))
            .await
            .map_err(|e| format!("Poll service err: {:?}", e))?;
        let resp = srv
            .call(msg)
            .await
            .map_err(|e| format!("Call service err: {:?}", e))?;

        let msg = bincode::serialize(&resp)?;
        sk.send_to(&msg, from_addr).await?;
    }
}

/// Serve `srv` on `st`.
pub async fn serve(
    st: impl AsyncWrite + AsyncRead + Unpin,
    srv: impl tower_service::Service<msg::Msg, Response = msg::Msg>,
) {
    let st: AsyncBincodeStream<_, msg::Msg, _, _> = AsyncBincodeStream::from(st).for_async();
    pipeline::Server::new(st, srv)
        .await
        .map_err(|_| ()) // argh, bincode::Error is not Debug
        .unwrap_or_else(|_| ());
}

/// Serve multiple Store shards on `shard_listeners`.
///
/// Each shard will listen on the provided listener. In addition, we use `shard_fn` to steer
/// requests from the first provided listener to the correct shard.
#[tracing::instrument(level = "debug", skip(shard_listeners, shard_fn))]
pub async fn shard_server<A, C, E>(
    shard_listeners: impl IntoIterator<Item = A>,
    shard_fn: impl Fn(&Msg) -> usize + 'static,
) -> Result<(), StdError>
where
    A: Stream<Item = Result<C, E>> + Send + 'static,
    C: AsyncRead + AsyncWrite + Unpin + Send,
    E: Into<Box<dyn Error + Sync + Send + 'static>> + std::fmt::Debug + Unpin + Send,
{
    let mut shard_listeners = shard_listeners.into_iter();
    // the first shard_listener is the sharder
    let sharder_listen = shard_listeners
        .next()
        .ok_or_else(|| String::from("must provide at least one listener"))?;

    // start the shards
    let shards: Vec<_> = shard_listeners
        .map(move |listener| {
            // The channel size passed to new() should be >= the maximum num. of concurrent requests.
            let srv = tower_buffer::Buffer::new(Store::default(), 100_000);
            let shard_srv = srv.clone();

            // serve srv on listener
            tokio::spawn(async move {
                listener
                    .for_each_concurrent(None, move |st| serve(st.unwrap(), srv.clone()))
                    .await
            });

            shard_srv
        })
        .collect();

    // if shards.len() == 0, then there can only be one shard: sharder_listen. So, we just serve directly and
    // ignore `shard_fn`.
    if shards.is_empty() {
        let srv = tower_buffer::Buffer::new(Store::default(), 100_000);
        sharder_listen
            .for_each_concurrent(None, move |st| serve(st.unwrap(), srv.clone()))
            .await;
        return Ok(());
    }

    let shard_fn = std::sync::Arc::new(shard_fn);

    // start the sharder
    sharder_listen
        .for_each_concurrent(None, |st| {
            let mut shards = shards.clone();
            let shard_fn = shard_fn.clone();
            let mut concurrent_history = vec![];
            async move {
                let mut resps = futures_util::stream::FuturesOrdered::new();
                let mut st: AsyncBincodeStream<C, msg::Msg, msg::Msg, _> =
                    AsyncBincodeStream::from(st.unwrap()).for_async();

                loop {
                    trace!(queue = resps.len(), "loop start");
                    tokio::select! {
                        Some(Ok(req)) = st.next() => {
                            // call the right shard and push the future onto the out-queue
                            let srv = &mut shards[shard_fn(&req)];

                            // maybe don't await this in the sharder loop
                            trace!("poll_ready for service");
                            if let Err(_) = futures_util::future::poll_fn(|cx| srv.poll_ready(cx)).await {
                                break;
                            }

                            trace!("ready, pushing call future");
                            resps.push(srv.call(req));
                        }
                        Some(Ok(resp)) = resps.next() => {
                            // this clause is basically equivalent to st.send_all(resps)
                            // but the compiler is unhappy about error types in that case.
                            trace!("start send resp");
                            st.send(resp).await.unwrap();
                            trace!("finish send resp");
                        }
                        else => break,
                    };

                    trace!(queue = resps.len(), "loop end");
                    concurrent_history.push(resps.len());
                }

                concurrent_history.sort();
                let len = concurrent_history.len() as f64;
                let quantile_idxs = [0.25, 0.5, 0.75, 0.95];
                let quantiles: Vec<_> = quantile_idxs
                    .iter()
                    .map(|q| (len * q) as usize)
                    .map(|i| concurrent_history[i])
                    .collect();
                info!( num = concurrent_history.len(), min = concurrent_history[0], p25 = quantiles[0], p50 = quantiles[1], p75 = quantiles[2], p95 = quantiles[3], max = concurrent_history[concurrent_history.len() - 1], "Finished connection");
            }
            .instrument(span!(Level::TRACE, "sharder"))
        })
        .await;

    Ok(())
}

type ClientService<C> = pipeline::Client<
    AsyncBincodeStream<C, msg::Msg, msg::Msg, async_bincode::AsyncDestination>,
    StdError,
    msg::Msg,
>;

/// Get a client service.
pub fn client<C>(st: C) -> ClientService<C>
where
    C: AsyncWrite + AsyncRead + Unpin + Send + 'static,
{
    let st: AsyncBincodeStream<_, msg::Msg, _, _> = AsyncBincodeStream::from(st).for_async();
    pipeline::Client::<_, StdError, _>::new(st)
}

/// Connect to a Kv service.
pub struct Client<S>(S);

impl<C> Client<ClientService<C>>
where
    C: AsyncWrite + AsyncRead + Unpin + Send + 'static,
{
    pub fn from_stream(st: C) -> Client<ClientService<C>> {
        Self(client::<C>(st))
    }
}

impl<S> Client<S>
where
    S: tower_service::Service<msg::Msg, Response = msg::Msg, Error = StdError>,
    S::Future: 'static,
{
    fn do_req(
        &mut self,
        req: Msg,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Option<String>, StdError>>>> {
        let fut = self.0.call(req);
        Box::pin(async move { Ok(fut.await?.into_kv().1) })
    }

    pub fn update_fut(
        &mut self,
        key: String,
        val: String,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Option<String>, StdError>>>> {
        let req = msg::Msg::put_req(key, val);
        self.do_req(req)
    }

    pub async fn update(&mut self, key: String, val: String) -> Result<Option<String>, StdError> {
        futures_util::future::poll_fn(|cx| self.poll_ready(cx)).await?;
        self.update_fut(key, val).await
    }

    pub fn get_fut(
        &mut self,
        key: String,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Option<String>, StdError>>>> {
        let req = msg::Msg::get_req(key);
        self.do_req(req)
    }

    pub async fn get(&mut self, key: String) -> Result<Option<String>, StdError> {
        futures_util::future::poll_fn(|cx| self.poll_ready(cx)).await?;
        self.get_fut(key).await
    }

    pub fn poll_ready(&mut self, cx: &mut Context) -> Poll<Result<(), StdError>> {
        self.0.poll_ready(cx)
    }
}

impl<S> From<S> for Client<S>
where
    S: tower_service::Service<msg::Msg, Response = msg::Msg, Error = StdError>,
{
    fn from(s: S) -> Client<S> {
        Client(s)
    }
}

impl<T: Clone> Clone for Client<T> {
    fn clone(&self) -> Client<T> {
        Client(self.0.clone())
    }
}

/// A [`Service`] implementation for UdpSocket-backed connections.
///
/// Necessarily also includes `tower-buffer`-like functionality since UDP does not guarantee
/// ordering.
#[derive(Debug, Clone)]
pub struct UdpClientService {
    sk_write: Arc<Mutex<tokio::net::udp::SendHalf>>,
    dest: std::net::SocketAddr,
    pending_msg: tokio::sync::mpsc::Sender<(usize, oneshot::Sender<msg::Msg>)>,
}

// Way with just one channel: send all received messages on a broadcast channel.
// Each request future has a listener on the broadcast channel, and discards any message that
// doesn't correspond to its request. Disadvantage is the response message will get cloned once per receiver.
impl UdpClientService {
    pub async fn new(
        sk: tokio::net::UdpSocket,
        dest: std::net::SocketAddr,
    ) -> Result<Self, StdError> {
        let (mut sk_read, sk_write) = sk.split();
        let (pending_msg, mut new_req) =
            tokio::sync::mpsc::channel::<(usize, oneshot::Sender<msg::Msg>)>(1_000);
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            // if new_req.recv() happened first, store the oneshot here.
            let mut inflight: HashMap<usize, oneshot::Sender<msg::Msg>> = Default::default();
            // if sk_read.recv() happened first, store the message here.
            let mut recvd: HashMap<usize, msg::Msg> = Default::default();
            loop {
                tokio::select!(
                    Ok((len, _)) = sk_read.recv_from(&mut buf) => {
                        let msg = &buf[..len];
                        let msg: msg::Msg = bincode::deserialize(msg)?;
                        trace!(msg = ?&msg, "got response");
                        if let Some(ch) = inflight.remove(&msg.id) {
                            let id = msg.id;
                            // it's possible for the receiver to have hung up, if the service was
                            // dropped. In this case ignore.
                            if let Err(_) = ch.send(msg) {
                                trace!(id, "Msg send failed");
                            }
                        } else {
                            // put it in recvd.
                            recvd.insert(msg.id, msg);
                        }
                    }
                    Some((id, ch)) = new_req.recv() => {
                        if let Some(msg) = recvd.remove(&id) {
                            // it's possible for the receiver to have hung up, if the service was
                            // dropped. In this case ignore.
                            if let Err(_) = ch.send(msg) {
                                trace!(id, "Msg send failed");
                            }
                        } else {
                            inflight.insert(id, ch);
                        }
                    }
                    else => {
                        debug!("UdpClientService loop exiting");
                        break;
                    }
                )
            }

            Ok::<_, StdError>(())
        });

        Ok(Self {
            sk_write: Arc::new(Mutex::new(sk_write)),
            dest,
            pending_msg,
        })
    }
}

impl tower_service::Service<msg::Msg> for UdpClientService {
    type Response = msg::Msg;
    type Error = StdError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: msg::Msg) -> Self::Future {
        let sk = self.sk_write.clone();
        let mut pnd = self.pending_msg.clone();
        let dest = self.dest.clone();
        Box::pin(async move {
            trace!(id = req.id, "sending request");
            let msg = bincode::serialize(&req)?;
            sk.lock().await.send_to(&msg, &dest).await?;
            let (s, r) = oneshot::channel();
            pnd.send((req.id, s)).await?;
            match tokio::time::timeout(std::time::Duration::from_secs(10), r).await {
                Err(_) => {
                    error!(id = req.id, "Request timed out. Dropped?");
                    panic!("Request timed out.");
                }
                Ok(x) => Ok(x?),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::StdError;
    use crate::msg;
    use std::error::Error;
    use tokio::io::{AsyncRead, AsyncWrite};
    use tower_service::Service;

    async fn server(st: impl AsyncWrite + AsyncRead + Unpin) -> Result<(), StdError> {
        Ok(super::serve(st, super::Store::default()).await)
    }

    macro_rules! assert_match {
        ($eq:pat, $x:expr) => {{
            match $x {
                $eq => (),
                _ => panic!("{:?} did not match", $x),
            }
        }};
    }

    #[test]
    fn put_get() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_io()
            .build()?;

        rt.block_on(async move {
            let (c, s) = tokio::net::UnixStream::pair()?;
            tokio::spawn(server(s));

            let mut c = super::client(c);
            futures_util::future::poll_fn(|cx| c.poll_ready(cx)).await?;
            let req = msg::Msg::put_req("foo", "bar");
            let resp = c.call(req).await?;

            assert_match!(msg::Msg{op: msg::Op::Put, val: None, ..}, resp);
            assert_eq!(resp.key(), "foo");

            futures_util::future::poll_fn(|cx| c.poll_ready(cx)).await?;
            let req = msg::Msg::get_req("foo");
            let resp = c.call(req).await?;

            assert_match!(msg::Msg{op: msg::Op::Get, val: Some(_), ..}, resp);
            assert_eq!(resp.key(), "foo");
            assert_eq!(resp.val().unwrap(), "bar");

            Ok(())
        })
    }
}
