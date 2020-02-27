use async_bincode::AsyncBincodeStream;
use core::task::{Context, Poll};
use futures_util::{
    future::Ready,
    never::Never,
    sink::SinkExt,
    stream::{Stream, StreamExt},
};
use std::error::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tower::pipeline;
use tower_service::Service;
use tracing::{span, Level};
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

/// Serve `srv` on `st`.
pub async fn serve(
    st: impl AsyncWrite + AsyncRead + Unpin,
    srv: impl tower_service::Service<msg::Msg, Response = msg::Msg>,
) {
    let st: AsyncBincodeStream<_, msg::Msg, _, _> = AsyncBincodeStream::from(st).for_async();
    pipeline::Server::new(st, srv)
        .await
        .map_err(|_| ()) // argh, bincode::Error is not Debug
        .expect("shard crashed")
}

/// Serve multiple Store shards on `shard_listeners`.
///
/// Each shard will listen on the provided listener. In addition, we use `shard_fn` to steer
/// requests from the first provided listener to the correct shard.
pub async fn shard_server<S, C, E>(
    shard_listeners: impl IntoIterator<Item = S>,
    shard_fn: impl Fn(&Msg) -> usize + 'static,
) -> Result<(), StdError>
where
    S: Stream<Item = Result<C, E>> + Send + 'static,
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
            let srv = tower_buffer::Buffer::new(Store::default(), 10);
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
        let srv = tower_buffer::Buffer::new(Store::default(), 10);
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
            async move {
                let mut resps = futures_util::stream::FuturesOrdered::new();
                let mut st: AsyncBincodeStream<C, msg::Msg, msg::Msg, _> =
                    AsyncBincodeStream::from(st.unwrap()).for_async();

                loop {
                    tokio::select! {
                        Some(Ok(req)) = st.next() => {
                            // call the right shard and push the future onto the out-queue
                            let srv = &mut shards[shard_fn(&req)];
                            resps.push(srv.call(req));
                        }
                        Some(Ok(resp)) = resps.next() => {
                            // this clause is basically equivalent to st.send_all(resps)
                            // but the compiler is unhappy about error types in that case.
                            st.send(resp).await.unwrap();
                        }
                    };
                }
            }
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
{
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn update(&mut self, key: String, val: String) -> Result<Option<String>, StdError> {
        tracing::trace!("starting update");
        futures_util::future::poll_fn(|cx| self.0.poll_ready(cx))
            .instrument(span!(Level::DEBUG, "update:poll_ready"))
            .await?;
        let req = msg::Msg::put_req(key, val);
        let resp = self
            .0
            .call(req)
            .instrument(span!(Level::DEBUG, "update:call"))
            .await?;
        tracing::trace!("finished update");
        Ok(resp.into_kv().1)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get(&mut self, key: String) -> Result<Option<String>, StdError> {
        tracing::trace!("starting get");
        futures_util::future::poll_fn(|cx| self.0.poll_ready(cx))
            .instrument(span!(Level::DEBUG, "get:poll_ready"))
            .await?;
        let req = msg::Msg::get_req(key);
        let resp = self
            .0
            .call(req)
            .instrument(span!(Level::DEBUG, "get:call"))
            .await?;
        tracing::trace!("finished get");
        Ok(resp.into_kv().1)
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
