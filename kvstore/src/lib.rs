use async_bincode::AsyncBincodeStream;
use core::task::{Context, Poll};
use futures_util::future::Ready;
use std::error::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tower::pipeline;
type StdError = Box<dyn Error + Send + Sync + 'static>;

mod kv;
mod msg;

pub use msg::Msg;

#[derive(Debug, Default)]
pub struct Store {
    inner: kv::Kv,
}

impl tower_service::Service<msg::Msg> for Store {
    type Response = msg::Msg;
    type Error = futures_util::never::Never;
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

/// Serve a Store on `st`.
pub async fn server(st: impl AsyncWrite + AsyncRead + Unpin) -> Result<(), StdError> {
    let st = AsyncBincodeStream::from(st).for_async();
    Ok(pipeline::Server::new(st, Store::default())
        .await
        .expect("server crashed"))
}

/// Get a client service.
pub fn client(
    st: impl AsyncWrite + AsyncRead + Unpin + Send + 'static,
) -> impl tower_service::Service<msg::Msg, Response = msg::Msg, Error = StdError> {
    let st: AsyncBincodeStream<_, msg::Msg, _, _> = AsyncBincodeStream::from(st).for_async();
    pipeline::Client::<_, StdError, _>::new(st)
}

/// Connect to a Kv service.
pub struct Client<S>(S);

impl<C> From<C>
    for Client<
        pipeline::Client<
            AsyncBincodeStream<C, msg::Msg, msg::Msg, async_bincode::AsyncDestination>,
            StdError,
            msg::Msg,
        >,
    >
where
    C: AsyncWrite + AsyncRead + Unpin + Send + 'static,
{
    fn from(st: C) -> Self {
        Self(pipeline::Client::new(
            AsyncBincodeStream::from(st).for_async(),
        ))
    }
}

impl<S> Client<S>
where
    S: tower_service::Service<msg::Msg, Response = msg::Msg, Error = StdError>,
{
    pub async fn update(&mut self, key: String, val: String) -> Result<Option<String>, StdError> {
        futures_util::future::poll_fn(|cx| self.0.poll_ready(cx)).await?;
        let req = msg::Msg::put_req(key, val);
        let resp = self.0.call(req).await?;
        Ok(resp.into_kv().1)
    }

    pub async fn get(&mut self, key: String) -> Result<Option<String>, StdError> {
        futures_util::future::poll_fn(|cx| self.0.poll_ready(cx)).await?;
        let req = msg::Msg::get_req(key);
        let resp = self.0.call(req).await?;
        Ok(resp.into_kv().1)
    }
}

#[cfg(test)]
mod tests {
    use crate::msg;
    use std::error::Error;
    use tower_service::Service;

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
            tokio::spawn(super::server(s));

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
