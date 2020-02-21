use async_bincode::AsyncBincodeStream;
use core::task::{Context, Poll};
use futures_util::future::Ready;
use std::error::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tower::pipeline;

mod kv;
mod msg;

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
pub async fn server(
    st: impl AsyncWrite + AsyncRead + Unpin,
) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let st = AsyncBincodeStream::from(st).for_async();
    Ok(pipeline::Server::new(st, Store::default())
        .await
        .expect("server crashed"))
}

/// Connect to a Kv service using `st`.
pub fn client(
    st: impl AsyncWrite + AsyncRead + Unpin + Send + 'static,
) -> impl tower_service::Service<
    msg::Msg,
    Response = msg::Msg,
    Error = Box<dyn Error + Send + Sync + 'static>,
> {
    let st: AsyncBincodeStream<_, msg::Msg, _, _> = AsyncBincodeStream::from(st).for_async();
    pipeline::Client::<_, Box<dyn Error + Send + Sync + 'static>, _>::new(st)
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
