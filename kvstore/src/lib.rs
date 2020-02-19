use core::task::{Context, Poll};
use futures_util::future::Ready;

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
                let old = self.inner.put(req.key(), req.val().map(str::to_string));
                req.resp(old)
            }
        };

        futures_util::future::ready(Ok(resp))
    }
}

// TODO async_bincode

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
