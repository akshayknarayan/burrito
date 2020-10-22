//use std::collections::HashMap;
use super::msg::{Msg, Op};
use core::task::{Context, Poll};
use futures_util::future::Ready;
use hashbrown::HashMap;
use std::convert::Infallible;
use tower_service::Service;

#[derive(Debug, Default, Clone)]
struct Kv {
    inner: HashMap<String, String>,
}

impl From<HashMap<String, String>> for Kv {
    fn from(inner: HashMap<String, String>) -> Self {
        Kv { inner }
    }
}

impl Kv {
    pub fn get(&self, k: &str) -> Option<&str> {
        self.inner.get(k).map(String::as_str)
    }

    pub fn put(&mut self, k: &str, v: Option<String>) -> Option<String> {
        if let Some(v) = v {
            self.inner.insert(k.to_string(), v)
        } else {
            self.inner.remove(k)
        }
    }
}

#[derive(Debug, Default)]
pub struct Store {
    inner: Kv,
}

impl From<Kv> for Store {
    fn from(inner: Kv) -> Self {
        Self { inner }
    }
}

impl Service<Msg> for Store {
    type Response = Msg;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Msg) -> Self::Future {
        let resp = match req.op() {
            Op::Get => {
                let val = self.inner.get(req.key()).map(str::to_string);
                req.resp(val)
            }
            Op::Put => {
                let old = self.inner.put(req.key(), req.val());
                req.resp(old)
            }
        };

        futures_util::future::ready(Ok(resp))
    }
}
