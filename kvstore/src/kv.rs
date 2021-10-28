use super::msg::{Msg, Op};
use dashmap::DashMap;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct Store {
    inner: Arc<DashMap<String, String>>,
}

impl Clone for Store {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl From<DashMap<String, String>> for Store {
    fn from(inner: DashMap<String, String>) -> Self {
        Store {
            inner: Arc::new(inner),
        }
    }
}

impl Store {
    pub fn get(&self, k: &str) -> Option<String> {
        self.inner.get(k).map(|x| x.clone())
    }

    pub fn put(&self, k: &str, v: Option<String>) -> Option<String> {
        if let Some(v) = v {
            self.inner.insert(k.to_string(), v)
        } else {
            self.inner.remove(k).map(|x| x.1)
        }
    }

    pub fn call(&self, req: Msg) -> Msg {
        match req.op() {
            Op::Get => {
                let val = self.get(req.key());
                req.resp(val)
            }
            Op::Put => {
                let old = self.put(req.key(), req.val());
                req.resp(old)
            }
        }
    }
}
