use std::collections::HashMap;

#[derive(Debug, Default, Clone)]
pub struct Kv {
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
