use serde::{Deserialize, Serialize};

/// Which operation to perform
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum Op {
    Get,
    Put,
}

/// Both the request type and response type.
///
/// Get:
/// - requests will have val = None
/// - responses val = Some and rest copied.
/// Put:
/// - requests will have val = Some() (or None for deletion)
/// - responses, val = Some if update and rest copied.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Msg {
    pub(crate) id: usize,
    pub(crate) op: Op,
    pub(crate) key: String,
    pub(crate) val: Option<String>,
}

impl Msg {
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn val(&self) -> Option<String> {
        self.val.clone()
    }

    pub fn into_kv(self) -> (String, Option<String>) {
        (self.key, self.val)
    }

    pub fn op(&self) -> Op {
        self.op
    }

    pub fn get_req(key: impl Into<String>) -> Self {
        Msg {
            id: get_id(),
            op: Op::Get,
            key: key.into(),
            val: None,
        }
    }

    pub fn put_req(key: impl Into<String>, val: impl Into<String>) -> Self {
        Msg {
            id: get_id(),
            op: Op::Put,
            key: key.into(),
            val: Some(val.into()),
        }
    }

    pub fn resp(self, val: impl Into<Option<String>>) -> Self {
        Msg {
            val: val.into(),
            ..self
        }
    }
}

fn get_id() -> usize {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    rng.gen()
}
