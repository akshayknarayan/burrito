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
    id: usize,
    op: Op,
    key: String,
    val: Option<String>,
}

impl Msg {
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn val(&self) -> Option<&str> {
        self.val.map(|s| s.as_str())
    }

    pub fn op(&self) -> Op {
        self.op
    }

    pub fn get_req(key: String) -> Self {
        Msg {
            id: get_id(),
            op: Op::Get,
            key,
            val: None,
        }
    }

    pub fn put_req(key: String, val: String) -> Self {
        Msg {
            id: get_id(),
            op: Op::Put,
            key,
            val: Some(val),
        }
    }

    pub fn resp(self, val: Option<String>) -> Self {
        Msg { val, ..self }
    }
}

fn get_id() -> usize {
    use rand::Rng;
    let rng = rand::thread_rng();

    rng.gen()
}
