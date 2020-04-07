pub use burrito_discovery_ctl::proto::Addr;
use serde::{Deserialize, Serialize};

/// Callers must pass exactly one Addr::Burrito in name, and exactly one of:
/// - zero or one Addr::Tcp in register, which will be recursively registered with discovery-ctl.
/// - zero or one Addr::Unix in register will be regsitered locally (corresponding to the Addr::Burrito).
///
/// If an Addr::Tcp was given, the response will contain an Addr::Unix to listen on.
/// Any Addr::Udp or Addr::Burrito given in register wil be ignored silently.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterRequest {
    pub name: Addr,
    pub register: Option<Addr>,
}

/// register_addr: The Addr::Burrito that was passed.
/// local_addr: The assigned Addr::Unix to listen on.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterReplyOk {
    pub register_addr: Addr,
    pub local_addr: Addr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterReply(Result<RegisterReplyOk, String>);

impl From<Result<RegisterReplyOk, String>> for RegisterReply {
    fn from(s: Result<RegisterReplyOk, String>) -> Self {
        RegisterReply(s)
    }
}

impl Into<Result<RegisterReplyOk, String>> for RegisterReply {
    fn into(self) -> Result<RegisterReplyOk, String> {
        self.0
    }
}

/// A reply for a name query.
///
/// Returns all service entries matching the query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryNameReplyOk {
    pub addr: Addr,
    pub local_addr: Addr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryNameReply(Result<QueryNameReplyOk, String>);

impl From<Result<QueryNameReplyOk, String>> for QueryNameReply {
    fn from(s: Result<QueryNameReplyOk, String>) -> Self {
        QueryNameReply(s)
    }
}

impl Into<Result<QueryNameReplyOk, String>> for QueryNameReply {
    fn into(self) -> Result<QueryNameReplyOk, String> {
        self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Register(RegisterRequest),
    Query(Addr),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Reply {
    Register(RegisterReply),
    Query(QueryNameReply),
}
