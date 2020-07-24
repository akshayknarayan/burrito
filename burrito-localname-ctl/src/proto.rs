use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;

/// Callers must pass exactly one String in name, and exactly one of:
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterRequest {
    pub name: SocketAddr,
}

/// register_addr: The name that was passed.
/// local_addr: The assigned path to listen on.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterReplyOk {
    pub register_addr: SocketAddr,
    pub local_addr: PathBuf,
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
    pub addr: SocketAddr,
    pub local_addr: Option<PathBuf>,
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
    Query(SocketAddr),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Reply {
    Register(RegisterReply),
    Query(QueryNameReply),
}
