use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// Service discovery entry.
///
/// name: the name of the service
/// service: a controller-addr to query to find service-specific information
/// address: a string to pass to the controller-addr.
///
/// For example, for plain routing:
/// ```
/// # use burrito_discovery_ctl::proto::{Scope, Service};
/// Service {
///     name: "foo".into(),
///     scope: Scope::Global,
///     service: "/tmp/burrito/route-controller".into(),
///     address: "tcp://10.1.1.42:52151".parse().unwrap(),
/// };
/// ```
///
/// There may be another more efficient route available (locally scoped):
/// ```
/// # use burrito_discovery_ctl::proto::{Scope, Service};
/// Service {
///     name: "foo".into(),
///     scope: Scope::Local,
///     service: "/tmp/burrito/local-route-controller".into(),
///     address: "unix:///tmp/burrito/local-addr".parse().unwrap(),
/// };
/// ```
///  The local-route service has local-scope, so it wouldn't get synced globally.
///
/// An example for a sharded kv service:
/// ```
/// # use burrito_discovery_ctl::proto::{Scope, Service};
/// Service {
///     name: "bar".into(),
///     scope: Scope::Global,
///     service: "/tmp/burrito/shard-controller".into(),
///     address: "burrito://bar".parse().unwrap(),
/// };
/// ```
///
/// This "bar" service might have another Service entry:
/// ```
/// # use burrito_discovery_ctl::proto::{Scope, Service};
/// Service {
///     name: "bar".into(),
///     scope: Scope::Global,
///     service: "/tmp/burrito/route-controller".into(),
///     address: "udp://10.1.1.57:21421".parse().unwrap(),
/// };
/// ```
///
/// In table form, the above examples together look like this:
///
///  |-------|---------------|-------------------------|
///  |  Name |    Service    |        Address          |
///  |-------|---------------|-------------------------|
///  |  foo  |  route        |  tcp://10.1.1.42:52151  |
///  |  foo  |  local-route  |  burrito://foo          |
///  |  bar  |  route        |  udp://10.1.1.57:21421  |
///  |  bar  |  shard        |  burrito://bar          |
///  |-------|---------------|-------------------------|
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Service {
    pub name: String,
    pub scope: Scope,
    pub service: String,
    pub address: Addr,
}

/// Controls how far the service is advertised up the stack.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Hash)]
pub enum Scope {
    Host,
    Local,
    Global,
}

/// A generalized address type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Addr {
    Burrito(String),
    Tcp(SocketAddr),
    Udp(SocketAddr),
    Unix(std::path::PathBuf),
}

impl std::fmt::Display for Addr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Addr::Burrito(a) => write!(f, "burrito://{}", a),
            Addr::Tcp(a) => write!(f, "tcp://{}", a),
            Addr::Udp(a) => write!(f, "udp://{}", a),
            Addr::Unix(a) => write!(f, "unix://{}", a.display()),
        }
    }
}

impl std::str::FromStr for Addr {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split("://");
        Ok(
            match parts
                .next()
                .ok_or_else(|| anyhow::anyhow!("Malformed address: {}", s))?
            {
                "burrito" => Addr::Burrito(
                    parts
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("Malformed address: {}", s))?
                        .into(),
                ),
                "tcp" => Addr::Tcp(
                    parts
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("Malformed address: {}", s))?
                        .parse()?,
                ),
                "udp" => Addr::Udp(
                    parts
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("Malformed address: {}", s))?
                        .parse()?,
                ),
                "unix" => Addr::Unix(
                    parts
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("Malformed address: {}", s))?
                        .into(),
                ),
                a => Err(anyhow::anyhow!("Unknown addr type {}", a))?,
            },
        )
    }
}

/// A query for a name.
///
/// Optionally filtered by a list of filters for service names (match any).
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct QueryNameRequest {
    pub name: String,
    pub service_filters: Option<Vec<String>>,
}

/// A reply for a name query.
///
/// Returns all service entries matching the query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryNameReplyOk {
    pub name: String,
    pub services: Vec<Service>,
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

pub type RegisterReplyOk = ();

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Register(Service),
    Query(QueryNameRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Reply {
    Register(RegisterReply),
    Query(QueryNameReply),
}
