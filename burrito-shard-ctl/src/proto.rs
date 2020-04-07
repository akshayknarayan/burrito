pub use burrito_discovery_ctl::proto::Addr;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SimpleShardPolicy {
    pub packet_data_offset: u8,
    pub packet_data_length: u8,
}

/// Request type for servers registering.
///
/// `canonical_addr` must be routable. So, if `canonical_addr` is Addr::Burrito, then discovery-ctl
/// must be reachable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo {
    pub canonical_addr: Addr,
    pub shard_addrs: Vec<Addr>,
    pub shard_info: SimpleShardPolicy,
}

/// Response to a ShardInfo request which registers a shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RegisterShardReply {
    Err(String),
    Ok,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryShardRequest {
    pub canonical_addr: Addr,
}

/// Response type for clients wanting to know where the servers are.
/// The Ok variant is the same as the server request type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryShardReply {
    Err(String),
    Ok(ShardInfo),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Register(ShardInfo),
    Query(QueryShardRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Reply {
    Register(RegisterShardReply),
    Query(QueryShardReply),
}
