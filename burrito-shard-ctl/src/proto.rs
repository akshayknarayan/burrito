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

/// `canonical_addr`: The address to look up.
/// `min_egress_sharding_thresh`: If the number of available shards is greater than this, install
/// xdp sharding locally if available. If None, never install.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryShardRequest {
    pub canonical_addr: Addr,
    pub min_egress_sharding_thresh: Option<usize>,
}

/// Response type for clients wanting to know where the servers are.
/// The Ok variant is the same as the server request type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryShardReply {
    Err(String),
    Ok(ShardInfo),
}

/// Register: if ShardInfo, once resolved, has a Udp address and Udp shards, install xdp sharding
/// locally if available.
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
