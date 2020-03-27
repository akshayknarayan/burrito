use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Addr {
    Burrito(String),
    Tcp(SocketAddr),
    Udp(SocketAddr),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SimpleShardPolicy {
    pub packet_data_offset: u8,
    pub packet_data_length: u8,
}

/// Request type for servers registering.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo {
    // TODO service_name should not exist, should index by canonical_addr
    // otherwise it is not clean as service_name ends up doing the job of route-ctl
    pub service_name: String,
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
    pub service_name: String,
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
