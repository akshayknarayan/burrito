//! TLS encryption tunnel.

// Pin<Box<...>> is necessary and not worth breaking up
#![allow(clippy::type_complexity)]

#[cfg(feature = "ghostunnel")]
pub mod ghostunnel;

#[cfg(feature = "rustls")]
pub mod rustls;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct Encryption;

impl bertha::negotiate::CapabilitySet for Encryption {
    fn guid() -> u64 {
        0xb4bf3b3e0fa957df
    }

    fn universe() -> Option<Vec<Self>> {
        // return None to force both sides to match
        None
    }
}
