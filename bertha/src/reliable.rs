//! Negotiation type representing reliability.

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum ReliabilityNeg {
    Reliability,
    Ordering,
}

impl crate::negotiate::CapabilitySet for ReliabilityNeg {
    fn guid() -> u64 {
        0x9209a313d34f3ce4
    }

    // return None to force both sides to match
    fn universe() -> Option<Vec<Self>> {
        None
    }
}
