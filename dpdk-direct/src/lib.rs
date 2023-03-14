//! DPDK-powered UDP Chunnels.

mod dpdk_iokernel;
pub use dpdk_iokernel::{DpdkUdpReqChunnel, DpdkUdpSk, DpdkUdpSkChunnel};

mod dpdk_inline;
pub use dpdk_inline::{DpdkInlineChunnel, DpdkInlineCn, DpdkInlineReqChunnel};
pub use dpdk_inline::{DpdkState, Msg, SendMsg, DPDK_STATE};

pub use dpdk_wrapper::bindings::{get_lcore_id, get_lcore_map};
