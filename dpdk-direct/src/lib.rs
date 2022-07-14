//! DPDK-powered UDP Chunnels.

mod dpdk_iokernel;
pub use dpdk_iokernel::{DpdkUdpReqChunnel, DpdkUdpSk, DpdkUdpSkChunnel};

mod dpdk_inline;
pub use dpdk_inline::{DpdkInlineChunnel, DpdkInlineCn, DpdkInlineReqChunnel};
