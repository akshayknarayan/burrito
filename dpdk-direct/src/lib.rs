//! DPDK-powered UDP Chunnels.

mod dpdk_iokernel;
pub use dpdk_iokernel::{DpdkUdpReqChunnel, DpdkUdpSk, DpdkUdpSkChunnel};
