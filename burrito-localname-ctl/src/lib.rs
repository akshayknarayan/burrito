#![warn(clippy::all)]
#![allow(clippy::type_complexity)]

pub const CONTROLLER_ADDRESS: &str = "localname-ctl";

pub mod client;
pub mod proto;

#[cfg(feature = "ctl")]
pub mod ctl;

use std::net::SocketAddr;

pub trait GetSockAddr {
    fn as_sk_addr(&self) -> SocketAddr;
}

impl GetSockAddr for SocketAddr {
    fn as_sk_addr(&self) -> SocketAddr {
        *self
    }
}

impl<A> GetSockAddr for (SocketAddr, A) {
    fn as_sk_addr(&self) -> SocketAddr {
        self.0
    }
}

mod runtime_fastpath;
pub use runtime_fastpath::*;

mod static_fastpath;
pub use static_fastpath::*;
