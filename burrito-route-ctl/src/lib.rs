//! Add shared filesystem for pipes to new containers,
//! and translate between service-level addresses and
//! pipes.

#![warn(clippy::all)]
#![allow(clippy::type_complexity)]

pub mod docker_proxy;
pub use docker_proxy::*;

pub mod burrito_net;
pub use burrito_net::*;

pub mod static_net;
pub use static_net::*;
