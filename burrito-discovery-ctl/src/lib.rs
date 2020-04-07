//! String -> Vec<Service>

pub const CONTROLLER_ADDRESS: &str = "name-ctl";

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "ctl")]
pub mod ctl;
pub mod proto;
