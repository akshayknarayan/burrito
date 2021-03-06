use eyre::Report;
use std::collections::HashMap;

#[cfg(feature = "ebpf")]
pub mod bindings;
#[cfg(feature = "ebpf")]
pub mod stats;
#[cfg(feature = "ebpf")]
pub use stats::*;
#[cfg(feature = "ebpf")]
pub mod handles;
#[cfg(feature = "ebpf")]
pub use handles::*;

pub fn sum_maps(curr: &mut Vec<Vec<HashMap<u16, usize>>>, prev: &[Vec<HashMap<u16, usize>>]) {
    for (curr, prev) in curr.iter_mut().zip(prev.iter()) {
        for (curr, prev) in curr.iter_mut().zip(prev.iter()) {
            for (port, count) in curr.iter_mut() {
                if prev.contains_key(port) {
                    *count += prev.get(port).unwrap();
                }
            }
        }
    }
}

pub fn diff_maps(curr: &mut Vec<Vec<HashMap<u16, usize>>>, prev: &[Vec<HashMap<u16, usize>>]) {
    for (curr, prev) in curr.iter_mut().zip(prev.iter()) {
        for (curr, prev) in curr.iter_mut().zip(prev.iter()) {
            for (port, count) in curr.iter_mut() {
                if prev.contains_key(port) {
                    *count -= prev.get(port).unwrap();
                }
            }
        }
    }
}

/// Wraps `getifaddrs`.
pub fn get_interface_name(addr: std::net::IpAddr) -> Result<Vec<String>, Report> {
    use nix::ifaddrs;
    let ifaddrs = ifaddrs::getifaddrs()?
        .filter_map(|a| match a {
            ifaddrs::InterfaceAddress {
                interface_name,
                address: Some(nix::sys::socket::SockAddr::Inet(if_addr)),
                ..
            } if addr.is_unspecified() || addr == if_addr.ip().to_std() => Some(interface_name),
            _ => None,
        })
        .collect();

    Ok(ifaddrs)
}

/// Wraps `if_nametoindex`.
pub fn get_interface_id(interface_name: &str) -> Result<u32, Report> {
    Ok(nix::net::if_::if_nametoindex(interface_name)?)
}
