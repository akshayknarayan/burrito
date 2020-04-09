use std::collections::HashMap;

type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

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

pub fn sum_maps(curr: &mut Vec<Vec<HashMap<u16, usize>>>, prev: &Vec<Vec<HashMap<u16, usize>>>) {
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

pub fn diff_maps(curr: &mut Vec<Vec<HashMap<u16, usize>>>, prev: &Vec<Vec<HashMap<u16, usize>>>) {
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

pub fn get_outgoing_interface_name(dst_addr: std::net::IpAddr) -> Result<String, StdError> {
    let out = std::process::Command::new("ip")
        .args(&["-o", "route", "get", &dst_addr.to_string()])
        .output()?
        .stdout;
    let out = String::from_utf8(out)?;
    let sp: Vec<&str> = out.split_whitespace().take(3).collect();
    Ok(sp[2].to_owned())
}

/// Wraps `getifaddrs`.
pub fn get_interface_name(addr: std::net::IpAddr) -> Result<Vec<String>, StdError> {
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
pub fn get_interface_id(interface_name: &str) -> Result<u32, StdError> {
    Ok(nix::net::if_::if_nametoindex(interface_name)?)
}
