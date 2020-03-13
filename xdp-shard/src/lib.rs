use std::collections::HashMap;
type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub mod libbpf;

pub fn get_interface_id(interface_name: &str) -> Result<u32, StdError> {
    Ok(nix::net::if_::if_nametoindex(interface_name)?)
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
