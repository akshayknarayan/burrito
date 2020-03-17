use std::collections::HashMap;
type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub mod bindings;
use bindings::*;

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

use xdp_shared::{AvailableShards, Datarec};

#[repr(C)]
#[derive(Debug)]
pub struct Record {
    timestamp: std::time::Instant,
    cpu: Vec<Datarec>,
}

impl Record {
    // map_key is the rxq
    fn update(&mut self, map_key: usize, map: *mut libbpf::bpf_map) -> Result<(), StdError> {
        // collect stats.
        let fd = unsafe { libbpf::bpf_map__fd(map) };
        assert!(fd > 0); // already checked before

        let stats_percpu_values = &mut self.cpu;
        let stats_percpu_values_ptr = stats_percpu_values.as_mut_ptr();

        let ok = unsafe {
            bpf::bpf_map_lookup_elem(
                fd,
                &map_key as *const _ as *const _,
                stats_percpu_values_ptr as *mut _ as *mut _,
            )
        };
        if ok != 0 {
            Err(String::from(
                "Could not bpf_map_lookup_elem for stats_global_map",
            ))?;
        }

        self.timestamp = std::time::Instant::now();
        Ok(())
    }

    pub fn get_cpu_port_count(&self) -> Vec<HashMap<u16, usize>> {
        self.cpu
            .iter()
            .map(|d| {
                let mut h: HashMap<u16, usize> = d.ports[..]
                    .iter()
                    .enumerate()
                    .take_while(|(_, x)| **x != 0)
                    .map(|(idx, port)| (*port, d.counts[idx] as usize))
                    .collect();
                if d.counts[16] > 0 {
                    h.insert(0, d.counts[16] as usize);
                }

                h
            })
            .collect()
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct StatsRecord {
    rxqs: Vec<Record>,
}

impl StatsRecord {
    fn empty(num_rxqs: usize) -> Self {
        // not adding 2 causes memory corruption after bpf_map_lookup_elem.
        // why does libbpf lie about the amount bpf is going to write...
        let num_cpus = unsafe { libbpf::libbpf_num_possible_cpus() } as usize + 2;
        StatsRecord {
            rxqs: (0..num_rxqs)
                .map(|_| Record {
                    timestamp: std::time::Instant::now(),
                    cpu: (0..num_cpus).map(|_| Default::default()).collect(),
                })
                .collect(),
        }
    }

    fn update(&mut self, rx_queue_index_map: *mut libbpf::bpf_map) -> Result<(), StdError> {
        let fd = unsafe { libbpf::bpf_map__fd(rx_queue_index_map) };
        assert!(fd > 0);

        for (i, rxq) in self.rxqs.iter_mut().enumerate() {
            rxq.update(i, rx_queue_index_map)?;
        }

        Ok(())
    }

    // Vec<Vec<HashMap<u16, usize>>> means: rxq_id -> cpu_id -> port -> count
    pub fn get_rxq_cpu_port_count(&self) -> Vec<Vec<HashMap<u16, usize>>> {
        self.rxqs.iter().map(|r| r.get_cpu_port_count()).collect()
    }
}

/// Collection of handles to BPF objects.
///
/// On drop, unloads the XDP program, via [`remove_xdp`].
#[derive(Debug)]
pub struct BpfHandles {
    prog_fd: std::os::raw::c_int,
    ifindex: u32,
    bpf_obj: *mut libbpf::bpf_object,
    rx_queue_index_map: *mut libbpf::bpf_map,
    available_shards_map: *mut libbpf::bpf_map,
    num_rxqs: usize,
    curr_record: StatsRecord,
    prev_record: StatsRecord,
}

impl BpfHandles {
    /// Load xdp_port XDP program onto the given interface.
    pub fn load_on_interface_name(interface_name: &str) -> Result<Self, StdError> {
        BpfHandles::load_on_interface_id(get_interface_id(interface_name)?)
    }

    /// Load xdp_port XDP program onto the given interface id.
    pub fn load_on_interface_id(interface_id: u32) -> Result<Self, StdError> {
        let bpf_filename = concat!(env!("OUT_DIR"), "/xdp_port.o\0");

        let bpf_filename_cstr = std::ffi::CStr::from_bytes_with_nul(bpf_filename.as_bytes())?;
        let attr = libbpf::bpf_prog_load_attr {
            file: bpf_filename_cstr.as_ptr(),
            prog_type: libbpf::bpf_prog_type_BPF_PROG_TYPE_XDP,
            prog_flags: 0,
            expected_attach_type: libbpf::bpf_attach_type_BPF_CGROUP_INET_INGRESS,
            ifindex: 0,
            log_level: 0,
        };

        let mut bpf_obj: *mut libbpf::bpf_object = std::ptr::null_mut();
        let mut prog_fd = 0;

        let ok = unsafe {
            libbpf::bpf_prog_load_xattr(
                &attr,
                &mut bpf_obj as *mut *mut libbpf::bpf_object,
                &mut prog_fd as *mut _,
            )
        };
        if ok > 0 {
            Err(format!("bpf_prog_load_xattr failed: {}", ok))?;
        }

        if prog_fd == 0 {
            Err(format!("bpf_prog_load_xattr returned null fd"))?;
        }

        if prog_fd < 0 {
            Err(format!("bpf_prog_load_xattr returned bad fd: {}", prog_fd))?;
        }

        let rx_queue_index_map = get_map_by_name("rx_queue_index_map\0", bpf_obj)?;
        let num_rxqs = unsafe {
            let ptr = libbpf::bpf_map__def(rx_queue_index_map);
            if ptr.is_null() {
                Err(String::from(
                    "Could not get bpf_map_def for rx_queue_index_map",
                ))?;
            }

            (*ptr).max_entries
        };

        let available_shards_map = get_map_by_name("available_shards_map\0", bpf_obj)?;

        let mut this = BpfHandles {
            prog_fd,
            ifindex: interface_id,
            bpf_obj,
            rx_queue_index_map,
            available_shards_map,
            num_rxqs: num_rxqs as _,
            curr_record: StatsRecord::empty(num_rxqs as _),
            prev_record: StatsRecord::empty(num_rxqs as _),
        };

        this.set_ifindex()?;
        this.activate()?;

        this.curr_record.update(rx_queue_index_map)?;
        Ok(this)
    }

    /// Define the set of sharding ports.
    ///
    /// By default, the shards map is empty and xdp_port will not rewrite port numbers, only log.
    /// Setting this will define a set of ports that xdp_port will shard between.
    ///
    /// Note: It is not safe to call this concurrently, so it takes `&mut` self even though it would
    /// compile (unsafely) taking `&self`.
    pub fn shard_ports(&mut self, orig_port: u16, ports: &[u16]) -> Result<(), StdError> {
        if ports.len() > 16 {
            Err(format!(
                "Too many ports to shard (max 16): {:?}",
                ports.len()
            ))?;
        }

        let mut av = AvailableShards {
            num: ports.len() as _,
            ports: [0u16; 16],
        };

        av.ports[0..ports.len()].copy_from_slice(ports);

        // now set it
        let fd = unsafe { libbpf::bpf_map__fd(self.available_shards_map) };
        if fd < 0 {
            Err(format!("available_shards_map returned bad fd: {}", fd))?;
        }

        let ok = unsafe {
            bpf::bpf_map_update_elem(
                fd,
                &orig_port as *const _ as *const _,
                &av as *const _ as *const _,
                0,
            )
        };
        if ok < 0 {
            let errno = nix::errno::Errno::last();
            Err(format!(
                "available_shards_map update elem failed: {}",
                errno
            ))?;
        }

        Ok(())
    }

    /// Query cpu-rxq-port records.
    ///
    /// Returns (curr_record, prev_record) tuple. prev_record is equal to the previous call's
    /// curr_record.
    pub fn get_stats(&mut self) -> Result<(&StatsRecord, &StatsRecord), StdError> {
        std::mem::swap(&mut self.prev_record, &mut self.curr_record);
        self.curr_record = StatsRecord::empty(self.num_rxqs);
        self.curr_record.update(self.rx_queue_index_map)?;
        Ok((&self.curr_record, &self.prev_record))
    }

    fn set_ifindex(&mut self) -> Result<(), StdError> {
        let ifindex_map = get_map_by_name("ifindex_map\0", self.bpf_obj)?;

        let ifindex_map_fd = unsafe { libbpf::bpf_map__fd(ifindex_map) };
        if ifindex_map_fd < 0 {
            Err(format!(
                "ifindex_map_fd returned bad fd: {}",
                ifindex_map_fd
            ))?;
        }

        let key = 0;
        let ifindex = self.ifindex as i32;
        let ok = unsafe {
            bpf::bpf_map_update_elem(
                ifindex_map_fd,
                &key as *const _ as *const _,
                &ifindex as *const _ as *const _,
                0,
            )
        };
        if ok < 0 {
            Err(format!("ifindex_map bpf_map_update_elem failed: {}", ok))?;
        }

        Ok(())
    }

    fn activate(&mut self) -> Result<(), StdError> {
        let xdp_flags = if_link::XDP_FLAGS_SKB_MODE | if_link::XDP_FLAGS_UPDATE_IF_NOEXIST;
        let ok = unsafe { libbpf::bpf_set_link_xdp_fd(self.ifindex as _, self.prog_fd, xdp_flags) };
        if ok < 0 {
            Err(format!("bpf_set_link_xdp_fd failed: {}", ok))?;
        }

        Ok(())
    }
}

impl Drop for BpfHandles {
    fn drop(&mut self) {
        tracing::warn!("removing xdp program");
        unsafe { remove_xdp(self.ifindex) }
    }
}

/// Remove any XDP program on the interface.
pub unsafe fn remove_xdp(interface_id: u32) {
    let xdp_flags = if_link::XDP_FLAGS_SKB_MODE | if_link::XDP_FLAGS_UPDATE_IF_NOEXIST;
    libbpf::bpf_set_link_xdp_fd(interface_id as _, -1, xdp_flags);
}

fn get_map_by_name(
    name: &str,
    bpf_obj: *mut libbpf::bpf_object,
) -> Result<*mut libbpf::bpf_map, StdError> {
    let map_name_str = std::ffi::CStr::from_bytes_with_nul(name.as_bytes())?;
    let map = unsafe { libbpf::bpf_object__find_map_by_name(bpf_obj, map_name_str.as_ptr()) };

    if map.is_null() {
        Err(format!("{} map not found", name))?;
    }

    Ok(map)
}

fn get_interface_id(interface_name: &str) -> Result<u32, StdError> {
    Ok(nix::net::if_::if_nametoindex(interface_name)?)
}
