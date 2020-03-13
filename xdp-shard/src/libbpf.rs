#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(unused)]
pub mod libbpf {
    // libbpf rust bindings
    include!(concat!(env!("OUT_DIR"), "/libbpf.rs"));
}

#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(unused)]
pub mod bpf {
    // bpf rust bindings
    include!(concat!(env!("OUT_DIR"), "/bpf.rs"));
}

#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(unused)]
mod if_link {
    // if_link.h bindings
    // used only for consts
    include!(concat!(env!("OUT_DIR"), "/if_link.rs"));
}

use crate::StdError;
use std::collections::HashMap;

/* Common stats data Record shared with xdp_port.c */
#[repr(C)]
#[derive(Debug, Default, Copy, Clone)]
pub struct Datarec {
    ports: [u16; 16],
    counts: [u32; 17],
}

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

    fn update(&mut self, maps: &BpfHandles) -> Result<(), StdError> {
        let fd = unsafe { libbpf::bpf_map__fd(maps.rx_queue_index_map) };
        assert!(fd > 0);

        for (i, rxq) in self.rxqs.iter_mut().enumerate() {
            rxq.update(i, maps.rx_queue_index_map)?;
        }

        Ok(())
    }

    // Vec<Vec<HashMap<u16, usize>>> means: rxq_id -> cpu_id -> port -> count
    pub fn get_rxq_cpu_port_count(&self) -> Vec<Vec<HashMap<u16, usize>>> {
        self.rxqs.iter().map(|r| r.get_cpu_port_count()).collect()
    }
}

use std::sync::{atomic::AtomicBool, Arc};

pub struct BpfHandles {
    stop: Arc<AtomicBool>,
    prog_fd: std::os::raw::c_int,
    ifindex: u32,
    bpf_obj: *mut libbpf::bpf_object,
    rx_queue_index_map: *mut libbpf::bpf_map,
}

impl BpfHandles {
    pub fn new(interface_id: u32, stop: Arc<AtomicBool>) -> Result<Self, StdError> {
        load_bpf_program(interface_id, stop)
    }

    pub fn dump_loop(
        self,
        interval: std::time::Duration,
        use_res: impl Fn(&StatsRecord, &StatsRecord) -> (),
    ) -> Result<(), StdError> {
        let num_rxqs = unsafe {
            let ptr = libbpf::bpf_map__def(self.rx_queue_index_map);
            if ptr.is_null() {
                Err(String::from(
                    "Could not get bpf_map_def for rx_queue_index_map",
                ))?;
            }

            (*ptr).max_entries
        };

        let mut record = StatsRecord::empty(num_rxqs as _);
        let mut prev_record = StatsRecord::empty(num_rxqs as _);

        record.update(&self)?;

        while !self.stop.load(std::sync::atomic::Ordering::SeqCst) {
            std::thread::sleep(interval);
            std::mem::swap(&mut prev_record, &mut record);
            record = StatsRecord::empty(record.rxqs.len());
            record.update(&self)?;
            use_res(&record, &prev_record);
        }

        Ok(())
    }
}

impl Drop for BpfHandles {
    fn drop(&mut self) {
        unsafe { remove_xdp(self.ifindex) }
    }
}

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

fn load_bpf_program(interface_id: u32, stop: Arc<AtomicBool>) -> Result<BpfHandles, StdError> {
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

    let ifindex_map = get_map_by_name("ifindex_map\0", bpf_obj)?;

    let ifindex_map_fd = unsafe { libbpf::bpf_map__fd(ifindex_map) };
    if ifindex_map_fd < 0 {
        Err(format!(
            "ifindex_map_fd returned bad fd: {}",
            ifindex_map_fd
        ))?;
    }

    let key = 0;
    let ifindex = interface_id as i32;
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

    let rx_queue_index_map = get_map_by_name("rx_queue_index_map\0", bpf_obj)?;

    let xdp_flags = if_link::XDP_FLAGS_SKB_MODE | if_link::XDP_FLAGS_UPDATE_IF_NOEXIST;
    let ok = unsafe { libbpf::bpf_set_link_xdp_fd(interface_id as i32, prog_fd, xdp_flags) };
    if ok < 0 {
        Err(format!("bpf_set_link_xdp_fd failed: {}", ok))?;
    }

    Ok(BpfHandles {
        stop,
        prog_fd,
        ifindex: interface_id,
        bpf_obj,
        rx_queue_index_map,
    })
}
