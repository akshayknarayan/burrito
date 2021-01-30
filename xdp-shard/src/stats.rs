use crate::bindings::*;
use eyre::{eyre, Report};
use std::collections::HashMap;
use xdp_shard_prog::Datarec;

#[repr(C)]
#[derive(Debug)]
struct Record {
    rxq_id: usize,
    timestamp: std::time::Instant,
    cpu: Vec<Datarec>,
}

impl Record {
    // map_key is the rxq
    fn update(&mut self, fd: std::os::raw::c_int) -> Result<(), Report> {
        let map_key = self.rxq_id;

        // collect stats.
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
            return Err(eyre!("Could not bpf_map_lookup_elem for stats_global_map"));
        }

        self.timestamp = std::time::Instant::now();
        Ok(())
    }

    fn clear(&mut self) {
        for r in self.cpu.iter_mut() {
            for v in &mut r.ports[..] {
                *v = 0;
            }

            for v in &mut r.counts[..] {
                *v = 0;
            }
        }
    }

    fn get_cpu_port_count(&self) -> Vec<HashMap<u16, usize>> {
        self.cpu
            .iter()
            .map(|d| {
                let mut h: HashMap<u16, usize> = d.ports[..]
                    .iter()
                    .enumerate()
                    .take_while(|(_, x)| **x != 0)
                    .map(|(idx, port)| (*port, d.counts[idx] as usize))
                    .collect();

                if d.counts[xdp_shard_prog::NUM_PORTS as usize] > 0 {
                    h.insert(0, d.counts[xdp_shard_prog::NUM_PORTS as usize] as usize);
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
    pub fn empty(num_rxqs: usize) -> Self {
        // not adding 2 causes memory corruption after bpf_map_lookup_elem.
        // why does libbpf lie about the amount bpf is going to write...
        let num_cpus = unsafe { libbpf::libbpf_num_possible_cpus() } as usize + 2;
        StatsRecord {
            rxqs: (0..num_rxqs)
                .map(|i| Record {
                    rxq_id: i,
                    timestamp: std::time::Instant::now(),
                    cpu: (0..num_cpus).map(|_| Default::default()).collect(),
                })
                .collect(),
        }
    }

    pub fn clear(&mut self) {
        for r in self.rxqs.iter_mut() {
            r.clear();
        }
    }

    pub fn update(&mut self, rx_queue_index_map: std::os::raw::c_int) -> Result<(), Report> {
        for rxq in self.rxqs.iter_mut() {
            rxq.update(rx_queue_index_map)?;
        }

        Ok(())
    }

    // Vec<Vec<HashMap<u16, usize>>> means: rxq_id -> cpu_id -> port -> count
    pub fn get_rxq_cpu_port_count(&self) -> Vec<Vec<HashMap<u16, usize>>> {
        self.rxqs.iter().map(|r| r.get_cpu_port_count()).collect()
    }
}
