use crate::bindings::*;
use crate::*;
use tracing::debug;
use xdp_shard_prog::{AvailableShards, ShardRules};

pub struct Ingress;

/// Collection of handles to BPF objects.
///
/// On drop, unloads the XDP program, via [`remove_xdp`].
#[derive(Debug)]
pub struct BpfHandles<T> {
    prog_fd: std::os::raw::c_int,
    ifindex: u32,
    ifindex_map: std::os::raw::c_int,
    rx_queue_index_map: std::os::raw::c_int,
    available_shards_map: std::os::raw::c_int,
    num_rxqs: usize,
    curr_record: StatsRecord,
    prev_record: StatsRecord,
    _type: std::marker::PhantomData<T>,
}

impl<T> BpfHandles<T> {
    /// Define the set of sharding ports.
    ///
    /// By default, the shards map is empty and xdp_port will not rewrite port numbers, only log.
    /// Setting this will define a set of ports that xdp_port will shard between.
    /// Both the TCP and UDP `orig_port`s will be sharded to the respective ports.
    ///
    /// Note: It is not safe to call this concurrently, so it takes `&mut` self even though it would
    /// compile (unsafely) taking `&self`.
    pub fn shard_ports(
        &mut self,
        orig_port: u16,
        ports: &[u16],
        msg_offset: u8,
        field_size: u8,
    ) -> Result<(), StdError> {
        if ports.len() > 16 {
            Err(format!(
                "Too many ports to shard (max 16): {:?}",
                ports.len()
            ))?;
        }

        if field_size != 4 {
            Err(format!(
                "field_size != 4 currently doesn't pass the bpf verifier :("
            ))?;
        }

        let mut av = AvailableShards {
            num: ports.len() as _,
            ports: [0u16; 16],
            rules: ShardRules {
                msg_offset,
                field_size,
            },
        };

        av.ports[0..ports.len()].copy_from_slice(ports);

        // now set it
        let ok = unsafe {
            bpf::bpf_map_update_elem(
                self.available_shards_map,
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

    pub fn clear_port(&mut self, port: u16) -> Result<(), StdError> {
        // now set it
        let ok = unsafe {
            bpf::bpf_map_delete_elem(self.available_shards_map, &port as *const _ as *const _)
        };
        if ok < 0 {
            let errno = nix::errno::Errno::last();
            Err(format!(
                "available_shards_map delete elem failed: {}",
                errno
            ))?;
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

impl BpfHandles<Ingress> {
    /// Load xdp_shard XDP program onto the given interface.
    pub fn load_on_interface_name(interface_name: &str) -> Result<Self, StdError> {
        Self::load_on_interface_id(get_interface_id(interface_name)?)
    }

    /// Load xdp_shard XDP program onto all the interfaces matching the given socket address.
    ///
    /// Returns a list of BpfHandles, one per matching interface.
    pub fn load_on_address(serv_addr: std::net::IpAddr) -> Result<Vec<Self>, StdError> {
        get_interface_name(serv_addr)?
            .into_iter()
            .map(|if_name| Self::load_on_interface_name(&if_name))
            .collect()
    }

    /// Load xdp_shard XDP program onto the given interface id.
    pub fn load_on_interface_id(interface_id: u32) -> Result<Self, StdError> {
        let bpf_filename = concat!(env!("OUT_DIR"), "/xdp_shard_ingress.o\0");

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

        let rx_queue_index_map = unsafe { libbpf::bpf_map__fd(rx_queue_index_map) };
        if rx_queue_index_map < 0 {
            Err(format!(
                "rx_queue_index_map returned bad fd: {}",
                rx_queue_index_map
            ))?;
        }

        let available_shards_map = get_map_by_name("available_shards_map\0", bpf_obj)?;
        let available_shards_map = unsafe { libbpf::bpf_map__fd(available_shards_map) };
        if available_shards_map < 0 {
            Err(format!(
                "available_shards_map returned bad fd: {}",
                available_shards_map
            ))?;
        }

        let ifindex_map = get_map_by_name("ifindex_map\0", bpf_obj)?;
        let ifindex_map = unsafe { libbpf::bpf_map__fd(ifindex_map) };
        if ifindex_map < 0 {
            Err(format!("ifindex_map_fd returned bad fd: {}", ifindex_map))?;
        }

        let mut this = Self {
            prog_fd,
            ifindex: interface_id,
            ifindex_map,
            rx_queue_index_map,
            available_shards_map,
            num_rxqs: num_rxqs as _,
            curr_record: StatsRecord::empty(num_rxqs as _),
            prev_record: StatsRecord::empty(num_rxqs as _),
            _type: std::marker::PhantomData::<Ingress>,
        };

        this.set_ifindex()?;
        this.activate()?;

        this.curr_record.update(rx_queue_index_map)?;
        Ok(this)
    }

    /// Query cpu-rxq-port records.
    ///
    /// Returns (curr_record, prev_record) tuple. prev_record is equal to the previous call's
    /// curr_record.
    pub fn get_stats(&mut self) -> Result<(&StatsRecord, &StatsRecord), StdError> {
        std::mem::swap(&mut self.prev_record, &mut self.curr_record);
        self.curr_record.clear();
        self.curr_record.update(self.rx_queue_index_map)?;
        Ok((&self.curr_record, &self.prev_record))
    }

    fn set_ifindex(&mut self) -> Result<(), StdError> {
        let ifindex_map_fd = self.ifindex_map;

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
}

impl<T> Drop for BpfHandles<T> {
    fn drop(&mut self) {
        tracing::warn!("removing xdp program");
        unsafe { remove_xdp(self.ifindex) }
    }
}

pub fn remove_xdp_on_address(serv_addr: std::net::IpAddr) -> Result<(), StdError> {
    for interface_name in get_interface_name(serv_addr)? {
        let if_id = get_interface_id(&interface_name)?;
        debug!(
            ifname = ?&interface_name,
            ifid = if_id,
            "Removing XDP from interface"
        );
        unsafe { remove_xdp(if_id) };
    }

    Ok(())
}

pub fn remove_xdp_on_ifname(interface: &str) -> Result<(), StdError> {
    let id = get_interface_id(interface)?;
    unsafe { remove_xdp(id) };
    Ok(())
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
