use ahash::AHashMap as HashMap;
use color_eyre::eyre::{ensure, eyre, Report, WrapErr};
use dpdk_wrapper::{
    bindings::*,
    mbuf_slice,
    utils::{parse_cfg, AddressInfo, HeaderInfo, TOTAL_HEADER_SIZE},
    wrapper::*,
};
use eui48::MacAddress;
use flume::Sender;
use std::net::{Ipv4Addr, SocketAddrV4};
use tracing::{debug, trace, warn};

/// A message from DPDK.
#[derive(Debug)]
pub struct Msg {
    /// The local port.
    pub port: u16,
    /// The remote address.
    pub addr: SocketAddrV4,
    mbuf: *mut rte_mbuf,
    payload_length: usize,
}

impl Msg {
    pub fn get_buf(&self) -> &[u8] {
        unsafe { mbuf_slice!(self.mbuf, TOTAL_HEADER_SIZE, self.payload_length) }
    }
}

impl Drop for Msg {
    fn drop(&mut self) {
        unsafe {
            rte_pktmbuf_free(self.mbuf);
        }
    }
}

#[derive(Debug, Clone)]
pub struct SendMsg {
    pub to_addr: SocketAddrV4,
    pub src_port: u16,
    pub buf: Vec<u8>,
}

/// DPDK state with which packets can be sent/received.
///
/// It is responsible for actually sending and receiving packets, and doing bookkeeping (mux/demux)
/// associated with tracking sockets.
pub struct DpdkState {
    eth_addr: MacAddress,
    eth_addr_raw: rte_ether_addr,
    ip_addr: Ipv4Addr,
    ip_addr_raw: u32,
    port: u16,
    mbuf_pool: *mut rte_mempool,
    arp_table: HashMap<Ipv4Addr, MacAddress>,

    num_queues: u16,
    rx_queue_id: usize,
    rx_bufs: [*mut rte_mbuf; RECEIVE_BURST_SIZE as usize],
    rx_packets_for_ports: Vec<((u16, SocketAddrV4), Vec<Msg>)>,

    rx_recv_buf: Vec<Option<Msg>>,

    tx_bufs: [*mut rte_mbuf; RECEIVE_BURST_SIZE as usize],
    ip_id: u16,
}

// SAFETY: rte_mempools should be ok to pass between threads.
unsafe impl Send for DpdkState {}

impl DpdkState {
    /// Do global initialization.
    ///
    /// `config_path` should be a TOML files with:
    /// - "dpdk" table with "eal_init" key. "eal_init" should be a string array of DPDK init args.
    /// - "net" table with "ip" key and "arp" list-of-tables.
    ///   - "arp" entries should have "ip" and "mac" keys.
    ///
    /// # Example Config
    /// ```toml
    /// [dpdk]
    /// eal_init = ["-n", "4", "-l", "0-4", "--allow", "0000:08:00.0", "--proc-type=auto"]
    ///
    /// [net]
    /// ip = "1.2.3.4"
    ///
    ///   [[net.arp]]
    ///   ip = "1.2.3.4"
    ///   mac = "00:01:02:03:04:05"
    ///
    ///   [[net.arp]]
    ///   ip = "4.3.2.1"
    ///   mac = "05:04:03:02:01:00"
    /// ```
    pub fn new(
        config_path: std::path::PathBuf,
        num_dpdk_threads: usize,
    ) -> Result<Vec<Self>, Report> {
        let (dpdk_config, ip_addr, arp_table) = parse_cfg(config_path.as_path())?;
        let (mbuf_pools, nb_ports) = dpdk_init(dpdk_config, num_dpdk_threads)?;
        ensure!(
            mbuf_pools.len() == num_dpdk_threads,
            "Not enough mempools/queues initialized for requested number of threads"
        );
        let port = nb_ports - 1;

        // what is my ethernet address (rte_ether_addr struct)
        let my_eth = get_my_macaddr(port)?;
        let eth_addr = MacAddress::from_bytes(&my_eth.addr_bytes).wrap_err("Parse mac address")?;
        let eth_addr_raw = rte_ether_addr {
            addr_bytes: eth_addr.to_array(),
        };

        let octets = ip_addr.octets();
        let ip_addr_raw: u32 = unsafe { make_ip(octets[0], octets[1], octets[2], octets[3]) };

        unsafe { flush_flow_steering(port) }?;

        Ok(mbuf_pools
            .into_iter()
            .enumerate()
            .map(|(qid, mbuf_pool)| Self {
                eth_addr,
                eth_addr_raw,
                ip_addr,
                ip_addr_raw,
                port,
                mbuf_pool,
                arp_table: arp_table.clone(),
                num_queues: num_dpdk_threads as _,
                rx_queue_id: qid,
                rx_bufs: [std::ptr::null_mut(); RECEIVE_BURST_SIZE as usize],
                rx_packets_for_ports: Vec::with_capacity(16),
                rx_recv_buf: (0..RECEIVE_BURST_SIZE).map(|_| None).collect(),
                tx_bufs: [std::ptr::null_mut(); RECEIVE_BURST_SIZE as usize],
                ip_id: 0,
            })
            .collect())
    }

    pub fn rx_queue_id(&self) -> usize {
        self.rx_queue_id
    }

    pub fn ip_addr(&self) -> Ipv4Addr {
        self.ip_addr
    }

    pub fn num_queues(&self) -> u16 {
        self.num_queues
    }

    pub fn register_flow_buffer(&mut self, local_port: u16, remote_addr: SocketAddrV4) {
        self.rx_packets_for_ports
            .push(((local_port, remote_addr), Vec::with_capacity(16)));
    }

    pub fn deregister_flow_buffer(&mut self, local_port: u16, remote_addr: Option<SocketAddrV4>) {
        for i in 0..self.rx_packets_for_ports.len() {
            let ((local_port_slot, remote_addr_slot), ref mut stash) =
                &mut self.rx_packets_for_ports[i];
            if let Some(remote_addr) = remote_addr {
                if *remote_addr_slot == remote_addr && *local_port_slot == local_port {
                    // remove this stash from the list of stashes, so that a new matching
                    // packet triggers new connection logic.
                    self.rx_packets_for_ports.swap_remove(i);
                }
            } else {
                if *local_port_slot == local_port {
                    stash.clear();
                    break;
                }
            }
        }
    }

    /// Set up a single-flow, single-queue steering rule.
    pub fn register_flow_steering(
        &mut self,
        local_port: u16,
        queues: &[u16],
    ) -> Result<FlowSteeringHandle, Report> {
        debug!(?queues, ?local_port, "Registering flow steering rule");
        if queues.len() == 1 {
            unsafe { setup_flow_steering_solo(self.port, local_port, queues[0] as _) }
        } else {
            unsafe { setup_flow_steering_rss(self.port, local_port, queues) }
        }
        .wrap_err(eyre!(
            "Could not register flow steering for port {:?} to queues {:?}",
            local_port,
            queues
        ))
    }

    pub fn eth_stats(&self) -> Result<rte_eth_stats, Report> {
        unsafe { get_eth_stats(self.port) }
    }

    pub fn try_recv_burst<'cn>(
        &'cn mut self,
        port_filter: Option<(u16, Option<SocketAddrV4>)>,
        new_conns: Option<&Sender<SocketAddrV4>>,
    ) -> Result<&'cn mut [Option<Msg>], Report> {
        match port_filter {
            None => (),
            // if dst_port filter is enable or both dst_port and src_addr filter are enabled, try popping off some packets from stashed packets.
            Some((wanted_dst_port, maybe_wanted_src_addr)) => {
                for ((cand_dst_port, cand_src_addr), ref mut stash) in
                    &mut self.rx_packets_for_ports
                {
                    if *cand_dst_port == wanted_dst_port
                        && (maybe_wanted_src_addr.is_none()
                            || *cand_src_addr == maybe_wanted_src_addr.unwrap())
                        && !stash.is_empty()
                    {
                        let mut num_returned = 0;
                        while num_returned < self.rx_recv_buf.len() && !stash.is_empty() {
                            self.rx_recv_buf[num_returned] = Some(stash.pop().unwrap());
                            num_returned += 1;
                        }

                        return Ok(&mut self.rx_recv_buf[..num_returned]);
                    }
                }
            }
        }

        ensure!(
            self.rx_recv_buf.len() >= RECEIVE_BURST_SIZE as usize,
            "Received messages slice not large enough"
        );
        let num_received = unsafe {
            rte_eth_rx_burst(
                self.port,
                self.rx_queue_id as _,
                self.rx_bufs.as_mut_ptr(),
                RECEIVE_BURST_SIZE as u16,
            )
        } as usize;
        let mut num_valid = 0;
        let mut num_invalid = 0;
        let mut num_stashed = 0;
        let mut num_dropped = 0;

        'per_pkt: for i in 0..num_received {
            // first: parse if valid packet, and what the payload size is
            let (is_valid, src_ether, src_ip, src_port, dst_port, payload_length) =
                unsafe { parse_packet(self.rx_bufs[i], &self.eth_addr_raw as _, self.ip_addr_raw) };
            if !is_valid {
                unsafe { rte_pktmbuf_free(self.rx_bufs[i]) };
                num_invalid += 1;
                continue;
            }

            let [oct1, oct2, oct3, oct4] = src_ip.to_be_bytes();
            let pkt_src_ip = Ipv4Addr::new(oct1, oct2, oct3, oct4);

            // opportunistically update arp
            self.arp_table
                .entry(pkt_src_ip)
                .or_insert_with(|| MacAddress::from_bytes(&src_ether.addr_bytes).unwrap());
            let pkt_src_addr = SocketAddrV4::new(pkt_src_ip, src_port);

            let msg = Msg {
                port: dst_port,
                addr: pkt_src_addr,
                mbuf: self.rx_bufs[i],
                payload_length,
            };

            // Msg::drop will free the mbuf. So, after this point we should not call free
            // ourselves, and instead let Msg handle it for us.

            // this call wants packets for the flow on `call_port`, but we might have gotten
            // packets for other flows. So we stash those packets for future calls.
            match port_filter {
                Some((wanted_dst_port, None)) if wanted_dst_port != dst_port => {
                    for ((p, _), ref mut stash) in &mut self.rx_packets_for_ports {
                        if *p == dst_port {
                            stash.push(msg);
                            trace!(stash_size = ?stash.len(), "Stashed packet");
                            num_stashed += 1;
                            break;
                        }
                    }

                    // the packet didn't match any ports. can drop.
                    num_dropped += 1;
                    trace!(?dst_port, ?pkt_src_addr, "dropping pkt");
                }
                Some((ref wanted_dst_port, Some(ref wanted_src_addr)))
                    if *wanted_dst_port != dst_port || *wanted_src_addr != pkt_src_addr =>
                {
                    let mut found_dst_port = false;
                    for ((cand_dst_port, cand_src_addr), ref mut stash) in
                        &mut self.rx_packets_for_ports
                    {
                        if *cand_dst_port == dst_port {
                            found_dst_port = true;
                            if *cand_src_addr == pkt_src_addr {
                                stash.push(msg);
                                num_stashed += 1;
                                continue 'per_pkt;
                            }
                        }
                    }

                    // if found_dst_port but we reached this point, then we've found a new flow.
                    // allocate a new stash and save `msg` in it.
                    if found_dst_port {
                        let mut new_stash = Vec::with_capacity(16);
                        new_stash.push(msg);
                        num_stashed += 1;
                        self.rx_packets_for_ports
                            .push(((dst_port, pkt_src_addr), new_stash));
                        debug!(?dst_port, ?pkt_src_addr, "created new stash for connection");
                        // signal new connection.
                        if let Some(nc) = new_conns {
                            nc.send(pkt_src_addr)
                                .wrap_err("New connection channel send failed")?;
                        }
                    } else {
                        // else !found_dst_port, which means no one was listening and we can drop.
                        num_dropped += 1;
                        trace!(?dst_port, ?pkt_src_addr, "dropping pkt");
                    }
                }
                _ => {
                    // either there is no filtering, or there is and it matched. either way, we can
                    // return this packet now.
                    self.rx_recv_buf[num_valid] = Some(msg);
                    num_valid += 1;
                }
            }
        }

        ensure!(
            num_valid + num_stashed + num_invalid + num_dropped == num_received,
            "packets have gone missing: received {:?}, {:?} valid {:?} stashed {:?} invalid {:?} dropped",
            num_received, num_valid, num_stashed, num_invalid, num_dropped,
        );

        if num_received > 0 {
            trace!(
                ?num_received,
                ?num_valid,
                ?num_invalid,
                ?num_stashed,
                ?num_dropped,
                "Received packets"
            );
        }

        Ok(&mut self.rx_recv_buf[..num_valid])
    }

    pub fn try_recv_burst_stash_only<'cn>(
        &'cn mut self,
        new_conns: Option<&Sender<SocketAddrV4>>,
    ) -> Result<(), Report> {
        ensure!(
            self.rx_recv_buf.len() >= RECEIVE_BURST_SIZE as usize,
            "Received messages slice not large enough"
        );
        let num_received = unsafe {
            rte_eth_rx_burst(
                self.port,
                self.rx_queue_id as _,
                self.rx_bufs.as_mut_ptr(),
                RECEIVE_BURST_SIZE as u16,
            )
        } as usize;

        let mut num_valid = 0;
        'per_pkt: for i in 0..num_received {
            // first: parse if valid packet, and what the payload size is
            let (is_valid, src_ether, src_ip, src_port, dst_port, payload_length) =
                unsafe { parse_packet(self.rx_bufs[i], &self.eth_addr_raw as _, self.ip_addr_raw) };
            if !is_valid {
                unsafe { rte_pktmbuf_free(self.rx_bufs[i]) };
                continue;
            }

            let [oct1, oct2, oct3, oct4] = src_ip.to_be_bytes();
            let pkt_src_ip = Ipv4Addr::new(oct1, oct2, oct3, oct4);

            // opportunistically update arp
            self.arp_table
                .entry(pkt_src_ip)
                .or_insert_with(|| MacAddress::from_bytes(&src_ether.addr_bytes).unwrap());
            let pkt_src_addr = SocketAddrV4::new(pkt_src_ip, src_port);

            num_valid += 1;
            let msg = Msg {
                port: dst_port,
                addr: pkt_src_addr,
                mbuf: self.rx_bufs[i],
                payload_length,
            };

            // Msg::drop will free the mbuf. So, after this point we should not call free
            // ourselves, and instead let Msg handle it for us.

            // we immediately stash this packet for later retrieval.
            for ((cand_dst_port, cand_src_addr), ref mut stash) in &mut self.rx_packets_for_ports {
                if *cand_dst_port == dst_port {
                    // packet for existing flow. we should not re-notify.
                    if *cand_src_addr == pkt_src_addr {
                        stash.push(msg);
                        continue 'per_pkt;
                    }
                }
            }

            // if found_dst_port but we reached this point, then we've found a new flow.
            // allocate a new stash and save `msg` in it.
            let mut new_stash = Vec::with_capacity(16);
            new_stash.push(msg);
            self.rx_packets_for_ports
                .push(((dst_port, pkt_src_addr), new_stash));
            debug!(?dst_port, ?pkt_src_addr, "created new stash for connection");
            // signal new connection.
            if let Some(nc) = new_conns {
                nc.try_send(pkt_src_addr)
                    .wrap_err("New connection channel send failed")?;
            }
        }

        if num_valid > 0 {
            trace!(?num_valid, "stashed packets");
        }

        Ok(())
    }

    pub fn send_burst(&mut self, msgs: impl Iterator<Item = SendMsg>) -> Result<(), Report> {
        let mut i = 0;
        for SendMsg {
            to_addr,
            src_port,
            buf,
        } in msgs
        {
            let to_ip = to_addr.ip();
            let to_port = to_addr.port();
            let dst_ether_addr = match self.arp_table.get(to_ip) {
                Some(eth) => eth,
                None => {
                    warn!(?to_ip, "Could not find IP in ARP table");
                    continue;
                }
            };

            let src_info = AddressInfo {
                udp_port: src_port,
                ipv4_addr: self.ip_addr,
                ether_addr: self.eth_addr,
            };

            let dst_info = AddressInfo {
                udp_port: to_port,
                ipv4_addr: *to_ip,
                ether_addr: *dst_ether_addr,
            };

            unsafe {
                self.tx_bufs[i] = alloc_mbuf(self.mbuf_pool).unwrap();

                // fill header
                let hdr_size = match fill_in_header(
                    self.tx_bufs[i],
                    &HeaderInfo { src_info, dst_info },
                    buf.len(),
                    self.ip_id,
                ) {
                    Ok(s) => {
                        self.ip_id += 1;
                        self.ip_id %= 0xffff;
                        s
                    }
                    Err(err) => {
                        debug!(?err, "Error writing header");
                        rte_pktmbuf_free(self.tx_bufs[i]);
                        continue;
                    }
                };

                // write payload
                let payload_slice = mbuf_slice!(self.tx_bufs[i], hdr_size, buf.len());
                rte_memcpy_wrapper(
                    payload_slice.as_mut_ptr() as _,
                    buf.as_ptr() as _,
                    buf.len(),
                );

                (*self.tx_bufs[i]).pkt_len = (hdr_size + buf.len()) as u32;
                (*self.tx_bufs[i]).data_len = (hdr_size + buf.len()) as u16;

                i += 1;
                if i >= self.tx_bufs.len() {
                    if let Err(err) = tx_burst(
                        self.port,
                        self.rx_queue_id as _,
                        self.tx_bufs.as_mut_ptr(),
                        i as u16,
                    ) {
                        warn!(?err, "tx_burst error");
                    }

                    i = 0;
                }
            }
        }

        if i > 0 {
            if let Err(err) = unsafe {
                tx_burst(
                    self.port,
                    self.rx_queue_id as _,
                    self.tx_bufs.as_mut_ptr(),
                    i as u16,
                )
            } {
                warn!(?err, "tx_burst error");
            }
        }

        Ok(())
    }
}
