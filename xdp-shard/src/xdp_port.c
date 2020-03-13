/* SPDX-License-Identifier: GPL-2.0
 * Copyright (c) 2017 Jesper Dangaard Brouer, Red Hat Inc.
 *
 *  Example howto extract XDP RX-queue info
 */
#include <asm/byteorder.h>
#include <linux/stddef.h>
#include <linux/if_ether.h>
#include <linux/if_packet.h>
#include <linux/if_vlan.h>
#include <linux/ip.h>
#include <linux/ipv6.h>
#include <linux/in.h>
#include <linux/tcp.h>
#include <linux/udp.h>
#include <linux/bpf.h>
#include "bpf_helpers.h"


typedef __u8 u8;
typedef __u16 u16;
typedef __u32 u32;
typedef __u64 u64;

#define ntohs(x) __constant_ntohs(x)
#define htons(x) __constant_htons(x)

// set from userspace with the ifindex
struct bpf_map_def SEC("maps") ifindex_map = {
	.type		= BPF_MAP_TYPE_ARRAY,
	.key_size	= sizeof(int),
	.value_size	= sizeof(int),
	.max_entries	= 1,
};

#define NUM_PORTS 16

/* Common stats data record (shared with userspace) */
struct datarec {
    u16 ports[NUM_PORTS];
    u32 counts[NUM_PORTS + 1];
};

#define MAX_RXQs 64

/* Stats per port, per rx_queue_index, per CPU */
struct bpf_map_def SEC("maps") rx_queue_index_map = {
	.type		= BPF_MAP_TYPE_PERCPU_ARRAY,
	.key_size	= sizeof(__u32),
	.value_size	= sizeof(struct datarec),
	.max_entries	= MAX_RXQs + 1,
};

static inline int record_port(u16 port, u32 rxq)
{
    struct datarec *rxq_rec;
    u8 i;
    u16 *ports;
    u32 *counts;
	rxq_rec = bpf_map_lookup_elem(&rx_queue_index_map, &rxq);
	if (!rxq_rec)
		return XDP_ABORTED;

    ports = rxq_rec->ports;
    counts = rxq_rec->counts;

    for (i = 0; i < NUM_PORTS; i++) {
        // we've seen this port before
        if (ports[i] == port) {
            counts[i]++;
            return XDP_PASS;
        }

        // we've gotten this far and a slot is free, fill it.
        if (ports[i] == 0) {
            ports[i] = port;
            counts[i] = 1;
            return XDP_PASS;
        }
    }

    // didn't find the port, and didn't find a free slot.
    // we have to dump it in the leftovers bin.
    counts[NUM_PORTS]++;
    return XDP_PASS;
}

static inline record_icmp(u32 rxq)
{
    struct datarec *rxq_rec;
	rxq_rec = bpf_map_lookup_elem(&rx_queue_index_map, &rxq);
	if (!rxq_rec)
		return XDP_ABORTED;

    bpf_printk("icmp pcket: %d: %u", rxq, rxq_rec->counts[NUM_PORTS]);

    // no port, stick it in the leftover bin.
    rxq_rec->counts[NUM_PORTS]++;
}

static inline int parse_tcp(void *tcp_data, void *data_end, u32 rxq)
{
    struct tcphdr *th;
    u16 port;
    th = (struct tcphdr *)tcp_data;
    if ((th + 1) > (struct tcphdr*) data_end)
        return XDP_ABORTED;
    port = ntohs(th->dest);
    return record_port(port, rxq);
}

static __always_inline int parse_udp(void *udp_data, void *data_end, u32 rxq)
{
    struct udphdr *uh;
    u16 port;
    uh = (struct udphdr *)udp_data;
    if ((uh + 1) > (struct udphdr*) data_end)
        return XDP_ABORTED;
    port = ntohs(uh->dest);
    return record_port(port, rxq);
}

static inline int parse_ipv4(void *data, void *data_end, u32 rxq)
{
    u8 ipproto;
    u16 ip_len;
    void *trans_data;

    struct iphdr *iph = data;
    trans_data = iph + 1;
    if (trans_data > data_end)
        return XDP_ABORTED;

    if (iph->protocol == IPPROTO_TCP) {
        return parse_tcp(trans_data, data_end, rxq);
    } else if (iph->protocol ==IPPROTO_UDP) {
        return parse_udp(trans_data, data_end, rxq);
    } else if (iph->protocol == IPPROTO_ICMP) {
        record_icmp(rxq);
        return XDP_PASS;
    } else {
        return XDP_PASS;
    }
}

static inline int parse_eth(void *data, void *data_end, u32 rxq)
{
    u16 h_proto;
    u64 nh_off;
    struct ethhdr *eth = data;

    nh_off = sizeof(*eth);
    if (data + nh_off > data_end) {
        // need to be at least big enough for the eth header
        return XDP_ABORTED;
    }

    h_proto = eth->h_proto;
    if (h_proto == htons(ETH_P_8021Q) || h_proto == htons(ETH_P_8021AD) || h_proto == htons(ETH_P_ARP)) {
        return XDP_PASS;
    }
    
    if (h_proto == htons(ETH_P_IP)) {
        return parse_ipv4(data + nh_off, data_end, rxq);
    }

    return XDP_PASS;
}

SEC("xdp_steer_prog")
int  xdp_steer(struct xdp_md *ctx)
{
	struct datarec *rec, *rxq_rec;
	int *expected_ifindex, ingress_ifindex;
	__u32 rxq = 0;
    void *data_end = (void *)(long)ctx->data_end;
    void *data = (void *)(long)ctx->data;

	/* Accessing ctx->ingress_ifindex, cause BPF to rewrite BPF
	 * instructions inside kernel to access xdp_rxq->dev->ifindex
	 */
	ingress_ifindex = ctx->ingress_ifindex;
	expected_ifindex = bpf_map_lookup_elem(&ifindex_map, &rxq);

	/* Simple test: check ctx provided ifindex is as expected */
	if (!expected_ifindex || ingress_ifindex != *expected_ifindex) {
		return XDP_ABORTED;
	}

	/* Update stats per rx_queue_index. Handle if rx_queue_index
	 * is larger than stats map can contain info for.
	 */
	rxq = ctx->rx_queue_index;
	if (rxq >= MAX_RXQs)
		rxq = MAX_RXQs;
    return parse_eth(data, data_end, rxq);
}

char _license[] SEC("license") = "GPL";
