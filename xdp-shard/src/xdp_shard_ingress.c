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
#include <linux/types.h>
#include "bpf_helpers.h"
#include "xdp_shard.h"

typedef __u64 Fnv64_t;

/*
 * 64 bit magic FNV-0 and FNV-1 prime
 */
#define FNV1_64_INIT ((Fnv64_t)0xcbf29ce484222325ULL)
#define FNV_64_PRIME ((Fnv64_t)0x100000001b3ULL)

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

#define MAX_RXQs 64

/* Stats per rx_queue_index, per port, per CPU */
struct bpf_map_def SEC("maps") rx_queue_index_map = {
	.type		= BPF_MAP_TYPE_PERCPU_ARRAY,
	.key_size	= sizeof(__u32),
	.value_size	= sizeof(struct datarec),
	.max_entries	= MAX_RXQs + 1,
};

struct bpf_map_def SEC("maps") active_clients_map = {
	.type		 = BPF_MAP_TYPE_HASH,
	.key_size	 = sizeof(struct active_client),
    .value_size  = sizeof(__u8),
	.max_entries = 64,
};

/* Dest. port -> available_shards map for sharding on that port */
struct bpf_map_def SEC("maps") available_shards_map = {
	.type		= BPF_MAP_TYPE_HASH,
	.key_size	= sizeof(__u16),
	.value_size	= sizeof(struct available_shards),
	.max_entries	= 64,
};

static inline int shard_generic(void *app_data, void *data_end, u16 *port) {
    struct available_shards *shards;
    u16 le_port = ntohs(*port);
    u16 out_port;
    u8 offset;
    u8 *pkt_val;
    u64 hash = FNV1_64_INIT;
    u8 idx = 0;
    u8 i;

    // lookup in the map of ports we have to do work for.
	shards = bpf_map_lookup_elem(&available_shards_map, &le_port);
    if (!shards) {
        bpf_printk("port not found %u\n", le_port);
        return XDP_PASS;
    }

    if (shards->num < 1 || shards->num > NUM_PORTS) {
        // sharding disabled
        bpf_printk("sharding disabled on %u\n", le_port);
        return XDP_PASS;
    }

    // ok, we have to do work. first, check that the max offset we might have to read is valid
    if (shards->rules.msg_offset > 64) {
        bpf_printk("Shard rules invalid: %u,%u\n", shards->rules.msg_offset, shards->rules.field_size);
        return XDP_ABORTED;
    } else {
        offset = shards->rules.msg_offset;
    }

    // This tends to break the verifier. The key seems to be getting the compiler to 
    // reuse the same register for both the comparison here and pkt_val = app_data + offset below.
    if (app_data + offset + 4 > data_end) {
        bpf_printk("Packet not large enough for msg: %u\n", (data_end - app_data));
        return XDP_ABORTED;
    }

    pkt_val = app_data + offset;

    // compute FNV hash
    #pragma clang loop unroll(full)
    for (i = 0; i < 4; i++) {
        hash = hash ^ ((u64) pkt_val[i]);
        hash *= FNV_64_PRIME;
    }

    // map to a shard and assign to that port.
    idx = hash % shards->num;

    if (idx > 14) {
        return XDP_ABORTED;
    }

    out_port = shards->ports[idx];
    bpf_printk("Sharding %u -> %u\n", le_port, out_port);
    *port = htons(out_port);
    return XDP_PASS;
}

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

/*
static inline void dump_stats() {
    struct datarec *rxq_rec;
    u32 i = 0;
    u8 j;
    u16 *ports;
    u32 *counts;

    rxq_rec = bpf_map_lookup_elem(&rx_queue_index_map, &i);
    if (!rxq_rec)
        return;

    ports = rxq_rec->ports;
    counts = rxq_rec->counts;

    #pragma clang loop unroll(full)
    for (j = 0; j < NUM_PORTS; j++) {
        bpf_printk("rxq %u -> port %u: %u\n", i, ports[j], counts[j]);
    }
}
*/

static inline int record_icmp(u32 rxq)
{
    struct datarec *rxq_rec;
	rxq_rec = bpf_map_lookup_elem(&rx_queue_index_map, &rxq);
	if (!rxq_rec)
		return XDP_ABORTED;

    // no port, stick it in the leftover bin.
    rxq_rec->counts[NUM_PORTS]++;
    bpf_printk("rxq %u -> icmp: %u (counts[%u])\n", rxq, rxq_rec->counts[NUM_PORTS], NUM_PORTS);
    return XDP_PASS;
}

static inline int parse_tcp(void *tcp_data, void *data_end, u32 src_addr, u32 rxq)
{
    struct tcphdr *th;
    int res;
    u8 data_offset;
    u8 *val;
    void *payload;
    struct active_client client;

    th = (struct tcphdr *)tcp_data;
    // check the TCP header itself
    if ((th + 1) > (struct tcphdr*) data_end)
        return XDP_ABORTED;

    __builtin_memset(&client, 0, sizeof(struct active_client));
    client.saddr = src_addr;
    client.sport = ntohs(th->dest);
	val = bpf_map_lookup_elem(&active_clients_map, &client);
    if (!val) {
        bpf_printk("client not registered %u:%u\n", client.saddr, client.sport);
        return XDP_PASS;
    }

    data_offset = ntohs(th->doff);
    if (data_offset > 0xf) {
        return XDP_ABORTED; // 4-bit bitfield
    }

    payload = ((char*)tcp_data) + data_offset * sizeof(u32);
    // check stated payload location
    if (payload > data_end) {
        return XDP_ABORTED;
    }

    res = shard_generic(payload, data_end, &(th->dest));
    if (res == XDP_ABORTED) { return res; }
    return record_port(ntohs(th->dest), rxq);
}

static inline int parse_udp(void *udp_data, void *data_end, u32 src_addr, u32 rxq)
{
    int res;
    struct udphdr *uh;
    u8 *val;
    struct active_client client;
    __builtin_memset(&client, 0, sizeof(struct active_client));

    uh = (struct udphdr *)udp_data;
    if ((uh + 1) > (struct udphdr*) data_end)
        return XDP_ABORTED;
    
    client.saddr = src_addr;
    client.sport = uh->dest;
	val = bpf_map_lookup_elem(&active_clients_map, &client);
    if (!val) {
        bpf_printk("client not registered %u:%u\n", client.saddr, client.sport);
        return XDP_PASS;
    }

    res = shard_generic((void*) (uh + 1), data_end, &(uh->dest));
    if (res == XDP_ABORTED) { return res; }
    return record_port(ntohs(uh->dest), rxq);
    //dump_stats();
    //return res;
}

static inline int parse_ipv4(void *data, void *data_end, u32 rxq)
{
    u32 src_addr;
    void *trans_data;
    struct iphdr *iph = data;
    trans_data = iph + 1;
    if (trans_data > data_end)
        return XDP_ABORTED;

    src_addr = iph->saddr;

    if (iph->protocol == IPPROTO_TCP) {
        return parse_tcp(trans_data, data_end, src_addr, rxq);
    } else if (iph->protocol ==IPPROTO_UDP) {
        return parse_udp(trans_data, data_end, src_addr, rxq);
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
    
    // TODO ipv6
    if (h_proto == htons(ETH_P_IP)) {
        return parse_ipv4(((char*)data) + nh_off, data_end, rxq);
    }

    return XDP_PASS;
}

SEC("xdp_steer_prog")
int  xdp_steer(struct xdp_md *ctx)
{
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
	if (rxq >= MAX_RXQs) {
		rxq = MAX_RXQs;
    }

    return parse_eth(data, data_end, rxq);
}

char _license[] SEC("license") = "GPL";
