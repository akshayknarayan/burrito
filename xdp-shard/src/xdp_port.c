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
#include "xdp_port.h"

typedef __u64 Fnv64_t;

/*
 * 64 bit magic FNV-0 and FNV-1 prime
 */
#define FNV1_64_INIT ((Fnv64_t)0xcbf29ce484222325ULL)
#define FNV_64_PRIME ((Fnv64_t)0x100000001b3ULL)

/* Given an input [buf; len] and a prior value hval,
 * return the hash value. For the first call, pass hval = FNV1_64_INIT.
 */
//static Fnv64_t inline
//fnv_64_buf(void *buf, __u64 len, Fnv64_t hval)
//{
//    __u64 i;
//    __u64 v;
//    __u8 *b = (__u8*) buf;
//    #pragma clang loop unroll(full)
//    for (i = 0; i < len; i++) {
//        v = (__u64) b[i];
//        hval = hval ^ v;
//        hval *= FNV_64_PRIME;
//    }
//
//    return hval;
//}

// 8-byte fixed
static Fnv64_t inline
fnv_64_buf_fixed(char *buf, Fnv64_t hval)
{
    __u64 i;
    __u64 v;
    __u8 *b = (__u8*) buf;
    #pragma clang loop unroll(full)
    for (i = 0; i < 8; i++) {
        v = (__u64) b[i];
        hval = hval ^ v;
        hval *= FNV_64_PRIME;
    }

    return hval;
}

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

/* Stats per port, per rx_queue_index, per CPU */
struct bpf_map_def SEC("maps") rx_queue_index_map = {
	.type		= BPF_MAP_TYPE_PERCPU_ARRAY,
	.key_size	= sizeof(__u32),
	.value_size	= sizeof(struct datarec),
	.max_entries	= MAX_RXQs + 1,
};

/* Dest. port -> available_shards map for sharding on that port */
struct bpf_map_def SEC("maps") available_shards_map = {
	.type		= BPF_MAP_TYPE_HASH,
	.key_size	= sizeof(__u16),
	.value_size	= sizeof(struct available_shards),
	.max_entries	= 4,
};

// async-bincode length in front of this?
// bincode serialization:
// bytes 0-8 : id (u64)
// bytes 9-12: op (u32)
// bytes 12-18: key length (u64)
// bytes 18-?: key
// byte n: 1 = Some, 0 = None
// bytes n-(n+8) (if Some): val length
// bytes (n+8)-m: val
struct bincode_msg {
    u32 len;
    u64 id;
    u32 op;
    u64 keylen;
    void *key; // key data follows this, so void *key is a pointer to it.
};

// decide which port to send this to.
// the possible options are in available_shards_map
static inline int shard_bincode(void *app_data, void *data_end, u16 *port) {
    struct available_shards *shards;
    struct bincode_msg *m;
    u32 msg_len;
    u8 keylen;
    u64 hash = 0;
    u8 idx = 0;
    u16 le_port = ntohs(*port);
    u8 *k;
    u8 i;
    char fixkey[8] = { 0,0,0,0,0,0,0,0 };
    u16 out_port;

	shards = bpf_map_lookup_elem(&available_shards_map, &le_port);
    if (!shards) {
        bpf_printk("Could not get shards map for %u\n", le_port);
        return XDP_PASS;
    }

    if (shards->num < 1 || shards->num > NUM_PORTS) {
        // sharding disabled
        bpf_printk("Sharding disabled for %u: %u\n", le_port, shards->num);
        return XDP_PASS;
    }

    if (sizeof(struct bincode_msg) + app_data > data_end) {
        bpf_printk("Packet not large enough for bincode msg\n");
        return XDP_ABORTED;
    }

    m = (struct bincode_msg*) app_data;
    msg_len = ntohs(m->len); // only len is networkendian, rest is littleendian
    if (msg_len > sizeof(struct bincode_msg) + 32) {
        msg_len = sizeof(struct bincode_msg) + 32;
    }

    if ((void*) (msg_len + ((char*) app_data)) > data_end) {
        // assumes the whole message is here - i.e. it won't do TCP stream reassembly
        bpf_printk("Packet not large enough for stated bincode msg len: port %u\n", le_port);
        return XDP_ABORTED;
    }

    k = (u8*) &m->key;
    // only take the first 8 bytes of the key
    if (m->keylen > 8) {
        keylen = 8;
        #pragma clang loop unroll(full)
        for (i = 0; i < 8; i++) {
            fixkey[i] = k[i];
        }
    } else {
        keylen = m->keylen;
        #pragma clang loop unroll(full)
        for (i = 0; i < keylen; i++) {
            fixkey[i] = k[i];
        }
    }


    hash = fnv_64_buf_fixed(fixkey, FNV1_64_INIT);
    //hash = fnv_64_buf((void*) &m->key, keylen, FNV1_64_INIT);

    idx = hash % shards->num;

    if (idx > 12) { // should be 16 but that doesn't verify
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

static inline int record_icmp(u32 rxq)
{
    struct datarec *rxq_rec;
	rxq_rec = bpf_map_lookup_elem(&rx_queue_index_map, &rxq);
	if (!rxq_rec)
		return XDP_ABORTED;

    // no port, stick it in the leftover bin.
    rxq_rec->counts[NUM_PORTS]++;
    return XDP_PASS;
}

static inline int parse_tcp(void *tcp_data, void *data_end, u32 rxq)
{
    struct tcphdr *th;
    u16 port;
    int res;
    u8 data_offset;
    void *payload;

    th = (struct tcphdr *)tcp_data;
    // check the TCP header itself
    if ((th + 1) > (struct tcphdr*) data_end)
        return XDP_ABORTED;
    data_offset = ntohs(th->doff);
    if (data_offset > 0xf) {
        return XDP_ABORTED; // 4-bit bitfield
    }

    payload = ((char*)tcp_data) + data_offset * sizeof(u32);
    // check stated payload location
    if (payload > data_end) {
        return XDP_ABORTED;
    }

    port = ntohs(th->dest);
    res = record_port(port, rxq);
    if (res == XDP_ABORTED) { return res; }
    return shard_bincode(payload, data_end, &(th->dest));
}

static inline int parse_udp(void *udp_data, void *data_end, u32 rxq)
{
    int res;
    struct udphdr *uh;
    u16 port;
    uh = (struct udphdr *)udp_data;
    if ((uh + 1) > (struct udphdr*) data_end)
        return XDP_ABORTED;

    port = ntohs(uh->dest);
    res = record_port(port, rxq);
    if (res == XDP_ABORTED) { return res; }
    return shard_bincode((void*) (uh + 1), data_end, &(uh->dest));
}

static inline int parse_ipv4(void *data, void *data_end, u32 rxq)
{
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
