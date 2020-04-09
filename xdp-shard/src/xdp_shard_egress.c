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

/* Dest. port -> available_shards map for sharding on that port */
struct bpf_map_def SEC("maps") available_shards_map = {
	.type		= BPF_MAP_TYPE_HASH,
	.key_size	= sizeof(__u16),
	.value_size	= sizeof(struct available_shards),
	.max_entries	= 4,
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

    if (((void*) (offset + 4 + ((char*) app_data))) > data_end) {
        bpf_printk("Packet not large enough for msg: %u\n", (data_end - app_data));
        return XDP_ABORTED;
    }
    
    // value start
    pkt_val = ((u8*) app_data) + offset;

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

static inline int parse_udp(void *udp_data, void *data_end)
{
    struct udphdr *uh;
    uh = (struct udphdr *)udp_data;
    if ((uh + 1) > (struct udphdr*) data_end)
        return XDP_ABORTED;

    return shard_generic((void*) (uh + 1), data_end, &(uh->dest));
}

static inline int parse_ipv4(void *data, void *data_end)
{
    void *trans_data;
    struct iphdr *iph = data;
    trans_data = iph + 1;
    if (trans_data > data_end)
        return XDP_ABORTED;

    if (iph->protocol ==IPPROTO_UDP) {
        return parse_udp(trans_data, data_end);
    } else {
        return XDP_PASS;
    }
}

static inline int parse_eth(void *data, void *data_end)
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
        return parse_ipv4(((char*)data) + nh_off, data_end);
    }

    return XDP_PASS;
}

SEC("tx_xdp_steer_prog")
int  xdp_steer(struct xdp_md *ctx)
{
    void *data_end = (void *)(long)ctx->data_end;
    void *data = (void *)(long)ctx->data;
    return parse_eth(data, data_end);
}

char _license[] SEC("license") = "GPL";
