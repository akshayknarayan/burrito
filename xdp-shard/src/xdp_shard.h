/* Map value types shared with userspace. */
#include <linux/types.h>

#define NUM_PORTS 16
struct datarec {
    __u16 ports[NUM_PORTS];
    __u32 counts[NUM_PORTS + 1];
};

struct shard_rules {
    __u8 msg_offset; // where in the message does the key start? (fixed location)
    __u8 field_size; // if static - easy mode TODO ignored (verifier)
};

struct available_shards {
    __u8 num;
    __u16 ports[16]; // max 16 shards
    struct shard_rules rules;
};
