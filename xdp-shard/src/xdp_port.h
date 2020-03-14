/* Map value types shared with userspace. */
#include <linux/types.h>

#define NUM_PORTS 16
struct datarec {
    __u16 ports[NUM_PORTS];
    __u32 counts[NUM_PORTS + 1];
};

struct available_shards {
    __u8 num;
    __u16 ports[16]; // max 16 shards
};
