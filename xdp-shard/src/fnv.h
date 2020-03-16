typedef __u64 Fnv64_t;

/*
 * 64 bit magic FNV-0 and FNV-1 prime
 */
#define FNV1_64_INIT ((Fnv64_t)0xcbf29ce484222325ULL)
#define FNV_64_PRIME ((Fnv64_t)0x100000001b3ULL)

/* Given an input [buf; len] and a prior value hval,
 * return the hash value. For the first call, pass hval = FNV1_64_INIT.
 */
Fnv64_t inline
fnv_64_buf(void *buf, __u64 len, Fnv64_t hval)
{
    __u64 i;
    __u64 v;
    __u8 *b = (__u8*) buf;
    for (i = 0; i < len; i++) {
        v = (__u64) b[i];
        hval = hval ^ v;
        hval *= FNV_64_PRIME;
    }

    return hval;
}
