#!/usr/bin/env python3

import sys

numshards = int(sys.argv[1])
fn = sys.argv[2]


FNV1_64_INIT = 0xcbf29ce484222325;
FNV_64_PRIME = 0x100000001b3;

def hash(key):
    k = key[:4]
    h = FNV1_64_INIT
    for b in k:
        h = h ^ ord(b)
        h *= FNV_64_PRIME
    return h

cls = {}
with open(fn, 'r') as f:
    for line in f:
        cl, _, key = line.split()[:3]
        if cl not in cls:
            cls[cl] = {}
        shard = hash(key) % numshards
        if shard not in cls[cl]:
            cls[cl][shard] = 0
        cls[cl][shard] += 1

for cl in cls:
    print("Client", cl)
    for shard in cls[cl]:
        print("Shard", shard, cls[cl][shard])
