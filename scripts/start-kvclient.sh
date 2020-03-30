#!/bin/bash

# $1 = redis addr
# $2 = interarrival_us
# $3 = client_shard_arg

sudo pkill -9 burrito-shard
sudo rm -rf /tmp/burrito/*

RUST_LOG=info ./target/release/burrito-shard -r redis://$1:6379 &

sudo RUST_LOG=info cset shield --userset=kv --exec ./target/release/ycsb -- \
            --burrito-root="/tmp/burrito" \
            --addr "kv" \
            --accesses ./kvstore-ycsb/ycsbc-mock/wrkloadb1-100.access \
            -i $2 -n $3 &
client1=$!
echo "started client 1"

RUST_LOG=info ./target/release/ycsb \
    --burrito-root="/tmp/burrito" \
    --addr "kv" \
    --accesses ./kvstore-ycsb/ycsbc-mock/wrkloadb2-100.access \
    -i $2 -n $3 &
client2=$!
echo "started client 2"

wait $client1 $client2
sudo pkill -9 burrito-shard
sudo rm -rf /tmp/burrito/*
