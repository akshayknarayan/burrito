#!/bin/bash

# $1 = redis addr
# $2 = interarrival_us
# $3 = client_shard_arg

sudo pkill -9 burrito-shard
sudo rm -rf /tmp/burrito/*

RUST_LOG=info ./target/release/burrito-shard -r "redis://$1:6379" &

sleep 5

sudo RUST_LOG="$RUST_LOG" cset shield --userset=kv --exec ./target/release/ycsb -- \
            --burrito-root="/tmp/burrito" \
            --addr "kv" \
            --accesses ./kvstore-ycsb/ycsbc-mock/wrkloadbunf1-100.access \
            -i "$2" -n "$3" -t "$4" &
client1=$!
echo "started client 1: $client1"

./target/release/ycsb \
    --burrito-root="/tmp/burrito" \
    --addr "kv" \
    --accesses ./kvstore-ycsb/ycsbc-mock/wrkloadbunf2-100.access \
    -i "$2" -n "$3" -t "$4" &
client2=$!
echo "started client 2: $client2"

wait "$client1" "$client2"
sudo pkill -9 burrito-shard
sudo rm -rf /tmp/burrito/*