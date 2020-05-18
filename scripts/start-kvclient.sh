#!/bin/bash

# $1 = redis addr
# $2 = interarrival_us
# $3 = client_shard_arg

sudo pkill -9 burrito-shard
sudo rm -rf /tmp/burrito/*

RUST_LOG=info ./target/release/burrito-shard -r "redis://$1:6379" &

sleep 5

sudo RUST_LOG="$RUST_LOG" ./target/release/ycsb-shenango \
            --burrito-root="/tmp/burrito" \
            --addr "$1:4242" \
            --accesses ./kvstore-ycsb/ycsbc-mock/wrkloadbunf1-100.access \
            --shenango-config ./ycsb-shenango/client.config \
            -i "$2" -n "$3"
echo "started client"

#./target/release/ycsb \
#    --burrito-root="/tmp/burrito" \
#    --addr "kv" \
#    --accesses ./kvstore-ycsb/ycsbc-mock/wrkloadbunf2-100.access \
#    -i "$2" -n "$3" &
#client2=$!
#echo "started client 2: $client2"

#wait "$client1" "$client2"
sudo pkill -9 burrito-shard
sudo rm -rf /tmp/burrito/*
