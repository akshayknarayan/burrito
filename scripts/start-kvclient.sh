#!/bin/bash

# $1 = redis addr
# $2 = interarrival_us

sudo rm -rf /tmp/burrito/*

sleep 5

sudo RUST_LOG="$RUST_LOG" ./target/release/ycsb \
            --addr "$1:4242" \
            --accesses ./kvstore-ycsb/ycsbc-mock/wrkloadbunf1-100.access \
            --shenango-config ./shenango-chunnel/client.config \
            -i "$2"
echo "started client"

#./target/release/ycsb \
#    --burrito-root="/tmp/burrito" \
#    --addr "kv" \
#    --accesses ./kvstore-ycsb/ycsbc-mock/wrkloadbunf2-100.access \
#    -i "$2" -n "$3" &
#client2=$!
#echo "started client 2: $client2"

#wait "$client1" "$client2"
sudo rm -rf /tmp/burrito/*
