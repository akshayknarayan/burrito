#!/bin/bash

# $1 = redis addr
# $2 = interarrival_us
# $3 = client_shard_arg

cd ~/burrito

if [ -z "$4" ]
then
    c1_out="/dev/stdout"
else
    c1_out="$4"
fi
if [ -z "$5" ]
then
    c1_err="/dev/stderr"
else
    c1_err="$5"
fi
if [ -z "$6" ]
then
    c2_out="/dev/stdout"
else
    c2_out="$6"
fi
if [ -z "$7" ]
then
    c2_err="/dev/stderr"
else
    c2_err="$7"
fi

sudo pkill -9 burrito-shard
sudo rm -rf /tmp/burrito/*

RUST_LOG=info ./target/release/burrito-shard -r redis://$1:6379 &

while true
do
    ls /tmp/burrito/shard-controller
    if [ $? -eq 0 ]
    then
        break
    fi
    echo "wait for shardctl"
    sleep 1
done

sleep 2

sudo RUST_LOG=info cset shield --userset=kv --exec ./target/release/ycsb -- \
            --burrito-root="/tmp/burrito" \
            --addr "kv" \
            --accesses ./kvstore-ycsb/ycsbc-mock/wrkloadbunf1-100.access \
            -i $2 -n $3 > "$c1_out" 2> "$c1_err" &
client1=$!
echo "started client 1"

RUST_LOG=info ./target/release/ycsb \
    --burrito-root="/tmp/burrito" \
    --addr "kv" \
    --accesses ./kvstore-ycsb/ycsbc-mock/wrkloadbunf2-100.access \
    -i $2 -n $3 > "$c2_out" 2> "$c2_err" &
client2=$!
echo "started client 2"

wait $client1 $client2
sudo pkill -9 burrito-shard
sudo rm -rf /tmp/burrito/*
