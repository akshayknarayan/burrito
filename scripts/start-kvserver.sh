#!/bin/bash

if [ -z "$4" ]
then
    srv_out="/dev/stdout"
else
    srv_out="$4"
fi
if [ -z "$5" ]
then
    srv_err="/dev/stderr"
else
    srv_err="$5"
fi
if [ -z "$6" ]
then
    shard_log="None"
else
    shard_log="$6"
fi
if [ -z "$7" ]
then
    shard_out="/dev/stdout"
else
    shard_out="$7"
fi
if [ -z "$8" ]
then
    shard_err="/dev/stderr"
else
    shard_err="$8"
fi

sudo pkill -9 burrito-shard
sudo pkill -9 kvserver
sudo rm -rf /tmp/burrito/*
sudo DOCKER_HOST=unix:///var/run/burrito-docker.sock docker rm -f burrito-shard-redis

sudo DOCKER_HOST=unix:///var/run/burrito-docker.sock docker run --name burrito-shard-redis -d -p 6379:6379 redis:5

sudo ./target/release/xdp_clear -i $1
sleep 2
sudo RUST_LOG=info ./target/release/burrito-shard -b /tmp/burrito -r "redis://localhost:6379" -l $shard_log > $shard_out 2> $shard_err &
sleep 2
if [ "$3" == "noshard" ]
then
    sudo RUST_LOG=info ./target/release/kvserver -i $1 -p 4242 -n $2 --no-shard-ctl > $srv_out 2> $srv_err
else
    sudo RUST_LOG=info ./target/release/kvserver -i $1 -p 4242 -n $2 > $srv_out 2> $srv_err
fi
