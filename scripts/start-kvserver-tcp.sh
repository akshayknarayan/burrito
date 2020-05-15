#!/bin/bash

sudo pkill -9 burrito-shard
sudo rm -rf /tmp/burrito/*
sudo docker rm -f burrito-shard-redis
sudo docker run --name burrito-shard-redis -d -p 63379:6379 redis:5

#sudo ./target/release/xdp_clear -i "$1"
sleep 2
sudo RUST_LOG=debug ./target/release/burrito-shard -b /tmp/burrito -r "redis://localhost:63379" &
sleep 2
sudo RUST_BACKTRACE=1 RUST_LOG=info,kvstore=debug ./target/debug/kvserver-tcp -i "$1" -p "$2" -n "$3" $4
