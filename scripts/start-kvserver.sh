#!/bin/bash

sudo pkill -9 burrito-shard
sudo rm -rf /tmp/burrito/*
sudo DOCKER_HOST=unix:///var/run/burrito-docker.sock docker rm -f burrito-shard-redis
sudo DOCKER_HOST=unix:///var/run/burrito-docker.sock docker run --name burrito-shard-redis -d -p 6379:6379 redis:5

sudo ./target/release/xdp_clear -i "$1"
sleep 2
sudo RUST_LOG=debug ./target/release/burrito-shard -b /tmp/burrito -r "redis://localhost:6379" &
sleep 2
sudo ./target/release/kvserver -i "$1" -p "$2" -n "$3" "$4"
