#!/bin/bash

sudo DOCKER_HOST=unix:///var/run/burrito-docker.sock docker rm -f burrito-shard-redis
sudo DOCKER_HOST=unix:///var/run/burrito-docker.sock docker run --name burrito-shard-redis -d -p 6379:6379 redis:5
sleep 5
RUST_LOG=debug ./target/release/burrito-shard -f -b /tmp/burrito -r "redis://localhost:6379" &
sleep 5
./target/release/kvserver -i $1 -p $2 -n $3
