#!/bin/bash

sudo DOCKER_HOST=unix:///var/run/burrito-docker.sock docker rm -f redis
sudo DOCKER_HOST=unix:///var/run/burrito-docker.sock docker run --name redis -d -p 6379:6379 redis:5
sleep 5
./target/release/burrito-shard -f -b /tmp/burrito -r "redis://localhost:6379"
sleep 5
./target/release/kvserver -i $1 -p $2
