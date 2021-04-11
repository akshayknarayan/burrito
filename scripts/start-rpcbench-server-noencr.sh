#!/bin/bash

set -e
set -x

mkdir -p /tmp/burrito
sudo docker rm -f rpcbench-srv || true
sudo docker run --mount type=bind,source=/tmp/burrito/,target=/burrito -p 4242:4242 -p 4242:4242/udp -it --name rpcbench-srv -d ubuntu:20.04 /bin/bash
sudo docker cp $1/bincode-pingserver rpcbench-srv:/server
# RUST_LOG=debug,tls_tunnel=trace ./target/release/bincode-pingserver -p 4242 
sudo docker exec -e RUST_LOG=debug rpcbench-srv /server -p=$2 $3
