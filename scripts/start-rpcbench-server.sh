#!/bin/bash

set -e
set -x

mkdir -p /tmp/burrito
sudo docker rm -f rpcbench-srv || true
sudo docker run --mount type=bind,source=/tmp/burrito/,target=/burrito -p 4242:4242 -p 4242:4242/udp -it --name rpcbench-srv -d ubuntu:20.04 /bin/bash
sudo docker cp $1/bincode-pingserver rpcbench-srv:/server
# copy ghostunnel
if [[ $3 != "none" ]]; then
  sudo docker cp $3 rpcbench-srv:/gt
  # RUST_LOG=debug,tls_tunnel=trace ./target/release/bincode-pingserver -p 4242 --encr-ghostunnel-root ~/ghostunnel
  sudo docker exec -e RUST_LOG=debug rpcbench-srv /server -p=$2 -o /server.trace --encr-ghostunnel-root=/gt $4
else
  sudo docker exec -e RUST_LOG=debug rpcbench-srv /server -p=$2 -o /server.trace $4
fi
