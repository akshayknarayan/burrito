#!/bin/bash

set -e
set -x

RLOG="info"
#RLOG="info,rpcbench=trace,bertha=trace,tls-tunnel=trace,burrito-localname-ctl=trace"

mkdir -p /tmp/burrito
sudo docker rm -f rpcbench-srv || true
sudo docker run --mount type=bind,source=/tmp/burrito/,target=/burrito -p 4242:4242 -p 4242:4242/udp -it --name rpcbench-srv -d ubuntu:20.04 /bin/bash
sudo docker cp $1/bincode-pingserver rpcbench-srv:/server
# copy ghostunnel
if [[ $3 != "none" ]]; then
  sudo docker cp $3 rpcbench-srv:/gt
  sudo docker exec -e RUST_LOG=$RLOG rpcbench-srv /server -p=$2 -o /server.trace --encr-ghostunnel-root=/gt $4  "${@:5}"
else
  sudo docker exec -e RUST_LOG=$RLOG rpcbench-srv /server -p=$2 -o /server.trace $4 "${@:5}"
fi
