#!/bin/bash

set -e
set -x

RLOG="error"
#RLOG="info,rpcbench=trace,bertha=trace,tls_tunnel=trace,burrito_localname_ctl=trace"

mkdir -p /tmp/burrito
ps aux | grep pingserver | grep -v "grep" | cut -d' ' -f3 | xargs sudo kill -9 || true
#sudo docker rm -f rpcbench-srv || true
#sudo docker run --mount type=bind,source=/tmp/burrito/,target=/burrito -p 4242:4242 -p 4242:4242/udp -it --name rpcbench-srv -d ubuntu:20.04 /bin/bash
#sudo docker cp $1/bincode-pingserver rpcbench-srv:/server
## copy ghostunnel
#if [[ $3 != "none" ]]; then
#  sudo docker cp $3 rpcbench-srv:/gt
#  sudo docker exec -e RUST_LOG=$RLOG rpcbench-srv /server -p=$2 -o /server.trace --encr-ghostunnel-root=/gt $4  "${@:5}"
#else
#  sudo docker exec -e RUST_LOG=$RLOG rpcbench-srv /server -p=$2 -o /server.trace $4 "${@:5}"
#fi
#
if [[ $3 != "none" ]]; then
  $1/bincode-pingserver -p=$2 --encr-ghostunnel-root=$3 $4  "${@:5}"
else
  $1/bincode-pingserver -p=$2 $4 "${@:5}"
fi
