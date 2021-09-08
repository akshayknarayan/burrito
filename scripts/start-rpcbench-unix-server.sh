#!/bin/bash

set -e
set -x

RLOG="error"
#RLOG="info,rpcbench=trace,bertha=trace,tls-tunnel=trace,burrito-localname-ctl=trace"

mkdir -p /tmp/burrito
rm -f /tmp/burrito/relhc
#sudo docker rm -f rpcbench-srv || true
#sudo docker run --mount type=bind,source=/tmp/burrito/,target=/burrito -p 4242:4242 -p 4242:4242/udp -it --name rpcbench-srv -d ubuntu:20.04 /bin/bash
#sudo docker cp $1/bincode-pingserver rpcbench-srv:/server
#sudo docker exec -e RUST_LOG=$RLOG rpcbench-srv /server --unix-addr=/burrito/relhc -o /server.trace "${@:2}"
$1/bincode-pingserver --unix-addr=/tmp/burrito/relhc "${@:2}"
