#!/bin/bash

set -e
set -x

mkdir -p /tmp/burrito
sudo docker rm -f rpcbench-cli || true
sudo docker run --mount type=bind,source=/tmp/burrito/,target=/burrito -it --name rpcbench-cli -d ubuntu:20.04 /bin/bash
sudo docker cp ./target/release/bincode-pingclient rpcbench-cli:/client

#RLOG="info,rpcbench=trace,bertha=trace,tls-tunnel=trace,burrito-localname-ctl=trace"
RLOG="info"
sudo docker exec -e RUST_LOG=$RLOG rpcbench-cli /client \
    --unix-addr /burrito/relhc --burrito-root=/burrito -w=imm $2 $3 -o="/$4.data" "${@:5}"

# local experiment. get server tracefile
sudo docker cp rpcbench-srv:/server.trace $1/"$4.srvtrace" || true
sudo docker cp rpcbench-cli:/"$4.data" $1/"$4.data"
sudo docker cp rpcbench-cli:/"$4.trace" $1/"$4.trace"

sudo docker rm -f rpcbench-cli
