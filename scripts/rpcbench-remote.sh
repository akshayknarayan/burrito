#!/bin/bash

set -e

if [ -z "$1" ]; then
    echo "Usage: rpcbench-remote.sh <remote machine> <output dir> <local ip>"
    exit 1;
fi

if [ -z "$2" ]; then
    echo "Usage: rpcbench-remote.sh <remote machine> <output dir> <local ip>"
    exit 1;
fi

if [ -z "$3" ]; then
    echo "Usage: rpcbench-remote.sh <remote machine> <output dir> <local ip>"
    exit 1;
fi

out="remote-$2"
mkdir -p ./$out

ssh $1 ls ~/burrito > /dev/null
ok=$?

if [ $ok -gt 0 ]; then
    echo "Could not find ~/burrito on $1"
    exit 2;
fi

echo "=> remote: $1"

image_name=rpcbench:`git rev-parse --short HEAD`
echo " -> local build"
cd ~/burrito && sudo docker build -t $image_name . &
local_docker_build=$!
echo " -> remote build"
ssh 10.1.1.6 "cd ~/burrito && sudo docker build -t $image_name ." &
remote_docker_build=$!
echo "-> build docker image $image_name"
wait $local_docker_build $remote_docker_build
sleep 2

ssh $1 mkdir -p ~/burrito/$out

echo "==> Baremetal UDP"
ssh $1 "sudo pkill -9 bincode" || true
sudo pkill -9 bincode-pingclient || true
echo " -> start server"
ssh $1 "cd ~/burrito && ./target/release/bincode-pingserver --port \"4242\"" &
ssh_server=$!
sleep 2
echo " -> start client"
RUST_LOG=info ./target/release/bincode-pingclient --addr "$1:4242" \
    -i 10000 --work "bw:1000" --reqs-per-iter 3 \
    -o $out/work_sqrts_1000-iters_10000_periter_3_udp_remote_baremetal.data 
kill -9 $ssh_server || true
ssh $1 "sudo pkill -9 bincode" || true
echo " -> baremetal UDP done"
sleep 2

echo "==> Docker UDP"

sudo docker rm -f rrpcclient || true
ssh $1 sudo docker rm -f rpcbench-server || true
ssh $1 sudo docker run --name rpcbench-server -e RUST_LOG=debug -p 4242:4242/udp -d $image_name ./bincode-pingserver --port="4242"
sleep 4
sudo docker run --name rrpcclient -e RUST_LOG=debug -d $image_name \
    ./bincode-pingclient --addr "$1:4242" -w "bw:1000" -i 10000 --reqs-per-iter 3 -o ./res.data 
sudo docker container wait rrpcclient
sudo docker cp rrpcclient:/app/res.data $out/work_sqrts_1000-iters_10000_periter_3_udp_remote_docker.data
sudo docker cp rrpcclient:/app/res.trace $out/work_sqrts_1000-iters_10000_periter_3_udp_remote_docker.trace
echo "-> docker UDP done"
sleep 2

echo "==> Burrito"
sudo pkill -9 burrito || true
sleep 2

echo "--> start burrito-ctl locally"
echo "--> start burrito-localname-ctl"
rm -rf /tmp/burrito && mkdir -p /tmp/burrito
sudo RUST_LOG=info,burrito_localname_ctl=debug ./target/release/burrito-localname -f \
    > $out/burritoctl-local.log 2> $out/burritoctl-local.log &
lburritoctl=$!
sleep 2

echo "--> start burrito-ctl on $1"
ssh $1 "mkdir -p ~/burrito/$out"
ssh $1 "rm -rf /tmp/burrito && mkdir -p /tmp/burrito"
echo "--> start burrito-localname-ctl on $1"
ssh $1 "cd ~/burrito && sudo RUST_LOG=debug ./target/release/burrito-localname -f > $out/burritoctl-local-remote.log 2> $out/burritoctl-local-remote.log &"
sleep 2

sudo docker rm -f rrpcclient || true
ssh $1 sudo docker rm -f rpcbench-server || true
mkdir -p /tmp/burrito
echo "--> start rpcbench-server"
ssh $1 "sudo docker run --name rpcbench-server --mount type=bind,source=/tmp/burrito/,target=/burrito -e RUST_LOG=debug -p 4242:4242/udp -d $image_name ./bincode-pingserver --burrito-root=\"/burrito\" --port=\"4242\""
sleep 2
echo "--> start pingclient"
sudo docker run --name rrpcclient \
    --mount type=bind,source=/tmp/burrito/,target=/burrito \
    -t -e RUST_LOG=debug -d $image_name ./bincode-pingclient \
    --burrito-root="/burrito" \
    --addr="$1:4242" \
    -w "bw:1000" -i 10000 --reqs-per-iter 3 \
    -o ./res.data
sudo docker container wait rrpcclient
sudo docker cp rrpcclient:/app/res.data $out/work_sqrts_1000-iters_10000_periter_3_burrito_remote_docker.data
sudo docker cp rrpcclient:/app/res.trace $out/work_sqrts_1000-iters_10000_periter_3_burrito_remote_docker.trace
echo "-> burrito done"

sleep 2
ssh $1 sudo docker rm -f rpcbench-server || true
sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f || true
ssh $1 sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f || true
sleep 2

echo "-> plotting"
python3 ./scripts/rpcbench-parse.py $out/work*.data > $out/combined.data
#./scripts/rpcbench-plot.r $out/combined.data $out/rpcs.pdf
