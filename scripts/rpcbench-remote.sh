#!/bin/bash

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
mkdir ./$out

ssh $1 ls ~/burrito > /dev/null
ok=$?

if [ $ok -gt 0 ]; then
    echo "Could not find ~/burrito on $1"
    exit 2;
fi

cargo build --release &
local_build=$!
ssh $1 "cd ~/burrito && ~/.cargo/bin/cargo build --release" &
remote_build=$!
wait $local_build $remote_build

echo "==> Baremetal TCP"

# server
ssh $1 "cd ~/burrito && ./target/release/server --port \"4242\"" &
ssh_server=$!
sleep 4

perf record -g ./target/release/client --addr "http://$1:4242" --iters 10000 --work 4 --amount 1000 --reqs-per-iter 3 \
    -o $out/work_sqrts_1000-iters_10000_periter_3_tcp_remote_baremetal.data
mv perf.data $out/work_sqrts_1000-iters_10000_periter_3_tcp_remote_baremetal.perf

kill -9 $ssh_server
ssh $1 "ps aux | grep \"release.*server\" | awk '{print \$2}' | head -n1 | xargs kill -9"
echo " -> baremetal TCP done"

sleep 4

echo "--> start burrito-ctl"
sudo DOCKER_HOST=unix:///var/run/burrito-docker.sock docker rm -f rpcbench-redis
sudo DOCKER_HOST=unix:///var/run/burrito-docker.sock docker run --name rpcbench-redis -d -p 6379:6379 redis:5
sudo ./target/release/burrito \
    -i /var/run/docker.sock \
    -o /var/run/burrito-docker.sock \
    --redis-addr "redis://localhost:6379" \
    --net-addr=$3 \
    > $out/burritoctl-local.log 2> $out/burritoctl-local.log &
burritoctl=$!

echo "--> start burrito-ctl on $1"
ssh $1 "mkdir -p ~/burrito/$out"
ssh $1 "cd ~/burrito && sudo ./target/release/burrito -i /var/run/docker.sock -o /var/run/burrito-docker.sock --redis-addr \"redis://$3:6379\" --net-addr=$1 > $out/burritoctl-local.log 2> $out/burritoctl-local.log" &

sleep 4
image_name=rpcbench:`git rev-parse --short HEAD`
sudo docker build -t $image_name . &
local_docker_build=$!
remote_commit=$(ssh $1 "cd ~/burrito && git rev-parse --short HEAD")
if [ -ne $remote_commit `git rev-parse --short HEAD` ]; then
    echo "Remote commit $remote_commit != local commit `git rev-parse --short HEAD`"
    exit 3;
fi
ssh $1 "cd ~/burrito && sudo docker build -t $image_name ." &
remote_docker_build=$!
echo "--> Building docker images"
wait $local_docker_build $remote_docker_build
sleep 2

echo "==> Docker TCP"
sudo docker rm -f rpcclient3
ssh $1 sudo docker rm -f rpcbench-server
ssh $1 sudo docker run --name rpcbench-server -p 4242:4242 -d $image_name ./server --port="4242"
sleep 4
sudo docker run --name rpcclient3 -it $image_name ./client --addr http://$1:4242 --amount 1000 -w 4 -i 10000 --reqs-per-iter 3 \
    -o ./work_sqrts_1000-iters_10000_periter_3_tcp_remote_docker.data 
sudo docker cp rpcclient3:/app/work_sqrts_1000-iters_10000_periter_3_tcp_remote_docker.data $out/work_sqrts_1000-iters_10000_periter_3_tcp_remote_docker.data
sudo docker cp rpcclient3:/app/work_sqrts_1000-iters_10000_periter_3_tcp_remote_docker.trace $out/work_sqrts_1000-iters_10000_periter_3_tcp_remote_docker.trace
echo "-> docker TCP 3 done"
sleep 2

echo "==> Burrito"
sudo docker rm -f rpcclient1 rpcclient3
ssh $1 sudo docker rm -f rpcbench-server
ssh $1 sudo docker run --name rpcbench-server -p 4242:4242 -d $image_name ./server --port="4242" --burrito-addr="pingserver" --burrito-root="/burrito"
sleep 4
sudo docker run --name rpcclient3 -it $image_name ./client --addr "pingserver" \
    --burrito-root="/burrito" --amount 1000 -w 4 -i 10000 --reqs-per-iter 3 \
    -o ./work_sqrts_1000-iters_10000_periter_3_burrito_remote_docker.data
sudo docker cp rpcclient3:/app/work_sqrts_1000-iters_10000_periter_3_burrito_remote_docker.data $out/work_sqrts_1000-iters_10000_periter_3_burrito_remote_docker.data
sudo docker cp rpcclient3:/app/work_sqrts_1000-iters_10000_periter_3_burrito_remote_docker.trace $out/work_sqrts_1000-iters_10000_periter_3_burrito_remote_docker.trace
echo "-> burrito 3 done"

sleep 2
ssh $1 sudo docker rm -f rpcbench-server
sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f
ssh $1 sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f
sleep 2

sudo kill -INT $burritoctl
sudo pkill -INT burrito
ssh $1 "cd ~/burrito && sudo pkill -INT burrito"

python3 ./scripts/rpcbench-parse.py $out/work*.data > $out/combined.data
./scripts/rpcbench-plot.r $out/combined.data $out/rpcs.pdf
