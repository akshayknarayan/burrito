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
ssh $1 "ps aux | grep \"release.*server\" | awk '{print \$2}' | head -n1 | xargs kill -9"
ssh $1 "cd ~/burrito && ./target/release/server --port \"4242\"" &
ssh_server=$!
sleep 2
./target/release/client --addr "http://$1:4242" -i 10000 --work 4 --amount 1000 --reqs-per-iter 3 \
    -o $out/work_sqrts_1000-iters_10000_periter_3_tcp_remote_baremetal.data || exit 1
kill -9 $ssh_server
ssh $1 "ps aux | grep \"release.*server\" | awk '{print \$2}' | head -n1 | xargs kill -9"
echo " -> baremetal TCP done"
sleep 2

echo "==> Docker TCP"
echo " -> start docker-proxy"
sudo ./target/release/dump-docker \
    -i /var/run/docker.sock \
    -o /var/run/burrito-docker.sock \
    > $out/dumpdocker-remote.log 2> $out/dumpdocker-remote.log &
burritoctl=$!
ssh $1 "cd ~/burrito && sudo ./target/release/dump-docker -i /var/run/docker.sock -o /var/run/burrito-docker.sock > $out/dumpdocker.log 2> $out/dumpdocker.log" &
sleep 4

image_name=rpcbench:`git rev-parse --short HEAD`
image_name=rpcbench:`git rev-parse --short HEAD`
sudo docker build -t $image_name . &
local_docker_build=$!
ssh 10.1.1.6 "cd ~/burrito && sudo docker build -t $image_name ." &
remote_docker_build=$!
echo "-> build docker image $image_name"
wait $local_docker_build $remote_docker_build
sleep 2

sudo docker rm -f rpcclient3 || exit 1
ssh $1 sudo docker rm -f rpcbench-server
ssh $1 sudo docker run --name rpcbench-server -p 4242:4242 -d $image_name ./server --port="4242"
sleep 4
sudo docker run --name rpcclient3 -it $image_name \
    ./client --addr http://$1:4242 --amount 1000 -w 4 -i 10000 --reqs-per-iter 3 -o ./res.data || exit 1
sudo docker cp rpcclient3:/app/res.data $out/work_sqrts_1000-iters_10000_periter_3_tcp_remote_docker.data
sudo docker cp rpcclient3:/app/res.trace $out/work_sqrts_1000-iters_10000_periter_3_tcp_remote_docker.trace
echo "-> docker TCP done"
sleep 2

echo "==> Burrito (tonic)"
sudo docker rm -f rpcbench-redis
echo "--> start redis"
sudo docker run --name rpcbench-redis -d -p 6379:6379 redis:5

echo "--> stop docker-proxy"
sudo kill -INT $burritoctl # kill dump-docker
ssh $1 "cd ~/burrito && sudo pkill -INT dump-docker"
sleep 2

echo "--> start burrito-ctl (tonic) locally"
sudo ./target/release/burrito \
    -i /var/run/docker.sock \
    -o /var/run/burrito-docker.sock \
    --redis-addr "redis://localhost:6379" \
    --net-addr=$3 \
    --tracing-file $out/burritoctl-tonic-remote.trace \
    --burrito-proto "tonic" \
    > $out/burritoctl-tonic-remote.log 2> $out/burritoctl-tonic-remote.log &
burritoctl=$!

echo "--> start burrito-ctl (tonic) on $1"
ssh $1 "mkdir -p ~/burrito/$out"
ssh $1 "cd ~/burrito && sudo ./target/release/burrito -i /var/run/docker.sock -o /var/run/burrito-docker.sock --redis-addr \"redis://$3:6379\" --net-addr=$3 --tracing-file $out/burritoctl-tonic-remote.trace  --burrito-mode \"tonic\" > $out/burritoctl-tonic-remote.log 2> $out/burritoctl-tonic-remote.log &"

sudo docker rm -f rpcclient1 rpcclient3
ssh $1 sudo docker rm -f rpcbench-server
ssh $1 sudo docker run --name rpcbench-server -p 4242:4242 -d $image_name ./server \
    --burrito-addr="pingserver" \
    --burrito-root="/burrito" \
    --burrito-proto="tonic" \
    --port="4242"
sleep 2
sudo docker run --name rpcclient3 -it $image_name ./client \
    --addr "pingserver" \
    --burrito-root="/burrito" \
    --burrito-proto="tonic" \
    --amount 1000 \
    -w 4 -i 10000 --reqs-per-iter 3 \
    -o ./res.data || exit 1
sudo docker cp rpcclient3:/app/res.data $out/work_sqrts_1000-iters_10000_periter_3_tonic-burrito_remote_docker.data
sudo docker cp rpcclient3:/app/res.trace $out/work_sqrts_1000-iters_10000_periter_3_tonic-burrito_remote_docker.trace
echo "-> burrito (tonic) done"

sleep 2
ssh $1 sudo docker rm -f rpcbench-server
sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f
ssh $1 sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f
sleep 2

sudo kill -INT $burritoctl
sudo pkill -INT burrito
ssh $1 "cd ~/burrito && sudo pkill -INT burrito"
sleep 2

echo "==> Burrito (flatbuf)"
sudo docker rm -f rpcbench-redis
echo "--> start redis"
sudo docker run --name rpcbench-redis -d -p 6379:6379 redis:5

echo "--> stop burrito-ctl (flatbuf)"
sudo kill -INT $burritoctl # kill dump-docker
ssh $1 "cd ~/burrito && sudo pkill -INT burrito"
sleep 2

echo "--> start burrito-ctl (flatbuf) locally"
sudo ./target/release/burrito \
    -i /var/run/docker.sock \
    -o /var/run/burrito-docker.sock \
    --redis-addr "redis://localhost:6379" \
    --net-addr=$3 \
    --tracing-file $out/burritoctl-flatbuf-remote.trace \
    --burrito-proto "flatbuf" \
    > $out/burritoctl-flatbuf-remote.log 2> $out/burritoctl-flatbuf-remote.log &
burritoctl=$!

echo "--> start burrito-ctl (flatbuf) on $1"
ssh $1 "mkdir -p ~/burrito/$out"
ssh $1 "cd ~/burrito && sudo ./target/release/burrito -i /var/run/docker.sock -o /var/run/burrito-docker.sock --redis-addr \"redis://$3:6379\" --net-addr=$3 --tracing-file $out/burritoctl-flatbuf-remote.trace --burrito-mode \"flatbuf\" > $out/burritoctl-flatbuf-remote.log 2> $out/burritoctl-flatbuf-remote.log &"

sudo docker rm -f rpcclient1 rpcclient3
ssh $1 sudo docker rm -f rpcbench-server
ssh $1 sudo docker run --name rpcbench-server -p 4242:4242 -d $image_name ./server \
    --burrito-addr="pingserver" \
    --burrito-root="/burrito" \
    --burrito-proto="flatbuf" \
    --port="4242"
sleep 2
sudo docker run --name rpcclient3 -it $image_name ./client \
    --addr "pingserver" \
    --burrito-root="/burrito" \
    --burrito-proto="flatbuf" \
    --amount 1000 \
    -w 4 -i 10000 --reqs-per-iter 3 \
    -o ./res.data || exit 1
sudo docker cp rpcclient3:/app/res.data $out/work_sqrts_1000-iters_10000_periter_3_flatbuf-burrito_remote_docker.data
sudo docker cp rpcclient3:/app/res.trace $out/work_sqrts_1000-iters_10000_periter_3_flatbuf-burrito_remote_docker.trace
echo "-> burrito (flatbuf) done"

sleep 2
ssh $1 sudo docker rm -f rpcbench-server
sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f
ssh $1 sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f
sleep 2

sudo kill -INT $burritoctl
sudo pkill -INT burrito
ssh $1 "cd ~/burrito && sudo pkill -INT burrito"
sleep 2

python3 ./scripts/rpcbench-parse.py $out/work*.data > $out/combined.data
./scripts/rpcbench-plot.r $out/combined.data $out/rpcs.pdf
