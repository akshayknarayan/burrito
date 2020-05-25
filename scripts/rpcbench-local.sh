#!/bin/bash

set -e

if [ -z "$1" ]; then
    echo "Usage: rpcbench-exp.sh <output dir>"
    exit 1;
fi

out="local-$1"
mkdir -p ./$out
cd burrito-discovery-ctl && cargo build --release --features "ctl" && cd ..
cd burrito-localname-ctl && cargo build --release --features "ctl,docker" && cd ..
cd rpcbench && cargo build --release && cd ..

echo "==> baremetal tcp"
./target/release/pingserver --port "4242" &
server=$!
sleep 2

./target/release/pingclient --addr "http://127.0.0.1:4242" --iters 10000 --work 4 --amount 1000 --reqs-per-iter 3 \
    -o $out/work_sqrts_1000-iters_10000_periter_3_tcp_localhost_baremetal.data
kill -9 $server
sleep 2

echo "==> baremetal unix"
rm -rf /tmp/burrito/server
./target/release/pingserver --unix-addr "/tmp/burrito/server" &
server=$!
sleep 2
./target/release/pingclient --addr "/tmp/burrito/server" --iters 10000 --work 4 --amount 1000 --reqs-per-iter 3 \
    -o $out/work_sqrts_1000-iters_10000_periter_3_unix_localhost_baremetal.data
kill -9 $server

echo "==> container tcp"
echo "-> start docker-proxy"
sleep 2
sudo ./target/release/dump-docker \
    -i /var/run/docker.sock \
    -o /var/run/burrito-docker.sock \
    > $out/dumpdocker-local.log 2> $out/dumpdocker-local.log &
burritoctl=$!
sleep 5

sleep 2
sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f || true
sleep 2

docker_host_addr=$(sudo docker network inspect -f '{{range .IPAM.Config}}{{.Gateway}}{{end}}' bridge)
image_name=rpcbench:`git rev-parse --short HEAD`
sudo docker build -t $image_name .

echo "-> start rpcbench-server"
# server
sudo docker run --name rpcbench-server -e RUST_LOG=debug -d $image_name ./pingserver --port="4242"
sleep 4
container_ip=$(sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' rpcbench-server)
echo "container ip: $container_ip"

# client 
echo "-> start rpcbench-client"
sudo docker run --name lrpcclient -e RUST_LOG=debug -t -d $image_name ./pingclient \
    --addr http://$container_ip:4242 \
    --amount 1000 -w 4 -i 10000 \
    --reqs-per-iter 3 \
    -o ./res.data
echo "-> wait rpcbench-client"
sudo docker container wait lrpcclient
echo "-> rpcbench-client done"
sudo docker cp lrpcclient:/app/res.data $out/work_sqrts_1000-iters_10000_periter_3_tcp_localhost_docker.data
sudo docker cp lrpcclient:/app/res.trace $out/work_sqrts_1000-iters_10000_periter_3_tcp_localhost_docker.trace

echo "==> container unix (burrito)"
echo " -> stop all containers"
sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f
echo "--> start redis"
sudo docker run --name rpcbench-redis -d -p 6379:6379 redis:5
echo "--> stop docker-proxy"
sudo kill -INT $burritoctl # kill dump-docker
sudo pkill -INT dump-docker
sleep 2
sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f
sleep 2

echo "==> Burrito"
echo "--> start redis"
sudo docker run --name rpcbench-redis -d -p 6379:6379 redis:5
sleep 2
sudo pkill -9 dump-docker 2> /dev/null || true
sudo pkill -9 burrito || true
sleep 2
echo "--> start burrito-discovery-ctl"
sudo RUST_LOG=debug ./target/release/burrito-discovery-ctl \
    --redis-addr "redis://localhost:6379" \
    --net-addr=$docker_host_addr \
    -f \
    > $out/burritoctl-discovery.log 2> $out/burritoctl-discovery.log &
burritoctl=$!
sleep 2
echo "--> start burrito-localname-ctl"
sudo RUST_LOG=debug ./target/release/burrito-localname \
    -i /var/run/docker.sock \
    -o /var/run/burrito-docker.sock \
    -f \
    > $out/burritoctl-local.log 2> $out/burritoctl-local.log &
lburritoctl=$!
sleep 2
sudo docker run --name rpcbench-server -e RUST_LOG=debug -d $image_name ./pingserver \
    --burrito-addr="rpcbench" \
    --burrito-root="/burrito" \
    --port="4242"
sleep 2
echo "-> start rpcbench-client"
sudo docker run --name lrpcclient -e RUST_LOG=debug -t -d $image_name ./pingclient \
    --addr="rpcbench" \
    --burrito-root="/burrito" \
    --amount 1000 -w 4 -i 10000 \
    --reqs-per-iter 3 \
    -o ./res.data
echo "-> wait rpcbench-client"
sudo docker container wait lrpcclient
echo "-> rpcbench-client done"
sudo docker cp lrpcclient:/app/res.data $out/work_sqrts_1000-iters_10000_periter_3_burrito-burrito_localhost_docker.data
sudo docker cp lrpcclient:/app/res.trace $out/work_sqrts_1000-iters_10000_periter_3_burrito-burrito_localhost_docker.trace

sudo kill -INT $lburritoctl
sudo kill -INT $burritoctl

echo "-> parse script"
python3 ./scripts/rpcbench-parse.py $out/work*.data > $out/combined.data
