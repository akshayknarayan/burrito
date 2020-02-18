#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: rpcbench-exp.sh <output dir>"
    exit 1;
fi

out="local-$1"
mkdir ./$out
cargo build --release

echo "==> baremetal tcp"
./target/release/server --port "4242" &
server=$!
sleep 2

./target/release/client --addr "http://127.0.0.1:4242" --iters 10000 --work 4 --amount 1000 --reqs-per-iter 3 \
    -o $out/work_sqrts_1000-iters_10000_periter_3_tcp_localhost_baremetal.data || exit 1
kill -9 $server
sleep 2

echo "==> baremetal unix"
rm -rf /tmp/burrito/server
./target/release/server --unix-addr "/tmp/burrito/server" &
server=$!
sleep 2
./target/release/client --addr "/tmp/burrito/server" --iters 10000 --work 4 --amount 1000 --reqs-per-iter 3 \
    -o $out/work_sqrts_1000-iters_10000_periter_3_unix_localhost_baremetal.data || exit 1
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
sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f
sleep 2

docker_host_addr=$(sudo docker network inspect -f '{{range .IPAM.Config}}{{.Gateway}}{{end}}' bridge)
image_name=rpcbench:`git rev-parse --short HEAD`
sudo docker build -t $image_name . || exit 1

# server
sudo docker run --name rpcbench-server -d $image_name ./server --port="4242"
sleep 4
container_ip=$(sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' rpcbench-server)
echo "container ip: $container_ip"

# client 
sudo docker run --name lrpcclient -it $image_name ./client \
    --addr http://$container_ip:4242 \
    --amount 1000 -w 4 -i 10000 \
    --reqs-per-iter 3 \
    -o ./res.data || exit 1
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
echo "--> start burrito-ctl (tonic)"
sudo ./target/release/burrito \
    -i /var/run/docker.sock \
    -o /var/run/burrito-docker.sock \
    --redis-addr "redis://localhost:6379" \
    --net-addr=$docker_host_addr \
    --tracing-file $out/burritoctl-tonic-local.trace \
    --burrito-proto "tonic" \
    > $out/burritoctl-tonic-local.log 2> $out/burritoctl-tonic-local.log &
burritoctl=$!
sleep 2
sudo docker run --name rpcbench-server -d $image_name ./server \
    --burrito-addr="rpcbench" \
    --burrito-root="/burrito" \
    --burrito-proto="tonic" \
    --port="4242"
sleep 2
sudo docker run --name lrpcclient -it $image_name ./client \
    --addr="rpcbench" \
    --burrito-root="/burrito" \
    --burrito-proto="tonic" \
    --amount 1000 -w 4 -i 10000 \
    --reqs-per-iter 3 \
    -o ./res.data || exit 1
sudo docker cp lrpcclient:/app/res.data $out/work_sqrts_1000-iters_10000_periter_3_tonic-burrito_localhost_docker.data
sudo docker cp lrpcclient:/app/res.trace $out/work_sqrts_1000-iters_10000_periter_3_tonic-burrito_localhost_docker.trace

sleep 2
sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f
sleep 2

echo "==> Burrito with flatbuf resolver"
echo "--> start redis"
sudo docker run --name rpcbench-redis -d -p 6379:6379 redis:5
sleep 2
echo "--> stop burrito-ctl (tonic)"
sudo kill -INT $burritoctl
sudo pkill -INT dump-docker 2> /dev/null
sudo pkill -INT burrito 2> /dev/null
sleep 2
echo "--> start burrito-ctl (flatbuf)"
sudo ./target/release/burrito \
    -i /var/run/docker.sock \
    -o /var/run/burrito-docker.sock \
    -f \
    --redis-addr "redis://localhost:6379" \
    --net-addr=$docker_host_addr \
    --tracing-file $out/burritoctl-flatbuf-tracing.trace \
    --burrito-proto "flatbuf" \
    > $out/burritoctl-flatbuf.log 2> $out/burritoctl-flatbuf.log &
burritoctl=$!
sleep 2
sudo docker run --name rpcbench-server -d $image_name ./server \
    --burrito-addr="rpcbench" \
    --burrito-root="/burrito" \
    --burrito-proto "flatbuf" \
    --port="4242"
sleep 2
sudo docker run --name lrpcclient -it $image_name ./client \
    --addr="rpcbench" \
    --burrito-root="/burrito" \
    --burrito-proto "flatbuf" \
    --amount 1000 -w 4 -i 10000 \
    --reqs-per-iter 3 \
    -o ./res.data || exit 1
sudo docker cp lrpcclient:/app/res.data $out/work_sqrts_1000-iters_10000_periter_3_flatbuf-burrito_localhost_docker.data
sudo docker cp lrpcclient:/app/res.trace $out/work_sqrts_1000-iters_10000_periter_3_flatbuf-burrito_localhost_docker.trace

python3 ./scripts/rpcbench-parse.py $out/work*.data > $out/combined.data
