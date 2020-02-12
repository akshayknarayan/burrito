#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: rpcbench-exp.sh <output dir>"
    exit 1;
fi

out="local-$1"
mkdir ./$out
cargo build --release

echo "==> baremetal tcp"
# server
./target/release/server --port "4242" &
server=$!
sleep 2
# client
./target/release/client --addr "http://127.0.0.1:4242" --iters 10000 --work 4 --amount 1000 --reqs-per-iter 3 \
    -o $out/work_sqrts_1000-iters_10000_periter_3_tcp_localhost_baremetal.data
kill -9 $server

sleep 2

echo "==> baremetal unix"
# server
rm -rf /tmp/burrito/server
./target/release/server --unix-addr "/tmp/burrito/server" &
server=$!
sleep 2
# client
./target/release/client --addr "/tmp/burrito/server" --iters 10000 --work 4 --amount 1000 --reqs-per-iter 3 \
    -o $out/work_sqrts_1000-iters_10000_periter_3_unix_localhost_baremetal.data
kill -9 $server

sleep 2
echo "--> start burrito-ctl"
sudo DOCKER_HOST=unix:///var/run/burrito-docker.sock docker ps -a | awk '{print $out}' | tail -n +2 | xargs sudo DOCKER_HOST=unix:///var/run/burrito-docker.sock docker rm -f
sudo DOCKER_HOST=unix:///var/run/burrito-docker.sock docker run --name rpcbench-redis -d -p 6379:6379 redis:5
docker_host_addr=$(sudo DOCKER_HOST=unix:///var/run/burrito-docker.sock  docker network inspect -f '{{range .IPAM.Config}}{{.Gateway}}{{end}}' bridge)
sudo perf record -g -o $out/burritoctl.perf.data ./target/release/burrito \
    -i /var/run/docker.sock \
    -o /var/run/burrito-docker.sock \
    --redis-addr "redis://localhost:6379" \
    --net-addr=$docker_host_addr \
    --tracing-file $out/burritoctl-tracing.trace \
    > $out/burritoctl.log 2> $out/burritoctl.log &
burritoctl=$!
sleep 15

image_name=rpcbench:`git rev-parse --short HEAD`
echo $image_name
sudo docker build -t $image_name .
sudo docker ps -a | grep -v redis | awk '{print $out}' | tail -n +2 | xargs sudo docker rm -f > /dev/null 2> /dev/null
sleep 2

echo "==> container tcp"
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
    -o ./work_sqrts_1000-iters_10000_periter_3_tcp_localhost_docker.data
sudo docker cp lrpcclient:/app/work_sqrts_1000-iters_10000_periter_3_tcp_localhost_docker.data \
    $out/work_sqrts_1000-iters_10000_periter_3_tcp_localhost_docker.data
sudo docker cp lrpcclient:/app/work_sqrts_1000-iters_10000_periter_3_tcp_localhost_docker.trace \
    $out/work_sqrts_1000-iters_10000_periter_3_tcp_localhost_docker.trace

sleep 2
sudo docker ps -a | grep -v redis | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f > /dev/null 2> /dev/null
sleep 2

echo "==> container unix (burrito)"
# server
sudo docker run --name rpcbench-server -d $image_name ./server \
    --burrito-addr="rpcbench" \
    --burrito-root="/burrito" \
    --port="4242"
sleep 4
# client
sudo docker run --name lrpcclient -it $image_name ./client \
    --addr="rpcbench" \
    --burrito-root="/burrito" \
    --amount 1000 -w 4 -i 10000 \
    --reqs-per-iter 3 \
    -o ./work_sqrts_1000-iters_10000_periter_3_burrito_localhost_docker.data
sudo docker cp lrpcclient:/app/work_sqrts_1000-iters_10000_periter_3_burrito_localhost_docker.data \
    $out/work_sqrts_1000-iters_10000_periter_3_burrito_localhost_docker.data
sudo docker cp lrpcclient:/app/work_sqrts_1000-iters_10000_periter_3_burrito_localhost_docker.trace \
    $out/work_sqrts_1000-iters_10000_periter_3_burrito_localhost_docker.trace

sleep 2
sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f
sleep 2

sudo kill -INT $burritoctl
sudo pkill -INT burrito

python3 ./scripts/rpcbench-parse.py $out/work*.data > $out/combined.data
./scripts/rpcbench-plot.r $out/combined.data $out/rpcs.pdf