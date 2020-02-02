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
./target/release/client --addr "http://127.0.0.1:4242" --iters 10000 --work 4 --amount 1000 2> /dev/null > $out/work_sqrts_1000-iters_10000_tcp_localhost_baremetal.data
kill -9 $server

sleep 2

echo "==> baremetal unix"
# server
rm -rf /tmp/burrito/server
./target/release/server --unix-addr "/tmp/burrito/server" &
server=$!
sleep 2
# client
./target/release/client --addr "/tmp/burrito/server" --iters 10000 --work 4 --amount 1000 2> /dev/null > $out/work_sqrts_1000-iters_10000_unix_localhost_baremetal.data
kill -9 $server

sleep 2
echo "--> start burrito-ctl"
sudo DOCKER_HOST=unix:///var/run/burrito-docker.sock docker rm -f rpcbench-redis
sudo DOCKER_HOST=unix:///var/run/burrito-docker.sock docker run --name rpcbench-redis -d -p 6379:6379 redis:5
docker_host_addr=$(sudo DOCKER_HOST=unix:///var/run/burrito-docker.sock  docker network inspect -f '{{range .IPAM.Config}}{{.Gateway}}{{end}}' bridge)
sudo ./target/release/burrito \
    -i /var/run/docker.sock \
    -o /var/run/burrito-docker.sock \
    --redis-addr "redis://localhost:6379" \
    --net-addr=$docker_host_addr \
    > $out/burritoctl.log 2> $out/burritoctl.log &
burritoctl=$!
sleep 15

sudo docker build -t rpcbench:$out .
sudo docker ps -a | grep -v redis | awk '{print $out}' | tail -n +2 | xargs sudo docker rm -f > /dev/null 2> /dev/null
sleep 2

echo "==> container tcp"
# server
sudo docker run --name rpcbench-server -d  rpcbench:$out server -- --port="4242"
sleep 15
container_ip=$(sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' rpcbench-server)

# client 
sudo docker run -it rpcbench:$out client -- \
    --addr http://$container_ip:4242 \
    --amount 1000 -w 4 -i 10000 \
    2> /dev/null > $out/work_sqrts_1000-iters_10000_tcp_localhost_docker.data

sleep 2
sudo docker ps -a | grep -v redis | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f > /dev/null 2> /dev/null
sleep 2

echo "==> container unix (burrito)"
# server
sudo docker run --name rpcbench-server -d rpcbench:$out server --\
    --burrito-addr="rpcbench" \
    --burrito-root="/burrito" \
    --port="4242"
sleep 15
# client
sudo docker run -it rpcbench:$out client --\
    --addr="rpcbench" \
    --burrito-root="/burrito" \
    --amount 1000 -w 4 -i 10000 \
    2> /dev/null > $out/work_sqrts_1000-iters_10000_burrito_localhost_docker.data

sleep 2
sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f
sleep 2

sudo kill -INT $burritoctl
sudo pkill -INT burrito

python3 ./scripts/rpcbench-parse.py $out/work*.data > $out/combined.data
./scripts/rpcbench-plot.r $out/combined.data $out/rpcs.pdf
