#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: rpcbench-exp.sh <output dir>"
    exit 1;
fi

mkdir ./$1

echo "==> baremetal tcp"
# server
cargo run --release --bin server -- --addr "[::1]:4242" &
server=$!
sleep 2
# client
cargo run --bin client --release -- --addr "http://[::1]:4242" --iters 10000 --work 4 --amount 1000 2> /dev/null > $1/work_sqrts_1000-iters_10000_tcp_localhost_baremetal.data
kill -9 $server

sleep 2

echo "==> baremetal unix"
# server
rm -rf /tmp/burrito/server
cargo run --release --bin server -- --addr "/tmp/burrito/server" &
server=$!
sleep 2
# client
cargo run --release --bin client -- --addr "/tmp/burrito/server" --iters 10000 --work 4 --amount 1000 2> /dev/null > $1/work_sqrts_1000-iters_10000_unix_localhost_baremetal.data
kill -9 $server

sleep 2
echo "--> start burrito-ctl"
sudo ./target/release/burrito -i /var/run/docker.sock -o /var/run/burrito-docker.sock > $1/burritoctl.log 2> $1/burritoctl.log &
burritoctl=$!

sleep 2
sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f > /dev/null 2> /dev/null
sleep 2

echo "==> container tcp"
# server
sudo screen -d -m docker run -it rpcbench:latest server -- --addr="0.0.0.0:4242" &
sleep 10
container_ip=$(sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' `sudo docker ps | awk '{print $1}' | tail -n 1`)

# client 
sudo docker run -it rpcbench:latest client -- \
    --addr http://$container_ip:4242 \
    --amount 1000 -w 4 -i 10000 \
    2> /dev/null > $1/work_sqrts_1000-iters_10000_tcp_localhost_docker.data

sleep 2
sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f
sleep 2

echo "==> container unix (burrito)"
# server
sudo screen -d -m docker run -it rpcbench:latest server -- --addr="rpcbench" --burrito-root="/burrito" > $1/burrito_localhost_docker_rpcserver.log 2> $1/burrito_localhost_docker_rpcserver.log &
sleep 15
# client
sudo docker run -it rpcbench:latest client -- --addr="rpcbench" --amount 1000 -w 4 -i 10000 --burrito-root="/burrito" 2> /dev/null > $1/work_sqrts_1000-iters_10000_burrito_localhost_docker.data

sleep 2
sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f

sudo kill -9 $burritoctl

python3 ./scripts/rpcbench-parse.py $1/work*.data > $1/combined.data
./scripts/rpcbench-plot.r $1/combined.data $1/rpcs.pdf
