#!/bin/bash

set -e

if [ -z "$1" ]; then
    echo "Usage: rpcbench-exp.sh <output dir>"
    exit 1;
fi

out="local-$1"
mkdir -p ./$out
cd burrito-discovery-ctl && cargo build --release --features "bin" && cd ..
cd burrito-localname-ctl && cargo build --release --features "ctl,docker" && cd ..
cd rpcbench && cargo build --release && cd ..

sudo pkill -9 bincode || true
sudo pkill -9 burrito || true

echo "==> baremetal tcp"
./target/release/bincode-pingserver --port "4242" &
server=$!
sleep 2

./target/release/bincode-pingclient --addr "http://127.0.0.1:4242" --iters 10000 --work 4 --amount 1000 --reqs-per-iter 3 \
    -o $out/work_sqrts_1000-iters_10000_periter_3_tcp_localhost_baremetal.data
kill -9 $server
sleep 2

###################################################

echo "==> baremetal unix"
rm -rf /tmp/burrito/server
./target/release/bincode-pingserver --unix-addr "/tmp/burrito/server" &
server=$!
sleep 2
./target/release/bincode-pingclient --addr "/tmp/burrito/server" --iters 10000 --work 4 --amount 1000 --reqs-per-iter 3 \
    -o $out/work_sqrts_1000-iters_10000_periter_3_unix_localhost_baremetal.data
kill -9 $server

###################################################

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
sudo docker run --name rpcbench-server -e RUST_LOG=debug -d $image_name ./bincode-pingserver --port="4242"
sleep 4
container_ip=$(sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' rpcbench-server)
echo "container ip: $container_ip"

# client 
echo "-> start rpcbench-client"
sudo docker run --name lrpcclient -e RUST_LOG=debug -t -d $image_name ./bincode-pingclient \
    --addr http://$container_ip:4242 \
    --amount 1000 -w 4 -i 10000 \
    --reqs-per-iter 3 \
    -o ./res.data
echo "-> wait rpcbench-client"
sudo docker container wait lrpcclient
echo "-> rpcbench-client done"
sudo docker cp lrpcclient:/app/res.data $out/work_sqrts_1000-iters_10000_periter_3_tcp_localhost_docker.data
sudo docker cp lrpcclient:/app/res.trace $out/work_sqrts_1000-iters_10000_periter_3_tcp_localhost_docker.trace

###################################################

echo "==> container unix"
rm -f /tmp/burrito/server
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
sudo DOCKER_HOST=unix:///var/run/burrito-docker.sock docker run --name rpcbench-server \
    --mount type=bind,source=/tmp/burrito/,target=/burrito \
    -e RUST_LOG=debug -d $image_name \
    ./bincode-pingserver --unix-addr "/burrito/server"
sleep 4
container_ip=$(sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' rpcbench-server)
echo "container ip: $container_ip"

# client 
echo "-> start rpcbench-client"
sudo DOCKER_HOST=unix:///var/run/burrito-docker.sock docker run --name lrpcclient \
    --mount type=bind,source=/tmp/burrito/,target=/burrito \
    -e RUST_LOG=debug \
    -t -d $image_name \
    ./bincode-pingclient \
    --addr "/burrito/server" \
    --amount 1000 -w 4 -i 10000 \
    --reqs-per-iter 3 \
    -o ./res.data
echo "-> wait rpcbench-client"
sudo docker container wait lrpcclient
echo "-> rpcbench-client done"
sudo docker cp lrpcclient:/app/res.data $out/work_sqrts_1000-iters_10000_periter_3_unix_localhost_docker.data
sudo docker cp lrpcclient:/app/res.trace $out/work_sqrts_1000-iters_10000_periter_3_unix_localhost_docker.trace

###################################################

echo "==> container unix (burrito)"
echo " -> stop all containers"
sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f
echo "--> start redis"
sudo docker run --name rpcbench-redis -d -p 127.0.0.1:6379:6379 redis:5
sleep 2
echo "--> stop docker-proxy"
sudo kill -INT $burritoctl # kill dump-docker
sudo pkill -INT dump-docker
sleep 2
sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f
sleep 2

###################################################

echo "==> Baremetal Burrito"
echo "--> start redis"
sudo docker rm -f rpcbench-redis || true
sudo docker run --name rpcbench-redis -d -p 6379:6379 redis:5
sleep 2
sudo pkill -9 dump-docker 2> /dev/null || true
sudo pkill -9 burrito || true
sleep 2
echo "--> start burrito-discovery-ctl"
sudo RUST_LOG=info,burrito_discovery_ctl=debug ./target/release/burrito-discovery-ctl \
    --redis-addr "redis://localhost:6379" \
    --net-addr=$docker_host_addr \
    -f \
    > $out/burritoctl-discovery.log 2> $out/burritoctl-discovery.log &
burritoctl=$!
sleep 2
echo "--> start burrito-localname-ctl"
sudo RUST_LOG=info,burrito_localname_ctl=debug ./target/release/burrito-localname \
    -i /var/run/docker.sock \
    -o /var/run/burrito-docker.sock \
    -f \
    > $out/burritoctl-local.log 2> $out/burritoctl-local.log &
lburritoctl=$!
sleep 2

sudo RUST_LOG=info ./target/release/bincode-pingserver \
    --burrito-addr="rpcbench" \
    --burrito-root="/tmp/burrito" \
    --port="4242" &
server=$!
sleep 2
echo "-> start rpcbench-client"
sudo RUST_LOG=info,rpcbench=trace,burrito_addr=trace ./target/release/bincode-pingclient \
    --addr="rpcbench" \
    --burrito-root="/tmp/burrito" \
    --amount 1000 -w 4 -i 10000 \
    --reqs-per-iter 3 \
    -o $out/work_sqrts_1000-iters_10000_periter_3_burrito_localhost_baremetal.data
echo "-> rpcbench-client done"
sudo pkill -9 bincode || true

sudo kill -INT $lburritoctl
sudo kill -INT $burritoctl

###################################################

echo "==> Burrito"
echo "--> start redis"
sudo docker rm -f rpcbench-redis || true
sudo docker run --name rpcbench-redis -d -p 127.0.0.1:6379:6379 redis:5
sleep 2
sudo pkill -9 dump-docker 2> /dev/null || true
sudo pkill -9 burrito || true
sleep 2
echo "--> start burrito-discovery-ctl"
sudo RUST_LOG=info,burrito_discovery_ctl=debug ./target/release/burrito-discovery-ctl \
    --redis-addr "redis://localhost:6379" \
    --net-addr=$docker_host_addr \
    -f \
    > $out/sec-burritoctl-discovery.log 2> $out/sec-burritoctl-discovery.log &
burritoctl=$!
sleep 2
echo "--> start burrito-localname-ctl"
sudo RUST_LOG=info,burrito_localname_ctl=debug ./target/release/burrito-localname \
    -i /var/run/docker.sock \
    -o /var/run/burrito-docker.sock \
    -f \
    > $out/sec-burritoctl-local.log 2> $out/sec-burritoctl-local.log &
lburritoctl=$!
sleep 2
sudo docker run --name rpcbench-server -e RUST_LOG=debug -d $image_name ./bincode-pingserver \
    --burrito-addr="rpcbench" \
    --burrito-root="/burrito" \
    --port="4242"
sleep 2
echo "-> start rpcbench-client"
sudo docker run --name lrpcclient -e RUST_LOG=debug,rpcbench=trace,burrito_addr=trace -t -d $image_name ./bincode-pingclient \
    --addr="rpcbench" \
    --burrito-root="/burrito" \
    --amount 1000 -w 4 -i 10000 \
    --reqs-per-iter 3 \
    -o ./res.data
echo "-> wait rpcbench-client"
sudo docker container wait lrpcclient
echo "-> rpcbench-client done"
sudo docker logs lrpcclient > $out/work_sqrts_1000-iters_10000_periter_3_burrito_localhost_docker.log
sudo docker cp lrpcclient:/app/res.data $out/work_sqrts_1000-iters_10000_periter_3_burrito_localhost_docker.data
sudo docker cp lrpcclient:/app/res.trace $out/work_sqrts_1000-iters_10000_periter_3_burrito_localhost_docker.trace

sudo kill -INT $lburritoctl
sudo kill -INT $burritoctl

###################################################
###################################################
###################################################

echo "===> 256B requests"

echo "==> baremetal tcp"
./target/release/bincode-pingserver --port "4242" &
server=$!
sleep 2

./target/release/bincode-pingclient --addr "http://127.0.0.1:4242" --iters 10000 --work 4 --amount 1000 --reqs-per-iter 3 -s 256 \
    -o $out/work_256bsqrts_1000-iters_10000_periter_3_tcp_localhost_baremetal.data
kill -9 $server
sleep 2

###################################################

sudo kill -9 $burritoctl
echo "==> baremetal unix"
rm -rf /tmp/burrito/server
./target/release/bincode-pingserver --unix-addr "/tmp/burrito/server" &
server=$!
sleep 2
./target/release/bincode-pingclient --addr "/tmp/burrito/server" --iters 10000 --work 4 --amount 1000 --reqs-per-iter 3 -s 256 \
    -o $out/work_256bsqrts_1000-iters_10000_periter_3_unix_localhost_baremetal.data
kill -9 $server

###################################################

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

echo "-> start rpcbench-server"
# server
sudo docker run --name rpcbench-server -e RUST_LOG=debug -d $image_name ./bincode-pingserver --port="4242"
sleep 4
container_ip=$(sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' rpcbench-server)
echo "container ip: $container_ip"

# client 
echo "-> start rpcbench-client"
sudo docker run --name lrpcclient -e RUST_LOG=debug -t -d $image_name ./bincode-pingclient \
    --addr http://$container_ip:4242 \
    --amount 1000 -w 4 -i 10000 \
    --reqs-per-iter 3 -s 256 \
    -o ./res.data
echo "-> wait rpcbench-client"
sudo docker container wait lrpcclient
echo "-> rpcbench-client done"
sudo docker cp lrpcclient:/app/res.data $out/work_256bsqrts_1000-iters_10000_periter_3_tcp_localhost_docker.data
sudo docker cp lrpcclient:/app/res.trace $out/work_256bsqrts_1000-iters_10000_periter_3_tcp_localhost_docker.trace

###################################################

echo "==> container unix"
rm -f /tmp/burrito/server
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

echo "-> start rpcbench-server"
# server
sudo DOCKER_HOST=unix:///var/run/burrito-docker.sock docker run --name rpcbench-server \
    --mount type=bind,source=/tmp/burrito/,target=/burrito \
    -e RUST_LOG=debug -d $image_name \
    ./bincode-pingserver --unix-addr "/burrito/server"
sleep 4
container_ip=$(sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' rpcbench-server)
echo "container ip: $container_ip"

# client 
echo "-> start rpcbench-client"
sudo DOCKER_HOST=unix:///var/run/burrito-docker.sock docker run --name lrpcclient \
    --mount type=bind,source=/tmp/burrito/,target=/burrito \
    -e RUST_LOG=debug \
    -t -d $image_name \
    ./bincode-pingclient \
    --addr "/burrito/server" \
    --amount 1000 -w 4 -i 10000 \
    --reqs-per-iter 3 -s 256 \
    -o ./res.data
echo "-> wait rpcbench-client"
sudo docker container wait lrpcclient
echo "-> rpcbench-client done"
sudo docker cp lrpcclient:/app/res.data $out/work_256bsqrts_1000-iters_10000_periter_3_unix_localhost_docker.data
sudo docker cp lrpcclient:/app/res.trace $out/work_256bsqrts_1000-iters_10000_periter_3_unix_localhost_docker.trace

###################################################

echo "==> container unix (burrito)"
echo " -> stop all containers"
sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f
echo "--> start redis"
sudo docker run --name rpcbench-redis -d -p 127.0.0.1:6379:6379 redis:5
sleep 2
echo "--> stop docker-proxy"
sudo kill -INT $burritoctl # kill dump-docker
sudo pkill -INT dump-docker
sleep 2
sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f
sleep 2

###################################################

echo "==> Baremetal Burrito"
echo "--> start redis"
sudo docker rm -f rpcbench-redis || true
sudo docker run --name rpcbench-redis -d -p 127.0.0.1:6379:6379 redis:5
sleep 2
sudo pkill -9 dump-docker 2> /dev/null || true
sudo pkill -9 burrito || true
sleep 2
echo "--> start burrito-discovery-ctl"
sudo RUST_LOG=info,burrito_discovery_ctl=debug ./target/release/burrito-discovery-ctl \
    --redis-addr "redis://localhost:6379" \
    --net-addr=$docker_host_addr \
    -f \
    > $out/burritoctl-discovery.log 2> $out/burritoctl-discovery.log &
burritoctl=$!
sleep 2
echo "--> start burrito-localname-ctl"
sudo RUST_LOG=info,burrito_localname_ctl=debug ./target/release/burrito-localname \
    -i /var/run/docker.sock \
    -o /var/run/burrito-docker.sock \
    -f \
    > $out/burritoctl-local.log 2> $out/burritoctl-local.log &
lburritoctl=$!
sleep 2

sudo RUST_LOG=info ./target/release/bincode-pingserver \
    --burrito-addr="rpcbench" \
    --burrito-root="/tmp/burrito" \
    --port="4242" &
sleep 2
echo "-> start rpcbench-client"
sudo RUST_LOG=info,rpcbench=trace,burrito_addr=trace ./target/release/bincode-pingclient \
    --addr="rpcbench" \
    --burrito-root="/tmp/burrito" \
    --amount 1000 -w 4 -i 10000 \
    --reqs-per-iter 3 -s 256 \
    -o $out/work_256bsqrts_1000-iters_10000_periter_3_burrito_localhost_baremetal.data
echo "-> rpcbench-client done"
sudo pkill -9 bincode || true

sudo kill -INT $lburritoctl
sudo kill -INT $burritoctl

###################################################

echo "==> Container Burrito"
echo "--> start redis"
sudo docker rm -f rpcbench-redis || true
sudo docker run --name rpcbench-redis -d -p 127.0.0.1:6379:6379 redis:5
sleep 2
sudo pkill -9 dump-docker 2> /dev/null || true
sudo pkill -9 burrito || true
sleep 2
echo "--> start burrito-discovery-ctl"
sudo RUST_LOG=info,burrito_discovery_ctl=debug ./target/release/burrito-discovery-ctl \
    --redis-addr "redis://localhost:6379" \
    --net-addr=$docker_host_addr \
    -f \
    > $out/burritoctl-discovery.log 2> $out/burritoctl-discovery.log &
burritoctl=$!
sleep 2
echo "--> start burrito-localname-ctl"
sudo RUST_LOG=info,burrito_localname_ctl=debug ./target/release/burrito-localname \
    -i /var/run/docker.sock \
    -o /var/run/burrito-docker.sock \
    -f \
    > $out/burritoctl-local.log 2> $out/burritoctl-local.log &
lburritoctl=$!
sleep 2
sudo docker run --name rpcbench-server -e RUST_LOG=debug -d $image_name ./bincode-pingserver \
    --burrito-addr="rpcbench" \
    --burrito-root="/burrito" \
    --port="4242"
sleep 2
echo "-> start rpcbench-client"
sudo docker run --name lrpcclient -e RUST_LOG=debug,rpcbench=trace,burrito_addr=trace -t -d $image_name ./bincode-pingclient \
    --addr="rpcbench" \
    --burrito-root="/burrito" \
    --amount 1000 -w 4 -i 10000 \
    --reqs-per-iter 3 -s 256 \
    -o ./res.data
echo "-> wait rpcbench-client"
sudo docker container wait lrpcclient
echo "-> rpcbench-client done"
sudo docker logs lrpcclient > $out/work_sqrts_1000-iters_10000_periter_3_burrito_localhost_docker.log
sudo docker cp lrpcclient:/app/res.data $out/work_256bsqrts_1000-iters_10000_periter_3_burrito_localhost_docker.data
sudo docker cp lrpcclient:/app/res.trace $out/work_256bsqrts_1000-iters_10000_periter_3_burrito_localhost_docker.trace

sudo kill -INT $lburritoctl
sudo kill -INT $burritoctl

###################################################
###################################################
###################################################

echo "===> 10k requests"

echo "==> baremetal tcp"
./target/release/bincode-pingserver --port "4242" &
server=$!
sleep 2

./target/release/bincode-pingclient --addr "http://127.0.0.1:4242" --iters 10000 --work 4 --amount 1000 --reqs-per-iter 3 -s 10240 \
    -o $out/work_10ksqrts_1000-iters_10000_periter_3_tcp_localhost_baremetal.data
kill -9 $server
sleep 2

###################################################

echo "==> baremetal unix"
rm -rf /tmp/burrito/server
./target/release/bincode-pingserver --unix-addr "/tmp/burrito/server" &
server=$!
sleep 2
./target/release/bincode-pingclient --addr "/tmp/burrito/server" --iters 10000 --work 4 --amount 1000 --reqs-per-iter 3 -s 10240 \
    -o $out/work_10ksqrts_1000-iters_10000_periter_3_unix_localhost_baremetal.data
kill -9 $server

##################################################

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

echo "-> start rpcbench-server"
# server
sudo docker run --name rpcbench-server -e RUST_LOG=trace -d $image_name ./bincode-pingserver --port="4242"
sleep 4
container_ip=$(sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' rpcbench-server)
echo "container ip: $container_ip"

# client 
echo "-> start rpcbench-client"
sudo docker run --name lrpcclient -e RUST_LOG=trace -t -d $image_name ./bincode-pingclient \
    --addr http://$container_ip:4242 \
    --amount 1000 -w 4 -i 10000 \
    --reqs-per-iter 3 -s 10240 \
    -o ./res.data
echo "-> wait rpcbench-client"
sudo docker container wait lrpcclient
echo "-> rpcbench-client done"
sudo docker cp lrpcclient:/app/res.data $out/work_10ksqrts_1000-iters_10000_periter_3_tcp_localhost_docker.data
sudo docker cp lrpcclient:/app/res.trace $out/work_10ksqrts_1000-iters_10000_periter_3_tcp_localhost_docker.trace

###################################################

echo "==> container unix"
rm -f /tmp/burrito/server
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

echo "-> start rpcbench-server"
# server
sudo DOCKER_HOST=unix:///var/run/burrito-docker.sock docker run --name rpcbench-server \
    --mount type=bind,source=/tmp/burrito/,target=/burrito \
    -e RUST_LOG=debug -d $image_name \
    ./bincode-pingserver --unix-addr "/burrito/server"
sleep 4
container_ip=$(sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' rpcbench-server)
echo "container ip: $container_ip"

# client 
echo "-> start rpcbench-client"
sudo DOCKER_HOST=unix:///var/run/burrito-docker.sock docker run --name lrpcclient \
    --mount type=bind,source=/tmp/burrito/,target=/burrito \
    -e RUST_LOG=debug \
    -t -d $image_name \
    ./bincode-pingclient \
    --addr "/burrito/server" \
    --amount 1000 -w 4 -i 10000 \
    --reqs-per-iter 3 -s 10240 \
    -o ./res.data
echo "-> wait rpcbench-client"
sudo docker container wait lrpcclient
echo "-> rpcbench-client done"
sudo docker cp lrpcclient:/app/res.data $out/work_10ksqrts_1000-iters_10000_periter_3_unix_localhost_docker.data
sudo docker cp lrpcclient:/app/res.trace $out/work_10ksqrts_1000-iters_10000_periter_3_unix_localhost_docker.trace

###################################################

echo "==> container unix (burrito)"
echo " -> stop all containers"
sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f
echo "--> start redis"
sudo docker run --name rpcbench-redis -d -p 127.0.0.1:6379:6379 redis:5
sleep 2
echo "--> stop docker-proxy"
sudo kill -INT $burritoctl # kill dump-docker
sudo pkill -INT dump-docker
sleep 2
sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f
sleep 2

###################################################

echo "==> Baremetal Burrito"
echo "--> start redis"
sudo docker rm -f rpcbench-redis || true
sudo docker run --name rpcbench-redis -d -p 127.0.0.1:6379:6379 redis:5
sleep 2
sudo pkill -9 dump-docker 2> /dev/null || true
sudo pkill -9 burrito || true
sleep 2
echo "--> start burrito-discovery-ctl"
sudo RUST_LOG=info,burrito_discovery_ctl=debug ./target/release/burrito-discovery-ctl \
    --redis-addr "redis://localhost:6379" \
    --net-addr=$docker_host_addr \
    -f \
    > $out/burritoctl-discovery.log 2> $out/burritoctl-discovery.log &
burritoctl=$!
sleep 2
echo "--> start burrito-localname-ctl"
sudo RUST_LOG=info,burrito_localname_ctl=debug ./target/release/burrito-localname \
    -i /var/run/docker.sock \
    -o /var/run/burrito-docker.sock \
    -f \
    > $out/burritoctl-local.log 2> $out/burritoctl-local.log &
lburritoctl=$!
sleep 2

sudo pkill -9 bincode || true
sudo RUST_LOG=info ./target/release/bincode-pingserver \
    --burrito-addr="rpcbench" \
    --burrito-root="/tmp/burrito" \
    --port="4242" &
sleep 2
echo "-> start rpcbench-client"
sudo RUST_LOG=info,rpcbench=trace,burrito_addr=trace ./target/release/bincode-pingclient \
    --addr="rpcbench" \
    --burrito-root="/tmp/burrito" \
    --amount 1000 -w 4 -i 10000 \
    --reqs-per-iter 3 -s 10240 \
    -o $out/work_10ksqrts_1000-iters_10000_periter_3_burrito_localhost_baremetal.data
echo "-> rpcbench-client done"
sudo pkill -9 pingserver || true

sudo kill -INT $lburritoctl
sudo kill -INT $burritoctl

###################################################

echo "==> Burrito"
echo "--> start redis"
sudo docker rm -f rpcbench-redis || true
sudo docker run --name rpcbench-redis -d -p 127.0.0.1:6379:6379 redis:5
sleep 2
sudo pkill -9 dump-docker 2> /dev/null || true
sudo pkill -9 burrito || true
sleep 2
echo "--> start burrito-discovery-ctl"
sudo RUST_LOG=info,burrito_discovery_ctl=debug ./target/release/burrito-discovery-ctl \
    --redis-addr "redis://localhost:6379" \
    --net-addr=$docker_host_addr \
    -f \
    > $out/burritoctl-discovery.log 2> $out/burritoctl-discovery.log &
burritoctl=$!
sleep 2
echo "--> start burrito-localname-ctl"
sudo RUST_LOG=info,burrito_localname_ctl=debug ./target/release/burrito-localname \
    -i /var/run/docker.sock \
    -o /var/run/burrito-docker.sock \
    -f \
    > $out/sec-burritoctl-local.log 2> $out/sec-burritoctl-local.log &
lburritoctl=$!
sleep 2
sudo docker run --name rpcbench-server -e RUST_LOG=debug -d $image_name ./bincode-pingserver \
    --burrito-addr="rpcbench" \
    --burrito-root="/burrito" \
    --port="4242"
sleep 2
echo "-> start rpcbench-client"
sudo docker run --name lrpcclient -e RUST_LOG=debug,rpcbench=trace,burrito_addr=trace -t -d $image_name ./bincode-pingclient \
    --addr="rpcbench" \
    --burrito-root="/burrito" \
    --amount 1000 -w 4 -i 10000 \
    --reqs-per-iter 3 -s 10240 \
    -o ./res.data
echo "-> wait rpcbench-client"
sudo docker container wait lrpcclient
echo "-> rpcbench-client done"
sudo docker logs lrpcclient > $out/work_sqrts_1000-iters_10000_periter_3_burrito_localhost_docker.log
sudo docker cp lrpcclient:/app/res.data $out/work_10ksqrts_1000-iters_10000_periter_3_burrito_localhost_docker.data
sudo docker cp lrpcclient:/app/res.trace $out/work_10ksqrts_1000-iters_10000_periter_3_burrito_localhost_docker.trace

sudo kill -INT $lburritoctl
sudo kill -INT $burritoctl

###################################################
###################################################
###################################################
###################################################

echo "-> parse script"
python3 ./scripts/rpcbench-parse.py $out/work*.data > $out/combined.data
