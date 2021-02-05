#!/bin/bash

# $1: outdir
# $2: request size

set -e

if [ -z "$1" ]; then
    echo "Usage: rpcbench-exp.sh <output dir> <req size bytes>"
    exit 1;
fi

out="local-$1"
mkdir -p ./$out

echo "==> request size: $2 bytes"
if [ -z "$2" ]; then
    REQSIZE=0
else
    REQSIZE="$2"
fi

rm -rf /tmp/burrito
mkdir -p /tmp/burrito

docker_host_addr=$(sudo docker network inspect -f '{{range .IPAM.Config}}{{.Gateway}}{{end}}' bridge)
image_name=rpcbench:`git rev-parse --short HEAD`
echo "==> Building docker image $image_name"
sudo docker build -t $image_name .

echo "==> using reqsize $REQSIZE"

sudo pkill -9 bincode || true
sudo pkill -9 burrito || true

###################################################

echo "==> baremetal tcp"
RUST_LOG=info ./target/release/bincode-pingserver --port "4242" &
server=$!
sleep 2

RUST_LOG=info ./target/release/bincode-pingclient --addr "127.0.0.1:4242" --iters 10000 --work "bw:1000" --reqs-per-iter 3 -s "$REQSIZE" \
    -o $out/work_"$REQSIZE"B+sqrts_1000-iters_10000_periter_3_tcp_localhost_baremetal.data
kill -9 $server
sleep 2

###################################################

echo "==> baremetal unix"
rm -rf /tmp/burrito/server
RUST_LOG=info ./target/release/bincode-pingserver --unix-addr "/tmp/burrito/server" &
server=$!
sleep 2
RUST_LOG=info ./target/release/bincode-pingclient --unix-addr "/tmp/burrito/server" --iters 10000 --work "bw:1000" --reqs-per-iter 3 -s "$REQSIZE" \
    -o $out/work_"$REQSIZE"B+sqrts_1000-iters_10000_periter_3_unix_localhost_baremetal.data
kill -9 $server
sleep 2

###################################################

echo "==> container tcp"
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
    --addr $container_ip:4242 \
    -w "bw:1000" -i 10000 \
    --reqs-per-iter 3 -s "$REQSIZE" \
    -o ./res.data
echo "-> wait rpcbench-client"
sudo docker container wait lrpcclient
echo "-> rpcbench-client done"
sudo docker cp lrpcclient:/app/res.data $out/work_"$REQSIZE"B+sqrts_1000-iters_10000_periter_3_tcp_localhost_docker.data
sudo docker cp lrpcclient:/app/res.trace $out/work_"$REQSIZE"B+sqrts_1000-iters_10000_periter_3_tcp_localhost_docker.trace

####################################################

echo "==> container unix"

sleep 2
sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f || true
sleep 2

docker_host_addr=$(sudo docker network inspect -f '{{range .IPAM.Config}}{{.Gateway}}{{end}}' bridge)
image_name=rpcbench:`git rev-parse --short HEAD`
sudo docker build -t $image_name .

echo "-> start rpcbench-server"
# server
rm -f /tmp/burrito/server
sudo docker run --name rpcbench-server \
    --mount type=bind,source=/tmp/burrito/,target=/burrito \
    -e RUST_LOG=debug -d $image_name \
    ./bincode-pingserver --unix-addr "/burrito/server"
sleep 4
container_ip=$(sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' rpcbench-server)
echo "container ip: $container_ip"

# client 
echo "-> start rpcbench-client"
sudo docker run --name lrpcclient \
    --mount type=bind,source=/tmp/burrito/,target=/burrito \
    -e RUST_LOG=debug \
    -t -d $image_name \
    ./bincode-pingclient \
    --burrito-root "/burrito"
    --unix-addr "/burrito/server" \
    -w "bw:1000" -i 10000 \
    --reqs-per-iter 3 -s "$REQSIZE" \
    -o ./res.data
echo "-> wait rpcbench-client"
sudo docker container wait lrpcclient
echo "-> rpcbench-client done"
sudo docker cp lrpcclient:/app/res.data $out/work_"$REQSIZE"B+sqrts_1000-iters_10000_periter_3_unix_localhost_docker.data
sudo docker cp lrpcclient:/app/res.trace $out/work_"$REQSIZE"B+sqrts_1000-iters_10000_periter_3_unix_localhost_docker.trace

####################################################

echo "==> Baremetal Burrito"
rm -rf /tmp/burrito
mkdir -p /tmp/burrito
sudo pkill -9 burrito || true
sleep 2
echo "--> start burrito-localname-ctl"
sudo RUST_LOG=info,burrito_localname_ctl=debug ./target/release/burrito-localname -f \
    > $out/burritoctl-local.log 2> $out/burritoctl-local.log &
lburritoctl=$!
sleep 2

sudo RUST_LOG=info ./target/release/bincode-pingserver \
    --burrito-root="/tmp/burrito" \
    --port="4242" &
server=$!
sleep 2
echo "-> start rpcbench-client"
sudo RUST_LOG=info,rpcbench=debug ./target/release/bincode-pingclient \
    --addr="127.0.0.1:4242" \
    --burrito-root="/tmp/burrito" \
    -w "bw:1000" -i 10000 \
    --reqs-per-iter 3 -s "$REQSIZE" \
    -o $out/work_"$REQSIZE"B+sqrts_1000-iters_10000_periter_3_burrito_localhost_baremetal.data
echo "-> rpcbench-client done"
sudo pkill -9 bincode || true

sudo kill -INT $lburritoctl || true
sudo kill -INT $server || true

####################################################

#echo "==> Burrito"
#sudo pkill -9 burrito || true
#sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f || true
#sleep 2
#echo "--> start burrito-localname-ctl"
#sudo RUST_LOG=info,burrito_localname_ctl=debug ./target/release/burrito-localname -f \
#    > $out/sec-burritoctl-local.log 2> $out/sec-burritoctl-local.log &
#lburritoctl=$!
#sleep 2
#echo "--> start rpcbench-server"
#sudo docker run --name rpcbench-server -e RUST_LOG=debug -d $image_name ./bincode-pingserver \
#    --burrito-root="/burrito" \
#    --port="4242"
#sleep 2
#echo "-> start rpcbench-client"
#sudo docker run --name lrpcclient -e RUST_LOG=debug,rpcbench=trace -t -d $image_name ./bincode-pingclient \
#    --addr="$docker_host_addr:4242" \
#    --burrito-root="/burrito" \
#    -w "bw:1000" -i 10000 \
#    --reqs-per-iter 3 -s "$REQSIZE" \
#    -o ./res.data
#echo "-> wait rpcbench-client"
#sudo docker container wait lrpcclient
#echo "-> rpcbench-client done"
#sudo docker logs lrpcclient > $out/work_"$REQSIZE"B+sqrts_1000-iters_10000_periter_3_burrito_localhost_docker.log
#sudo docker cp lrpcclient:/app/res.data $out/work_"$REQSIZE"B+sqrts_1000-iters_10000_periter_3_burrito_localhost_docker.data
#sudo docker cp lrpcclient:/app/res.trace $out/work_"$REQSIZE"B+sqrts_1000-iters_10000_periter_3_burrito_localhost_docker.trace
#
#sudo kill -INT $lburritoctl

####################################################
####################################################
####################################################
####################################################

#echo "-> parse script"
#python3 ./scripts/rpcbench-parse.py $out/work*.data > $out/combined.data
