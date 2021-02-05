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

ssh $1 mkdir -p ~/burrito/$out

echo "==> Baremetal TCP"
ssh $1 "sudo pkill -9 pingserver" || true
echo " -> start server"
ssh $1 "cd ~/burrito && ./target/release/pingserver --port \"4242\"" &
ssh_server=$!
sleep 2
echo " -> start client"
./target/release/pingclient --addr "http://$1:4242" -i 10000 --work 4 --amount 1000 --reqs-per-iter 3 \
    -o $out/work_sqrts_1000-iters_10000_periter_3_tcp_remote_baremetal.data 
kill -9 $ssh_server || true
ssh $1 "sudo pkill -9 pingserver" || true
echo " -> baremetal TCP done"
sleep 2

echo "==> Docker TCP"
echo " -> start docker-proxy"
sudo ./target/release/dump-docker \
    -i /var/run/docker.sock \
    -o /var/run/burrito-docker.sock \
    > $out/dumpdocker-remote.log 2> $out/dumpdocker-remote.log &
burritoctl=$!
echo " -> start remote docker-proxy"
ssh $1 "cd ~/burrito && sudo ./target/release/dump-docker -i /var/run/docker.sock -o /var/run/burrito-docker.sock > $out/dumpdocker.log 2> $out/dumpdocker.log" &
sleep 4

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

sudo docker rm -f rpcclient3 || true
ssh $1 sudo docker rm -f rpcbench-server || true
ssh $1 sudo docker run --name rpcbench-server -p 4242:4242 -d $image_name ./pingserver --port="4242"
#sudo tcpdump -w $out/work_sqrts_1000-iters_10000_periter_3_tcp_remote_docker.pcap -i 10gp1 port 4242 &
sleep 4
sudo docker run --name rpcclient3 -t -d $image_name \
    ./pingclient --addr http://$1:4242 --amount 1000 -w 4 -i 10000 --reqs-per-iter 3 -o ./res.data 
sudo docker container wait rpcclient3
sudo docker cp rpcclient3:/app/res.data $out/work_sqrts_1000-iters_10000_periter_3_tcp_remote_docker.data
sudo docker cp rpcclient3:/app/res.trace $out/work_sqrts_1000-iters_10000_periter_3_tcp_remote_docker.trace
#sudo pkill tcpdump
echo "-> docker TCP done"
sleep 2

echo "==> Burrito"
sudo docker rm -f rpcbench-redis || true
echo "--> start redis"
sudo docker run --name rpcbench-redis -d -p 6379:6379 redis:5

echo "--> stop docker-proxy"
sudo kill -9 $burritoctl || true # kill dump-docker
sudo pkill -9 burrito || true
sudo pkill -9 dump-docker || true
ssh $1 "cd ~/burrito && sudo pkill -9 dump-docker && rm -f /tmp/burrito/controller" || true
rm -f /tmp/burrito/controller # need to rm, dump-docker doesn't have the signal handler to rm TODO
sleep 2

echo "--> start burrito-ctl locally"
echo "--> start burrito-discovery-ctl"
sudo RUST_LOG=info,burrito_discovery_ctl=debug ./target/release/burrito-discovery-ctl \
    --redis-addr "redis://localhost:6379" \
    --net-addr=$3 \
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

echo "--> start burrito-ctl on $1"
ssh $1 "mkdir -p ~/burrito/$out"
ssh $1 "cd ~/burrito && sudo RUST_LOG=debug ./target/release/burrito-discovery-ctl --redis-addr \"redis://$3:6379\" --net-addr=$1 -f > $out/burritoctl-discovery-remote.log 2> $out/burritoctl-discovery-remote.log &"
sleep 2
echo "--> start burrito-localname-ctl on $1"
ssh $1 "cd ~/burrito && sudo RUST_LOG=debug ./target/release/burrito-localname -i /var/run/docker.sock -o /var/run/burrito-docker.sock -f > $out/burritoctl-local-remote.log 2> $out/burritoctl-local-remote.log &"
sleep 2

sudo docker rm -f rpcclient3 || true
ssh $1 sudo docker rm -f rpcbench-server || true
echo "--> start rpcbench-server"
ssh $1 "sudo docker run --name rpcbench-server -e RUST_LOG=debug -p 4242:4242 -d $image_name ./pingserver --burrito-addr=\"pingserver\" --burrito-root=\"/burrito\" --port=\"4242\""
sleep 2
#sudo tcpdump -w $out/work_sqrts_1000-iters_10000_periter_3_burrito_remote_docker.pcap -i 10gp1 port 4242 &
echo "--> start pingclient"
sudo docker run --name rpcclient3 -t -e RUST_LOG=debug -d $image_name ./pingclient \
    --addr "pingserver" \
    --burrito-root="/burrito" \
    --amount 1000 \
    -w 4 -i 10000 --reqs-per-iter 3 \
    -o ./res.data
sudo docker container wait rpcclient3
sudo docker cp rpcclient3:/app/res.data $out/work_sqrts_1000-iters_10000_periter_3_burrito_remote_docker.data
sudo docker cp rpcclient3:/app/res.trace $out/work_sqrts_1000-iters_10000_periter_3_burrito_remote_docker.trace
#sudo pkill tcpdump
echo "-> burrito done"

sleep 2
ssh $1 sudo docker rm -f rpcbench-server || true
sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f || true
ssh $1 sudo docker ps -a | awk '{print $1}' | tail -n +2 | xargs sudo docker rm -f || true
sleep 2

echo "-> plotting"
python3 ./scripts/rpcbench-parse.py $out/work*.data > $out/combined.data
./scripts/rpcbench-plot.r $out/combined.data $out/rpcs.pdf
