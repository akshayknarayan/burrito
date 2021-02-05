#!/bin/bash

set -e

commit=$(git rev-parse --short HEAD)

echo "===> rpcbench-local"
./scripts/rpcbench-local-bincode.sh $commit
./scripts/rpcbench-local-bincode.sh $commit 256
./scripts/rpcbench-local-bincode.sh $commit 512
#sleep 2
#echo "===> rpcbench-remote"
#./scripts/rpcbench-remote.sh 10.1.1.6 $commit 10.1.1.2
