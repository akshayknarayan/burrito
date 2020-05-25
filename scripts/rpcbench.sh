#!/bin/bash

set -e

commit=$(git rev-parse --short HEAD)

echo "===> rpcbench-local"
./scripts/rpcbench-local.sh $commit || exit 1
#sleep 2
#echo "===> rpcbench-remote"
#./scripts/rpcbench-remote.sh 10.1.1.6 $commit 10.1.1.2
