#!/bin/bash

commit=$(git rev-parse --short HEAD)

./scripts/rpcbench-local.sh $commit
sleep 2
./scripts/rpcbench-remote.sh 10.1.1.6 $commit 10.1.1.2
