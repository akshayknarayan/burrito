#!/bin/bash

set -e

pushd ~

sudo apt update
sudo apt upgrade -y

# install dependencies (first line dpdk, second line ycsbc-mock)
sudo apt install -y  \
    libclang-dev pkg-config meson libnuma-dev \
    libhiredis-dev

# install latest stable rust
if ! command -v cargo &> /dev/null 
then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > ./rustup.sh
    sh ./rustup.sh -y
    rm ./rustup.sh
fi

# docker
if ! command -v docker &> /dev/null
then
    sudo apt install -y ca-certificates curl gnupg lsb-release 

    sudo mkdir -p /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor --yes -o /etc/apt/keyrings/docker.gpg

    echo \
        "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
        $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

    sudo apt update
    sudo apt install -y docker-ce docker-ce-cli containerd.io
fi

popd

# ycsbc-mock
pushd ~/burrito/kvstore-ycsb/ycsbc-mock
make
