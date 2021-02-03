#FLS:=$(shell fd "\.rs$$")
SRCS := $(shell find . -name "*.rs" | grep -v "target" )
TOMLS := $(shell find . -name "*.toml" )
FLS := $(SRCS) $(TOMLS)

all: sharding localrpc

.PHONY: sharding localrpc

sharding: ./target/release/ycsb ./target/release/kvserver
localrpc: ./target/release/bincode-pingclient ./target/release/bincode-pingserver

./target/release/ycsb: $(FLS)
	cd kvstore-ycsb && cargo build --release --features="use-shenango"

./target/release/kvserver: $(FLS)
	cd kvstore && cargo build --release --features="bin","use-shenango"

./target/release/bincode-pingclient ./target/release/bincode-pingserver: $(FLS)
	cd rpcbench && cargo build --release
