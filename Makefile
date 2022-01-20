#FLS:=$(shell fd "\.rs$$")
SRCS := $(shell find . -name "*.rs" | grep -v "target" )
TOMLS := $(shell find . -name "*.toml" )

BERTHA_SRCS := $(shell find ./bertha -name "*.rs" -or -name "Cargo.toml" | grep -v "target")
SHARDCTL_SRCS := $(shell find ./burrito-shard-ctl -name "*.rs" -or -name "Cargo.toml" | grep -v "target")
KVSTORE_SRCS := $(shell find ./kvstore -name "*.rs" -or -name "Cargo.toml" | grep -v "target") $(BERTHA_SRCS) $(SHARDCTL_SRCS)
DPDK_SRCS := $(shell find ./dpdk-direct -name "*.rs" -or -name "Cargo.toml" | grep -v "target")
SHENANGO_SRCS := $(shell find ./shenango-chunnel -name "*.rs" -or -name "Cargo.toml" | grep -v "target")
SHENANGO_C_SRCS := $(shell find ./shenango-chunnel/caladan -name "*.c")
YCSB_SRCS := $(shell find ./kvstore-ycsb -name "*.rs" -or -name "Cargo.toml" | grep -v "target") $(KVSTORE_SRCS)
BERTHA_SHENANGO_SRCS := $(shell find ./shenango-bertha -name "*.rs" -or -name "Cargo.toml" | grep -v "target") $(SHENANGO_SRCS)

ALL_FLS := $(SRCS) $(TOMLS)
CARGO := $(shell command -v cargo 2> /dev/null)
ifndef CARGO
	CARGO = ~/.cargo/bin/cargo
endif

all: sharding rpcbench

.PHONY: sharding rpcbench

sharding: ./target/release/ycsb-shenango ./target/release/kvserver-shenango ./target/release/kvserver-shenango-raw ./target/release/ycsb-shenango-raw ./shenango-chunnel/caladan/iokerneld ./target/release/ycsb-kernel ./target/release/kvserver-kernel ./target/release/kvserver-dpdk ./target/release/ycsb-dpdk
rpcbench: ./target/release/bincode-pingclient ./target/release/bincode-pingserver ./target/release/burrito-localname

./target/release/ycsb-shenango: $(YCSB_SRCS) $(SHENANGO_SRCS)
	cd kvstore-ycsb && $(CARGO) build --release --features="use-shenango"
	rm -f ./target/release/ycsb-shenango && cp ./target/release/ycsb ./target/release/ycsb-shenango

./target/release/kvserver-shenango: $(KVSTORE_SRCS) $(SHENANGO_SRCS)
	cd kvstore && $(CARGO) build --release --features="bin,use-shenango"
	rm -f ./target/release/kvserver-shenango && cp ./target/release/kvserver ./target/release/kvserver-shenango

./target/release/ycsb-dpdk: $(YCSB_SRCS) $(DPDK_SRCS)
	cd kvstore-ycsb && $(CARGO) build --release  --features="use-dpdk-direct"
	rm -f ./target/release/ycsb-dpdk && cp ./target/release/ycsb ./target/release/ycsb-dpdk

./target/release/kvserver-dpdk: $(KVSTORE_SRCS) $(DPDK_SRCS)
	cd kvstore && $(CARGO) build --release --features="bin,use-dpdk-direct"
	rm -f ./target/release/kvserver-dpdk && cp ./target/release/kvserver ./target/release/kvserver-dpdk

./target/release/ycsb-kernel: $(YCSB_SRCS)
	cd kvstore-ycsb && $(CARGO) build --release 
	rm -f ./target/release/ycsb-kernel && cp ./target/release/ycsb ./target/release/ycsb-kernel

./target/release/kvserver-kernel: $(KVSTORE_SRCS)
	cd kvstore && $(CARGO) build --release --features="bin"
	rm -f ./target/release/kvserver-kernel && cp ./target/release/kvserver ./target/release/kvserver-kernel

./shenango-chunnel/caladan/iokerneld ./shenango-chunnel/caladan/libbase.a ./shenango-chunnel/caladan/libnet.a ./shenango-chunnel/caladan/libruntime.a: ./shenango-chunnel/caladan/Makefile $(SHENANGO_C_SRCS)
	make -C ./shenango-chunnel/caladan

./target/release/ycsb-shenango-raw ./target/release/kvserver-shenango-raw: $(FLS) ./shenango-chunnel/caladan/libbase.a ./shenango-chunnel/caladan/libnet.a ./shenango-chunnel/caladan/libruntime.a $(BERTHA_SHENANGO_SRCS)
	cd shenango-bertha && $(CARGO) build --release

./target/release/bincode-pingclient ./target/release/bincode-pingserver: $(FLS)
	cd rpcbench && $(CARGO) build --release

./target/release/burrito-localname: $(FLS)
	cd burrito-localname-ctl && $(CARGO) build --release --features="ctl"
