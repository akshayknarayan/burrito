#FLS:=$(shell fd "\.rs$$")
SRCS := $(shell find . -name "*.rs" | grep -v "target" )
TOMLS := $(shell find . -name "*.toml" )
FLS := $(SRCS) $(TOMLS)
CARGO := $(shell command -v cargo 2> /dev/null)
ifndef CARGO
	CARGO = ~/.cargo/bin/cargo
endif

all: sharding rpcbench

.PHONY: sharding rpcbench

sharding: ./target/release/ycsb-shenango ./target/release/kvserver-shenango ./target/release/kvserver-shenango-raw ./target/release/ycsb-shenango-raw ./shenango-chunnel/caladan/iokerneld ./target/release/ycsb-kernel ./target/release/kvserver-kernel
rpcbench: ./target/release/bincode-pingclient ./target/release/bincode-pingserver ./target/release/burrito-localname

./target/release/ycsb-shenango: $(FLS)
	cd kvstore-ycsb && $(CARGO) build --release --features="use-shenango"
	rm -f ./target/release/ycsb-shenango && cp ./target/release/ycsb ./target/release/ycsb-shenango

./target/release/kvserver-shenango: $(FLS)
	cd kvstore && $(CARGO) build --release --features="bin,use-shenango"
	rm -f ./target/release/kvserver-shenango && cp ./target/release/kvserver ./target/release/kvserver-shenango

./target/release/ycsb-kernel: $(FLS)
	cd kvstore-ycsb && $(CARGO) build --release 
	rm -f ./target/release/ycsb-kernel && cp ./target/release/ycsb ./target/release/ycsb-kernel

./target/release/kvserver-kernel: $(FLS)
	cd kvstore && $(CARGO) build --release --features="bin"
	rm -f ./target/release/kvserver-kernel && cp ./target/release/kvserver ./target/release/kvserver-kernel

./shenango-chunnel/caladan/iokerneld ./shenango-chunnel/caladan/libbase.a ./shenango-chunnel/caladan/libnet.a ./shenango-chunnel/caladan/libruntime.a: ./shenango-chunnel/caladan/Makefile
	make -C ./shenango-chunnel/caladan

./target/release/ycsb-shenango-raw ./target/release/kvserver-shenango-raw: $(FLS) ./shenango-chunnel/caladan/libbase.a ./shenango-chunnel/caladan/libnet.a ./shenango-chunnel/caladan/libruntime.a
	cd shenango-bertha && $(CARGO) build --release

./target/release/bincode-pingclient ./target/release/bincode-pingserver: $(FLS)
	cd rpcbench && $(CARGO) build --release

./target/release/burrito-localname: $(FLS)
	cd burrito-localname-ctl && $(CARGO) build --release --features="ctl"
