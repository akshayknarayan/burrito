#FLS:=$(shell fd "\.rs$$")
SRCS := $(shell find . -name "*.rs" | grep -v "target" )
TOMLS := $(shell find . -name "*.toml" )
FLS := $(SRCS) $(TOMLS)
CARGO := $(shell command -v cargo 2> /dev/null)

all: sharding rpcbench

.PHONY: sharding rpcbench

sharding: ./target/release/ycsb ./target/release/kvserver-ebpf ./target/release/kvserver-noebpf
rpcbench: ./target/release/bincode-pingclient ./target/release/bincode-pingserver ./target/release/burrito-localname

cargo:
ifndef CARGO
	CARGO := "~/.cargo/bin/cargo"
endif

./target/release/ycsb: $(FLS) cargo
	cd kvstore-ycsb && $(CARGO) build --release --features="use-shenango"

./target/release/kvserver-ebpf: $(FLS) cargo
	cd kvstore && $(CARGO) build --release --features="bin,ebpf,use-shenango"
	rm ./target/release/kvserver-ebpf && cp ./target/release/kvserver ./target/release/kvserver-ebpf

./target/release/kvserver-noebpf: $(FLS) cargo
	cd kvstore && $(CARGO) build --release --features="bin,use-shenango"
	rm ./target/release/kvserver-noebpf && cp ./target/release/kvserver ./target/release/kvserver-noebpf

./target/release/bincode-pingclient ./target/release/bincode-pingserver: $(FLS) cargo
	cd rpcbench && $(CARGO) build --release

./target/release/burrito-localname: $(FLS) cargo
	cd burrito-localname-ctl && $(CARGO) build --release --features="ctl"
