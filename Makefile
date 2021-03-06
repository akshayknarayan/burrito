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

sharding: ./target/release/ycsb ./target/release/kvserver-noebpf
rpcbench: ./target/release/bincode-pingclient ./target/release/bincode-pingserver ./target/release/burrito-localname

./target/release/ycsb: $(FLS)
	cd kvstore-ycsb && $(CARGO) build --release --features="use-shenango"

#./target/release/kvserver-ebpf: $(FLS)
#	cd kvstore && $(CARGO) build --release --features="bin,ebpf,use-shenango"
#	rm -f ./target/release/kvserver-ebpf && cp ./target/release/kvserver ./target/release/kvserver-ebpf

./target/release/kvserver-noebpf: $(FLS)
	cd kvstore && $(CARGO) build --release --features="bin,use-shenango"
	rm -f ./target/release/kvserver-noebpf && cp ./target/release/kvserver ./target/release/kvserver-noebpf

./target/release/bincode-pingclient ./target/release/bincode-pingserver: $(FLS)
	cd rpcbench && $(CARGO) build --release

./target/release/burrito-localname: $(FLS)
	cd burrito-localname-ctl && $(CARGO) build --release --features="ctl"
