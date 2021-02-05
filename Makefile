#FLS:=$(shell fd "\.rs$$")
SRCS := $(shell find . -name "*.rs" | grep -v "target" )
TOMLS := $(shell find . -name "*.toml" )
FLS := $(SRCS) $(TOMLS)

all: sharding rpcbench

.PHONY: sharding rpcbench

sharding: ./target/release/ycsb ./target/release/kvserver-ebpf ./target/release/kvserver-noebpf
rpcbench: ./target/release/bincode-pingclient ./target/release/bincode-pingserver ./target/release/burrito-localname

./target/release/ycsb: $(FLS)
	cd kvstore-ycsb && cargo build --release --features="use-shenango"

./target/release/kvserver-ebpf: $(FLS)
	cd kvstore && cargo build --release --features="bin,ebpf,use-shenango"
	rm ./target/release/kvserver-ebpf && cp ./target/release/kvserver ./target/release/kvserver-ebpf

./target/release/kvserver-noebpf: $(FLS)
	cd kvstore && cargo build --release --features="bin,use-shenango"
	rm ./target/release/kvserver-noebpf && cp ./target/release/kvserver ./target/release/kvserver-noebpf

./target/release/bincode-pingclient ./target/release/bincode-pingserver: $(FLS)
	cd rpcbench && cargo build --release

./target/release/burrito-localname: $(FLS)
	cd burrito-localname-ctl && cargo build --release --features="ctl"
