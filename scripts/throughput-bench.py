#!/usr/bin/python3

from kv import get_local, check, connect_machines, setup_all, intel_devbind
import subprocess
import agenda
import argparse
import os
import sys
import threading
import time
import toml

dpdk_ld_var = "LD_LIBRARY_PATH=/usr/local/lib64:/usr/local/lib:dpdk-direct/dpdk-wrapper/dpdk/install/lib/x86_64-linux-gnu"

def start_server(conn, outf, variant='kernel', use_bertha=False):
    conn.run("sudo pkill -INT throughput")
    if 'shenango' in variant:
        conn.run("sudo pkill -INT iokerneld")
        conn.run("./iokerneld", wd="~/burrito/shenango-chunnel/caladan", sudo=True, background=True)
        cfg = "--cfg shenango.config"
    elif 'dpdk' in variant:
        cfg = "--cfg dpdk.config"
    elif 'kernel' in variant:
        cfg = "--cfg dpdk.config"
    else:
        raise Exception("unknown datapath")

    no_bertha = '--no-bertha' if not use_bertha else ''
    time.sleep(5)
    ok = conn.run(f"RUST_LOG=info {dpdk_ld_var} ./target/release/throughput-bench -p 4242 --datapath {variant} {no_bertha} {cfg} server",
            wd="~/burrito",
            sudo=True,
            background=True,
            stdout=f"{outf}.out",
            stderr=f"{outf}.err",
            )
    check(ok, "spawn server", conn.addr)
    agenda.subtask("wait for server check")
    time.sleep(8)
    conn.check_proc(f"throughput", [f"burrito/{outf}.err", f"burrito/{outf}.out"])

def run_client(conn, server, num_clients, file_size, variant, use_bertha, outf):
    conn.run("sudo pkill -INT throughput")
    if 'shenango' in variant:
        conn.run("sudo pkill -INT iokerneld")
        conn.run("./iokerneld", wd="~/burrito/shenango-chunnel/caladan", sudo=True, background=True)
        cfg = "--cfg shenango.config"
    elif 'dpdk' in variant:
        cfg = "--cfg dpdk.config"
    elif 'kernel' in variant:
        cfg = "--cfg dpdk.config"
    else:
        raise Exception("unknown datapath")

    no_bertha = '--no-bertha' if not use_bertha else ''
    time.sleep(2)
    agenda.subtask(f"client starting -> {outf}.out")
    ok = conn.run(
        f"RUST_LOG=info {dpdk_ld_var}  ./target/release/throughput-bench \
        -p 4242 \
        --datapath {variant} \
        {cfg} \
        {no_bertha} \
        client \
        --addr {server} \
        --download-size {file_size} \
        --num-clients {num_clients} \
        --out-file={outf}.data",
        sudo=True,
        wd="~/burrito",
        stdout=f"{outf}.out",
        stderr=f"{outf}.err",
        )
    check(ok, "throughput-bench", conn.addr)
    if 'shenango' in variant:
        conn.run("sudo pkill -INT iokerneld")
    agenda.subtask("client done")

def do_exp(iter_num,
    outdir=None,
    machines=None,
    file_size=None,
    num_clients=None,
    datapath=None,
    use_bertha=False,
    overwrite=None
):
    assert(
        outdir is not None and
        machines is not None and
        file_size is not None and
        num_clients is not None and
        datapath is not None and
        overwrite is not None
    )

    nobertha = '_nobertha' if not use_bertha else ''
    server_prefix = f"{outdir}/{datapath}{nobertha}-num_clients={num_clients}-file_size={file_size}-{iter_num}-tbench_server"
    outf = f"{outdir}/{datapath}{nobertha}-num_clients={num_clients}-file_size={file_size}-{iter_num}-tbench_client"

    for m in machines:
        if m.local:
            m.run(f"mkdir -p {outdir}", wd="~/burrito")
            continue
        m.run(f"rm -rf {outdir}", wd="~/burrito")
        m.run(f"mkdir -p {outdir}", wd="~/burrito")

    if not overwrite and os.path.exists(f"{outf}-{machines[1].addr}.data"):
        agenda.task(f"skipping: {outf}.data")
        return True
    else:
        agenda.task(f"running: {outf}.data")

    time.sleep(2)
    server_addr = machines[0].addr
    agenda.task(f"starting: server = {machines[0].addr}, datapath = {datapath}, use_bertha = {use_bertha}, num_clients = {num_clients} file_size = {file_size}")

    # first one is the server, start the server
    agenda.subtask("starting server")
    start_server(machines[0], server_prefix, variant=datapath, use_bertha=use_bertha)
    time.sleep(7)

    # others are clients
    agenda.task("starting clients")
    clients = [threading.Thread(target=run_client, args=(
            m,
            server_addr,
            num_clients,
            file_size,
            datapath,
            use_bertha,
            outf,
        ),
    ) for m in machines[1:]]

    [t.start() for t in clients]
    [t.join() for t in clients]
    agenda.task("all clients returned")

    # kill the server
    machines[0].run("sudo pkill -9 throughput")
    if 'shenango' in datapath:
        machines[0].run("sudo pkill -INT iokerneld")

    agenda.task("get server files")
    try:
        if not machines[0].is_local:
            machines[0].get(f"burrito/{server_prefix}.out", local=f"{server_prefix}.out", preserve_mode=False)
            machines[0].get(f"burrito/{server_prefix}.err", local=f"{server_prefix}.err", preserve_mode=False)
    except Exception as e:
        agenda.subfailure(f"Could not get file {server_prefix}.[out,err] from client: {e}")


    def get_files():
        fn = c.get
        if c.is_local:
            agenda.subtask(f"Use get_local: {c.host}")
            fn = get_local

        agenda.subtask(f"getting {outf}-{c.addr}.err")
        fn(
            f"burrito/{outf}.err",
            local=f"{outf}-{c.addr}.err",
            preserve_mode=False,
        )
        agenda.subtask(f"getting {outf}-{c.addr}.out")
        fn(
            f"burrito/{outf}.out",
            local=f"{outf}-{c.addr}.out",
            preserve_mode=False,
        )
        agenda.subtask(f"getting {outf}-{c.addr}.data")
        fn(
            f"burrito/{outf}.data",
            local=f"{outf}-{c.addr}.data",
            preserve_mode=False,
        )

    agenda.task("get client files")
    for c in machines[1:]:
        try:
            get_files()
        except Exception as e:
            agenda.subfailure(f"At least one file missing for {c}: {e}")

    agenda.task("done")
    return True

def setup_machine(conn, outdir, datapaths, dpdk_driver):
    agenda.task(f"[{conn.addr}] setup machine")
    try:
        agenda.subtask(f"[{conn.addr}] make outdir {outdir}")
        ok = conn.run(f"mkdir -p ~/burrito/{outdir}")
        check(ok, "mk outdir", conn.addr)

        # dpdk dependencies, rust, and docker
        ok = conn.run("bash scripts/install-kv-deps.sh", wd="~/burrito")
        check(ok, "install dependencies", conn.addr)

        if 'shenango_channel' in datapaths:
            agenda.subtask("[{conn.addr}] building shenango")
            # need to compile iokerneld
            ok = conn.run("make", wd = "~/burrito/shenango-chunnel/caladan")
            check(ok, "build shenango", conn.addr)

            ok = conn.run("./scripts/setup_machine.sh", wd = "~/burrito/shenango-chunnel/caladan")
            check(ok, "shenango setup-machine", conn.addr)
        elif any('dpdk' in d for d in datapaths):
            ok = conn.run("./usertools/dpdk-hugepages.py -p 2M --setup 10G", wd = "~/burrito/dpdk-direct/dpdk-wrapper/dpdk", sudo=True)
            check(ok, "reserve hugepages", conn.addr)

        needed_features = []
        for d in datapaths:
            if 'dpdk' in d:
                if dpdk_driver == 'mlx' and 'dpdk-direct/cx3_mlx' not in needed_features:
                    needed_features.append('dpdk-direct/cx3_mlx')
                elif dpdk_driver == 'intel' and 'dpdk-direct/xl710_intel' not in needed_features:
                    needed_features.append('dpdk-direct/xl710_intel')
        agenda.subtask(f"building throughput-bench features={needed_features}")
        ok = conn.run(f"~/.cargo/bin/cargo +nightly build --release --features=\"{','.join(needed_features)}\"", wd = "~/burrito/throughput-bench")
        check(ok, "throughput-bench build", conn.addr)
        return conn
    except Exception as e:
        agenda.failure(f"[{conn.addr}] setup_machine failed: {e}")
        global thread_ok
        thread_ok = False
        raise e

### Sample config
### [machines]
### server = { access = "127.0.0.1", alt = "192.168.1.6", exp = "10.1.1.6", mac = "f4:52:14:76:98:a0" } # pd6
### clients = [
###     { access = "192.168.1.3", exp = "10.1.1.3", mac = "f4:52:14:76:a1:a0" }, # pd3
###     { access = "192.168.1.4", exp = "10.1.1.4", mac = "f4:52:14:76:a4:80" }, # pd4
###     { access = "192.168.1.5", exp = "10.1.1.5", mac = "f4:52:14:76:98:30" }, # pd5
### ]
###
### [cfg]
### lcores = "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31"
###
### [cfg.client]
### num-threads = 8
###
### [exp]
### num_clients = [1, 2, 4, 8, 16, 32, 64, 128]
### file_size = [50000000]
### datapath = ['dpdk', 'shenango']
###
if __name__ == '__main__':
    global thread_ok
    thread_ok = True
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, required=True)
    parser.add_argument('--outdir', type=str, required=True)
    parser.add_argument('--overwrite', action='store_true')
    parser.add_argument('--dpdk_driver', type=str, choices=['mlx', 'intel'],  required=False)
    args = parser.parse_args()
    agenda.task(f"reading cfg {args.config}")
    cfg = toml.load(args.config)
    print(cfg)

    outdir = args.outdir

    if len(cfg['machines']['clients']) < 1:
        agenda.failure("Need more machines")
        sys.exit(1)

    if 'bertha' not in cfg['exp']:
        cfg['exp']['bertha'] = [True]
    for t in cfg['exp']['bertha']:
        if t not in [True,False]:
            agenda.failure("No bertha must be bool")
            sys.exit(1)
    if 'num_clients' not in cfg['exp']:
        agenda.failure("Need num_clients")
        sys.exit(1)
    if 'file_size' not in cfg['exp']:
        agenda.failure("Need file_size")
        sys.exit(1)
    if 'datapath' not in cfg['exp']:
        cfg['exp']['datapath'] = ['dpdk']
    for t in cfg['exp']['datapath']:
        if t not in ['dpdkthread', 'dpdkmulti', 'shenango', 'kernel', 'shenangort']:
            agenda.failure('unknown datapath: ' + t)
            sys.exit(1)

    machines = connect_machines(cfg)

    found_local = False
    for m in machines:
        if m.host in ['127.0.0.1', '::1', 'localhost']:
            agenda.subtask(f"Local conn: {m.host}/{m.addr}")
            m.is_local = True
            found_local = True
        else:
            m.is_local = False

    if not found_local:
        subprocess.run(f"mkdir -p {args.outdir}", shell=True)

    setup_all(machines, cfg, args, setup_machine)

    for d in cfg['exp']['datapath']:
        if 'intel' == args.dpdk_driver:
            intel_devbind(machines, d)
        if d == 'dpdk' or d == 'shenango':
            for use_bertha in cfg['exp']['bertha']:
                for fs in cfg['exp']['file_size']:
                    for nc in cfg['exp']['num_clients']:
                        for i in range(int(cfg['exp']['iters'])):
                            do_exp(i,
                                    outdir=outdir,
                                    machines=machines,
                                    num_clients=nc,
                                    file_size=fs,
                                    datapath=d,
                                    use_bertha=use_bertha,
                                    overwrite=args.overwrite
                                    )
        else:
            for fs in cfg['exp']['file_size']:
                for nc in cfg['exp']['num_clients']:
                    for i in range(int(cfg['exp']['iters'])):
                        do_exp(i,
                                outdir=outdir,
                                machines=machines,
                                num_clients=nc,
                                file_size=fs,
                                datapath=d,
                                use_bertha=True,
                                overwrite=args.overwrite
                                )

    agenda.task("done")
