#!/usr/bin/python3

from kv import ConnectionWrapper, check_machine, get_local, check, get_timeout, write_dpdk_config, write_cfg
import agenda
import argparse
import os
import shutil
import subprocess
import sys
import threading
import time
import toml

def write_shenango_config(conn):
    shenango_config = f"""
host_addr {conn.addr}
host_netmask 255.255.255.0
host_gateway 10.1.1.1
runtime_kthreads 4
runtime_spininng_kthreads 4
runtime_guaranteed_kthreads 4"""
    write_cfg(conn, shenango_config)

def setup_machine(conn, outdir):
    ok = conn.run(f"mkdir -p ~/burrito/{outdir}")
    check(ok, "mk outdir", conn.addr)
    agenda.subtask(f"building burrito on {conn.addr}")
    ok = conn.run("../../.cargo/bin/cargo b --release", wd = "~/burrito/latency-bench")
    check(ok, "build", conn.addr)
    return conn

dpdk_ld_var = "LD_LIBRARY_PATH=/usr/local/lib64:/usr/local/lib:dpdk-direct/dpdk-wrapper/dpdk/install/lib/x86_64-linux-gnu"

def start_server(conn, outf, variant='dpdk'):
    conn.run("sudo pkill -INT latency")
    if 'shenango' in variant:
        conn.run("sudo pkill -INT iokerneld")
        write_shenango_config(conn)
        conn.run("./iokerneld", wd="~/burrito/shenango-chunnel/caladan", sudo=True, background=True)
    elif 'dpdk' in variant:
        write_dpdk_config(conn)
    elif 'kernel' in variant:
        pass
    else:
        raise Exception("unknown datapath")

    time.sleep(2)
    ok = conn.run(f"RUST_LOG=info {dpdk_ld_var} ./target/release/latency-bench -p 4242 --backend {variant} --mode conn --cfg host.config",
            wd="~/burrito",
            sudo=True,
            background=True,
            stdout=f"{outf}.out",
            stderr=f"{outf}.err",
            )
    check(ok, "spawn server", conn.addr)
    agenda.subtask("wait for server check")
    time.sleep(8)
    conn.check_proc(f"latency", f"{outf}.err")

def run_client(conn, server, variant, msg_size, outf):
    conn.run("sudo pkill -INT latency")
    if 'shenango' in variant:
        conn.run("sudo pkill -INT iokerneld")
        write_shenango_config(conn)
        conn.run("./iokerneld", wd="~/burrito/shenango-chunnel/caladan", sudo=True, background=True)
    elif 'dpdk' in variant:
        write_dpdk_config(conn)
    elif 'kernel' in variant:
        pass
    else:
        raise Exception("unknown datapath")

    time.sleep(2)
    agenda.subtask(f"client starting -> {outf}.out")
    ok = conn.run(
        f"RUST_LOG=info {dpdk_ld_var}  ./target/release/latency-bench \
        --backend {variant} \
        --mode conn \
        --cfg host.config \
        --client {server} \
        -p 4242 \
        --length-padding {msg_size} \
        --out-file={outf}.data",
        sudo=True,
        wd="~/burrito",
        stdout=f"{outf}.out",
        stderr=f"{outf}.err",
        )
    check(ok, "latency-bench", conn.addr)
    if 'shenango' in variant:
        conn.run("sudo pkill -INT iokerneld")
    agenda.subtask("client done")

def do_exp(iter_num,
    outdir=None,
    machines=None,
    msg_size=None,
    datapath=None,
    overwrite=None
):
    assert(
        outdir is not None and
        machines is not None and
        msg_size is not None and
        datapath is not None and
        overwrite is not None
    )

    server_prefix = f"{outdir}/{datapath}-msg_size={msg_size}-{iter_num}-lbench_server"
    outf = f"{outdir}/{datapath}-msg_size={msg_size}-{iter_num}-lbench_client"

    for m in machines:
        if m.local:
            m.run(f"mkdir -p {outdir}", wd="~/burrito")
            continue
        m.run(f"rm -rf {outdir}", wd="~/burrito")
        m.run(f"mkdir -p {outdir}", wd="~/burrito")

    if not overwrite and os.path.exists(f"{outf}.data"):
        agenda.task(f"skipping: {outf}.data")
        return True
    else:
        agenda.task(f"running: {outf}.data")

    time.sleep(2)
    server_addr = machines[0].addr
    agenda.task(f"starting: server = {machines[0].addr}, datapath = {datapath}, msg_size = {msg_size}")

    # first one is the server, start the server
    agenda.subtask("starting server")
    start_server(machines[0], server_prefix, variant=datapath)
    time.sleep(7)

    # others are clients
    agenda.task("starting client")
    run_client( machines[1], server_addr, datapath, msg_size, outf)
    agenda.task("client returned")

    # kill the server
    machines[0].run("sudo pkill -9 latency")
    if 'shenango' in datapath:
        machines[0].run("sudo pkill -INT iokerneld")

    for m in machines:
        m.run("rm ~/burrito/*.config")

    agenda.task("get client files")
    for c in machines[1:]:
        try:
            fn = c.get
            if c.local:
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
        except Exception as e:
            agenda.subfailure(f"At least one file missing for {c}: {e}")

    agenda.task("done")
    return True

### Sample config
### [machines]
### server = { access = "127.0.0.1", alt = "192.168.1.2", exp = "10.1.1.2" }
### clients = [
###     { access = "192.168.1.6", exp = "10.1.1.6" },
### ]
###
### [exp]
### msg_size = [0, 128, 1024, 8192]
### datapath = ['dpdk', 'shenango']
###
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, required=True)
    parser.add_argument('--outdir', type=str, required=True)
    parser.add_argument('--overwrite', action='store_true')
    args = parser.parse_args()
    agenda.task(f"reading cfg {args.config}")
    cfg = toml.load(args.config)
    print(cfg)

    outdir = args.outdir

    if len(cfg['machines']['clients']) < 1:
        agenda.failure("Need more machines")
        sys.exit(1)

    if 'msg_size' not in cfg['exp']:
        agenda.failure("Need msg_size")
        sys.exit(1)
    if 'datapath' not in cfg['exp']:
        cfg['exp']['datapath'] = ['dpdk']
    for t in cfg['exp']['datapath']:
        if t not in ['dpdk', 'shenango', 'kernel', 'shenangort']:
            agenda.failure('unknown datapath: ' + t)
            sys.exit(1)

    agenda.task(f"Checking for connection vs experiment ip")
    ips = [cfg['machines']['server']] + cfg['machines']['clients']
    agenda.task(f"connecting to {ips}")
    machines, commits = zip(*[check_machine(ip) for ip in ips])
    # check all the commits are equal
    if not all(c == commits[0] for c in commits):
        agenda.subfailure(f"not all commits equal: {commits}")
        sys.exit(1)

    for m in machines:
        if m.host in ['127.0.0.1', '::1', 'localhost']:
            agenda.subtask(f"Local conn: {m.host}/{m.addr}")
            m.local = True
        else:
            m.local = False

    # build
    agenda.task("building...")
    thread_ok = True
    setups = [threading.Thread(target=setup_machine, args=(m,outdir)) for m in machines]
    [t.start() for t in setups]
    [t.join() for t in setups]
    if not thread_ok:
        agenda.failure("Something went wrong")
        sys.exit(1)
    agenda.task("...done building")

    # copy config file to outdir
    shutil.copy2(args.config, args.outdir)

    for d in cfg['exp']['datapath']:
        for fs in cfg['exp']['msg_size']:
            for i in range(int(cfg['exp']['iters'])):
                do_exp(i,
                    outdir=outdir,
                    machines=machines,
                    msg_size=fs,
                    datapath=d,
                    overwrite=args.overwrite
                    )

    agenda.task("done")
