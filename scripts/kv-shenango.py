#!/usr/bin/python3

from kv import connect_machines, setup_all, get_local, check, get_timeout_local, write_cfg, write_shenango_config
import agenda
import argparse
import os
import shutil
import subprocess
import sys
import threading
import time
import toml

def start_server(conn, outf, shards=1, no_chunnels=False):
    conn.run("sudo pkill -INT kvserver")
    conn.run("sudo pkill -INT iokerneld")

    conn.run(f"./iokerneld ias nicpci {conn.pci_addr}", wd="~/burrito/shenango-chunnel/caladan", sudo=True, background=True)
    time.sleep(2)
    nochunnels = ''
    if no_chunnels:
        nochunnels = '-nochunnels'

    ok = conn.run(f"./target/release/kvserver-shenango-raw{nochunnels} --addr {conn.addr}:4242 --num-shards {shards} --cfg shenango.config",
            wd="~/burrito",
            sudo=True,
            background=True,
            stdout=f"{outf}.out",
            stderr=f"{outf}.err",
            )
    check(ok, "spawn server", conn.addr)
    agenda.subtask("wait for kvserver check")
    time.sleep(8)
    conn.check_proc(f"kvserver", [f"{outf}.err"])

def run_client(conn, server, interarrival, poisson_arrivals, no_chunnels, outf, wrkfile):
    conn.run("sudo pkill -INT iokerneld")

    timeout = get_timeout_local(wrkfile, interarrival)
    conn.run(f"./iokerneld ias nicpci {conn.pci_addr}", wd="~/burrito/shenango-chunnel/caladan", sudo=True, background=True)
    time.sleep(2)
    poisson_arg = "--poisson-arrivals" if poisson_arrivals  else ''

    nochunnels = ''
    shards_arg = ''
    if no_chunnels is not None:
        nochunnels = '-nochunnels'
        shards_arg = f'--num-shards={no_chunnels}'
    agenda.subtask(f"client starting, timeout {timeout} -> {outf}0.out")
    ok = conn.run(f"RUST_LOG=info ./target/release/ycsb-shenango-raw{nochunnels} \
            --addr {server}:4242 \
            -i {interarrival} \
            --accesses {wrkfile} \
            --out-file={outf}0.data \
            -s shenango.config \
            {shards_arg} \
            {poisson_arg} \
            --skip-loads",
        wd="~/burrito",
        stdout=f"{outf}0.out",
        stderr=f"{outf}0.err",
        timeout=timeout,
        )
    check(ok, "client", conn.addr)
    conn.run("sudo pkill -INT iokerneld")
    agenda.subtask("client done")

def run_loads(conn, server, outf, wrkfile, no_chunnels=False):
    conn.run("sudo pkill -INT iokerneld")

    conn.run(f"./iokerneld ias nicpci {conn.pci_addr}", wd="~/burrito/shenango-chunnel/caladan", sudo=True, background=True)
    time.sleep(2)
    nochunnels = ''
    shards_arg = ''
    if no_chunnels:
        nochunnels = '-nochunnels'
        shards_arg = f'--num-shards={no_chunnels}'
    loads_start = time.time()
    agenda.subtask(f"loads client starting")
    ok = None
    try:
        ok = conn.run(f"RUST_LOG=info ./target/release/ycsb-shenango-raw{nochunnels} \
                --addr {server}:4242 \
                -i 1000 \
                --accesses {wrkfile} \
                -s shenango.config \
                {shards_arg} \
                --loads-only",
            wd="~/burrito",
            stdout=f"{outf}-loads.out",
            stderr=f"{outf}-loads.err",
            timeout=30,
            )
        print(ok)
    except Exception as e:
        print(e)
        agenda.subfailure(f"loads failed") #, retrying after {time.time() - loads_start} s")
    finally:
        conn.run("sudo pkill -INT iokerneld")
    if ok is None or ok.exited != 0:
        agenda.subfailure(f"loads failed, retrying after {time.time() - loads_start} s")
        sys.exit(1)
    else:
        agenda.subtask(f"loads client done: {time.time() - loads_start} s")

def do_exp(iter_num,
    outdir=None,
    machines=None,
    num_shards=None,
    ops_per_sec=None,
    poisson_arrivals=None,
    no_chunnels=None,
    wrkload=None,
    overwrite=None
):
    assert(
        outdir is not None and
        machines is not None and
        num_shards is not None and
        ops_per_sec is not None and
        poisson_arrivals is not None and
        no_chunnels is not None and
        wrkload is not None and
        overwrite is not None
    )

    wrkname = wrkload.split("/")[-1].split(".")[0]
    if no_chunnels:
        noch = '_nochunnels'
    else:
        noch = ''

    server_prefix = f"{outdir}/shenango_rt{noch}-{num_shards}-clientshard-{ops_per_sec}-poisson={poisson_arrivals}-{wrkname}-{iter_num}-kvserver"
    outf = f"{outdir}/shenango_rt{noch}-{num_shards}-clientshard-{ops_per_sec}-poisson={poisson_arrivals}-{wrkname}-{iter_num}-client"

    for m in machines:
        if m.local:
            m.run(f"mkdir -p {outdir}", wd="~/burrito")
            continue
        m.run(f"rm -rf {outdir}", wd="~/burrito")
        m.run(f"mkdir -p {outdir}", wd="~/burrito")

    if not overwrite and os.path.exists(f"{outf}0-{machines[1].addr}.data"):
        agenda.task(f"skipping: server = {machines[0].addr}, num_shards = {num_shards}, no_chunnels = {no_chunnels} load = {ops_per_sec} ops/s")
        return True
    else:
        agenda.task(f"running: {outf}0-{machines[1].addr}.data")

    # load = (n (client threads / proc) * 1 (procs/machine) * {len(machines) - 1} (machines))
    #        / {interarrival} (per client thread)
    num_client_threads = int(wrkname.split('-')[-1])
    interarrival_secs = num_client_threads * len(machines[1:]) / ops_per_sec
    interarrival_us = int(interarrival_secs * 1e6)

    time.sleep(5)
    server_addr = machines[0].addr
    agenda.task(f"starting: server = {machines[0].addr}, num_shards = {num_shards}, no_chunnels = {no_chunnels}, load = {ops_per_sec} ops/s -> interarrival_us = {interarrival_us}, num_clients = {len(machines)-1}")

    # first one is the server, start the server
    agenda.subtask("starting server")
    start_server(machines[0], server_prefix, shards=num_shards, no_chunnels=no_chunnels)
    time.sleep(5)
    # prime the server with loads
    agenda.task("doing loads")
    run_loads(machines[1], server_addr, outf, wrkload, no_chunnels=num_shards if no_chunnels else 0)
    try:
        machines[1].get(f"{outf}-loads.out", local=f"{outf}-loads.out", preserve_mode=False)
        machines[1].get(f"{outf}-loads.err", local=f"{outf}-loads.err", preserve_mode=False)
    except Exception as e:
        agenda.subfailure(f"Could not get file from loads client: {e}")

    # others are clients
    agenda.task("starting clients")
    clients = [threading.Thread(target=run_client, args=(
            m,
            server_addr,
            interarrival_us,
            poisson_arrivals,
            num_shards if no_chunnels else None,
            outf,
            wrkload,
        ),
    ) for m in machines[1:]]

    [t.start() for t in clients]
    [t.join() for t in clients]
    agenda.task("all clients returned")

    # kill the server
    machines[0].run("sudo pkill -INT kvserver")
    machines[0].run("sudo pkill -INT iokerneld")

    agenda.task("get server files")
    if not machines[0].local:
        machines[0].get(f"~/burrito/{server_prefix}.out", local=f"{server_prefix}.out", preserve_mode=False)
        machines[0].get(f"~/burrito/{server_prefix}.err", local=f"{server_prefix}.err", preserve_mode=False)

    def get_files(num):
        fn = c.get
        if c.is_local:
            agenda.subtask(f"Use get_local: {c.host}")
            fn = get_local

        agenda.subtask(f"getting {outf}{num}-{c.addr}.err")
        fn(
            f"burrito/{outf}{num}.err",
            local=f"{outf}{num}-{c.addr}.err",
            preserve_mode=False,
        )
        agenda.subtask(f"getting {outf}{num}-{c.addr}.out")
        fn(
            f"burrito/{outf}{num}.out",
            local=f"{outf}{num}-{c.addr}.out",
            preserve_mode=False,
        )
        agenda.subtask(f"getting {outf}{num}-{c.addr}.data")
        fn(
            f"burrito/{outf}{num}.data",
            local=f"{outf}{num}-{c.addr}.data",
            preserve_mode=False,
        )
        agenda.subtask(f"getting {outf}{num}-{c.addr}.trace")
        fn(
            f"burrito/{outf}{num}.trace",
            local=f"{outf}{num}-{c.addr}.trace",
            preserve_mode=False,
        )

    agenda.task("get client files")
    for c in machines[1:]:
        try:
            get_files(0)
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

        # caladan/build/config:
        # CONFIG_MLX5=y/n, CONFIG_MLX4=y/n
        if dpdk_driver == 'mlx5':
            ok = conn.run("grep -q \"CONFIG_MLX5=y\" build/config", wd = "~/burrito/shenango-chunnel/caladan")
            check(ok, "dpdk_driver was mlx5 but shenango not configured with mlx5 support", conn.addr)
        elif dpdk_driver == 'mlx4':
            ok = conn.run("grep -q \"CONFIG_MLX4=y\" build/config", wd = "~/burrito/shenango-chunnel/caladan")
            check(ok, "dpdk_driver was mlx4 but shenango not configured with mlx4 support", conn.addr)
        else:
            raise Exception(f"Unknown dpdk_driver for shenango {dpdk_driver}")

        agenda.subtask(f"[{conn.addr}] building shenango")
        # need to compile iokerneld
        ok = conn.run("make", wd = "~/burrito/shenango-chunnel/caladan")
        check(ok, "build shenango", conn.addr)

        ok = conn.run("./scripts/setup_machine.sh", wd = "~/burrito/shenango-chunnel/caladan")
        check(ok, "shenango setup-machine", conn.addr)

        agenda.subtask(f"building kvserver-shenango")
        ok = conn.run(f"~/.cargo/bin/cargo build --release", wd = "~/burrito/shenango-bertha")
        check(ok, "shenango-bertha build", conn.addr)

        return conn
    except Exception as e:
        agenda.failure(f"[{conn.addr}] setup_machine failed: {e}")
        global thread_ok
        thread_ok = False
        raise e

### Sample config
### [machines]
### server = { access = "127.0.0.1", alt = "192.168.1.2", exp = "10.1.1.2" }
### clients = [
###     { access = "192.168.1.5", exp = "10.1.1.5" },
###     { access = "192.168.1.6", exp = "10.1.1.6" },
###     { access = "192.168.1.7", exp = "10.1.1.7" },
###     { access = "192.168.1.8", exp = "10.1.1.8" },
###     { access = "192.168.1.9", exp = "10.1.1.9" },
### ]
###
### [exp]
### wrk = ["~/burrito/kvstore-ycsb/ycsbc-mock/wrkloadbunf1-4.access"]
### load = [10000, 20000, 40000, 60000, 80000, 100000]
### shards = [4]
###
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, required=True)
    parser.add_argument('--outdir', type=str, required=True)
    parser.add_argument('--overwrite', action='store_true')
    parser.add_argument('--dpdk_driver', type=str, choices=['mlx4', 'mlx5'],  required=False)
    parser.add_argument('--setup_only', action='store_true',  required=False)
    args = parser.parse_args()
    agenda.task(f"reading cfg {args.config}")
    cfg = toml.load(args.config)
    print(cfg)

    outdir = args.outdir

    if len(cfg['machines']['clients']) < 1:
        agenda.failure("Need more machines")
        sys.exit(1)

    ops_per_sec = cfg['exp']['load']
    if cfg['exp']['shards'] is None:
        agenda.failure("Need shards arg")
        sys.exit(1)

    for t in cfg['exp']['wrk']:
        if not t.endswith(".access") or '-' not in t:
            agenda.failure(f"Workload file should be <name>-<concurrency>.access, got {t}")
            sys.exit(1)

    if 'no-chunnels' not in cfg['exp']:
        agenda.subfailure('no-chunnels field not found')
        cfg['exp']['no-chunnels'] = [False]
    for t in cfg['exp']['no-chunnels']:
        if t not in [True, False]:
            agenda.failure("Disabling chunnels must be bool")
            sys.exit(1)

    # ban fancy features from this experiment
    if 'shardtype' in cfg['exp']:
        for t in cfg['exp']['shardtype']:
            if t != 'client':
                agenda.failure(f"Unsupported shardtype {t}")
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

    # build
    cfg['exp']['datapath'] = ['shenangort']
    setup_all(machines, cfg, args, setup_machine)
    if args.setup_only:
        agenda.task("setup done")
        sys.exit(0)

    # copy config file to outdir
    shutil.copy2(args.config, args.outdir)

    for noch in cfg['exp']['no-chunnels']:
        for w in cfg['exp']['wrk']:
            for s in cfg['exp']['shards']:
                for p in cfg['exp']['poisson-arrivals']:
                    for o in ops_per_sec:
                        do_exp(0,
                                outdir=outdir,
                                machines=machines,
                                num_shards=s,
                                ops_per_sec=o,
                                poisson_arrivals=p,
                                wrkload=w,
                                no_chunnels=noch,
                                overwrite=args.overwrite
                                )

    agenda.task("done")
