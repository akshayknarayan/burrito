#!/usr/bin/python3

import os
import toml
import threading
import argparse
import sys
import time
import subprocess
import agenda
from kv import ConnectionWrapper, check_machine, setup_machine, start_redis, write_shenango_config, run_loads, run_client, get_local, dpdk_ld_var, setup_all, generate_ycsb_file, check

SRV_BASE_PORT = 4242

def start_shard(conn, shard_ports, outf, datapath='kernel', skip_negotiation=False):
    conn.run("sudo pkill -9 single-shard")
    conn.run("sudo pkill -9 iokerneld")
    if datapath == 'shenango_channel':
        conn.run(f"./iokerneld ias nicpci {conn.pci_addr}", wd="~/burrito/shenango-chunnel/caladan", sudo=True, background=True)
        datapath = 'shenango'
        cfg = '--cfg=shenango.config'
    elif datapath == 'kernel':
        cfg = ''
    elif 'dpdk' in datapath:
        cfg = '--cfg=dpdk.config'
    else:
        raise Exception(f"unknown datapath {datapath}")

    skip_neg = '--skip-negotiation' if skip_negotiation else ''
    addrs = ' '.join(f"--addr=0.0.0.0:{p}" for p in shard_ports)

    ok = conn.run(f"RUST_LOG=info {dpdk_ld_var} ./target/release/single-shard \
            --datapath={datapath} \
            --log \
            {addrs} \
            {cfg} \
            {skip_neg}",
            wd="~/burrito",
            sudo=True,
            background=True,
            stdout=f"{outf}.out",
            stderr=f"{outf}.err",
            )
    check(ok, "spawn shard", conn.addr)
    agenda.subtask("wait for single-shard check")
    time.sleep(8)
    conn.check_proc(f"single-shard", [f"{outf}.out", f"{outf}.err"])

def start_lb(conn, redis_addr, shard_addrs, threads, outf, datapath='kernel', skip_negotiation=False):
    conn.run("sudo pkill -9 burrito-lb")
    conn.run("sudo pkill -9 iokerneld")
    if datapath == 'shenango_channel':
        conn.run(f"./iokerneld ias nicpci {conn.pci_addr}", wd="~/burrito/shenango-chunnel/caladan", sudo=True, background=True)
        datapath = 'shenango'
        cfg = '--cfg=shenango.config'
    elif datapath == 'kernel':
        cfg = ''
    elif 'dpdk' in datapath:
        cfg = '--cfg=dpdk.config'
    else:
        raise Exception(f"unknown datapath {datapath}")

    skip_neg = '--skip-negotiation' if skip_negotiation else ''

    time.sleep(2)
    ok = conn.run(f"RUST_LOG=info {dpdk_ld_var} ./target/release/burrito-lb \
            --addr {conn.addr}:{SRV_BASE_PORT} \
            --redis-addr {redis_addr} \
            {shard_addrs} \
            --num-threads={threads} \
            --log \
            --datapath={datapath} \
            {cfg} \
            {skip_neg}",
            wd="~/burrito",
            sudo=True,
            background=True,
            stdout=f"{outf}.out",
            stderr=f"{outf}.err",
            )
    check(ok, "spawn burrito-lb", conn.addr)
    agenda.subtask("wait for burrito-lb check")
    time.sleep(8)
    conn.check_proc(f"burrito-lb", [f"{outf}.out", f"{outf}.err"])

def do_exp(
    outdir=None,
    conns=None,
    num_shards=None,
    ops_per_sec=None,
    datapath=None,
    poisson_arrivals=None,
    wrkload=None,
    overwrite=None,
    skip_negotiation=None,
    cfg_client=None,
    cfg_server=None,
):
    assert(
        outdir is not None and
        conns is not None and
        num_shards is not None and
        ops_per_sec is not None and
        datapath is not None and
        poisson_arrivals is not None and
        wrkload is not None and
        overwrite is not None and
        skip_negotiation is not None
    )
    wrkname = wrkload.split("/")[-1].split(".")[0]
    noneg = '_noneg' if skip_negotiation else ''

    lb = conns['lb']
    shards = conns['shards']
    clients = conns['clients']

    exp_pfx = f"{outdir}/{datapath}{noneg}-{num_shards}-remoteshard-load={ops_per_sec}-poisson={poisson_arrivals}-{wrkname}"
    server_prefix = f"{exp_pfx}-lb"
    shard_prefix = f"{exp_pfx}-shard"
    outf = f"{exp_pfx}-client"

    agenda.task(f"checking {outf}0-{clients[0].addr}.data")
    if not overwrite and os.path.exists(f"{outf}-{clients[0].addr}.data"):
        agenda.task(f"skipping: load = {ops_per_sec} ops/s")
        return True
    else:
        agenda.task(f"running: load = {ops_per_sec} ops/s")

    for m in machines:
        if m.is_local:
            m.run(f"mkdir -p {outdir}", wd="~/burrito")
            continue
        m.run(f"rm -rf {outdir}", wd="~/burrito")
        m.run(f"mkdir -p {outdir}", wd="~/burrito")

    redis_addr = start_redis(lb)
    time.sleep(5)
    # load = (n (client threads / proc) * 1 (procs/machine) * {len(machines) - 1} (machines))
    #        / {interarrival} (per client thread)
    num_client_threads = int(wrkname.split('-')[-1])
    interarrival_secs = num_client_threads * len(clients) / ops_per_sec
    interarrival_us = int(interarrival_secs * 1e6)
    agenda.task(f"starting: load = {ops_per_sec}, ops/s -> interarrival_us = {interarrival_us}, num_clients = {len(clients)}, num_shards = {num_shards}, skip_negotiation = {skip_negotiation}")

    shards_per_machine = int(num_shards / len(shards))
    if shards_per_machine > cfg_server['num-threads']:
        raise Exception(f"trying to start more shards per thread ({shards_per_machine}) than number of threads ({cfg_server['num_threads']})")

    threads = cfg_server['num-threads']

    curr_shard_port = SRV_BASE_PORT + 1
    shard_addrs = []
    agenda.subtask("starting shards")
    for s in shards:
        shard_ports = list(range(curr_shard_port, curr_shard_port+shards_per_machine))
        curr_shard_port += shards_per_machine
        shard_addrs += [f"{s.addr}:{p}" for p in shard_ports]
        start_shard(
            s,
            shard_ports,
            shard_prefix,
            datapath=datapath,
            skip_negotiation=skip_negotiation
        )
    time.sleep(5)

    agenda.subtask("starting lb")
    redis_port = redis_addr.split(":")[-1]
    start_lb(
        lb,
        f"127.0.0.1:{redis_port}",
        " ".join(f"--shards {a}" for a in shard_addrs),
        threads,
        server_prefix,
        datapath=datapath,
        skip_negotiation=skip_negotiation
    )
    time.sleep(5)

    # prime the server with loads
    server_addr = lb.addr
    # conn, server, redis_addr, outf, wrkload='uniform'
    agenda.task("doing loads")
    run_loads(
        clients[0],
        cfg_client,
        server_addr,
        datapath,
        redis_addr,
        outf,
        wrkload,
        skip_negotiation=num_shards if skip_negotiation else 0
    )

    # others are clients
    agenda.task("starting clients")
    clients = [threading.Thread(target=run_client, args=(
            m,
            cfg_client,
            server_addr,
            redis_addr,
            interarrival_us,
            poisson_arrivals,
            datapath,
            'remote',
            num_shards if skip_negotiation else 0,
            outf,
            wrkload
        ),
    ) for m in clients]

    [t.start() for t in client_threads]
    [t.join() for t in client_threads]
    agenda.task("all clients returned")

    # kill the server
    lb.run("sudo pkill -9 burrito-lb")
    lb.run("sudo pkill -9 iokerneld")
    for s in shards:
        s.run("sudo pkill -9 single-shard")
        s.run("sudo pkill -9 iokerneld")

    lb.run("rm ~/burrito/*.config")
    for m in shards:
        m.run("rm ~/burrito/*.config")
    for m in clients:
        m.run("rm ~/burrito/*.config")

    agenda.task("get lb files")
    if not lb.local:
        lb.get(f"burrito/{server_prefix}.out", local=f"{server_prefix}.out", preserve_mode=False)
        lb.get(f"burrito/{server_prefix}.err", local=f"{server_prefix}.err", preserve_mode=False)
    agenda.task("get shard files")
    for s in shards:
        if not s.local:
            s.get(f"burrito/{shard_prefix}-{s.addr}.out", local=f"{shard_prefix}-{s.addr}.out", preserve_mode=False)
            s.get(f"burrito/{shard_prefix}-{s.addr}.err", local=f"{shard_prefix}-{s.addr}.err", preserve_mode=False)
            #s.get(f"burrito/{shard_prefix}-{s.addr}.trace", local=f"{shard_prefix}-{s.addr}.trace", preserve_mode=False)

    def get_files(num):
        fn = c.get
        if c.local:
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
        #agenda.subtask(f"getting {outf}{num}-{c.addr}.trace")
        #fn(
        #    f"burrito/{outf}{num}.trace",
        #    local=f"{outf}{num}-{c.addr}.trace",
        #    preserve_mode=False,
        #)

    agenda.task("get client files")
    for c in clients:
        try:
            get_files(0)
        except Exception as e:
            agenda.subfailure(f"At least one file missing for {c}: {e}")

    agenda.task("done")
    return True

def connect_machines(cfg):
    agenda.task(f"Checking for connection vs experiment ip")
    ips = cfg['machines']['server'] + cfg['machines']['clients']
    agenda.task(f"connecting to {ips}")
    machines, commits = zip(*[check_machine(ip) for ip in ips])
    # check all the commits are equal
    if not all(c == commits[0] for c in commits):
        agenda.subfailure(f"not all commits equal: {commits}")
        raise Exception("Commits mismatched on machines")
    lb_ip = cfg['machines']['server'][0]['exp']
    shard_ips = [x['exp'] for x in cfg['machines']['server'][1:]]
    client_ips = [x['exp'] for x in cfg['machines']['clients']]
    conns = { c.addr: c for c in machines }
    return { 'lb': conns[lb_ip], 'shards': [conns[i] for i in shard_ips], 'clients': [conns[i] for i in client_ips] }

def setup_machine_lb(conn, outdir, datapaths, dpdk_driver):
    setup_machine(conn, outdir, datapaths, dpdk_driver)
    try:
        needed_features = ["bin"]
        for d in datapaths:
            if 'shenango_channel' == d:
                if 'shenango-chunnel' not in needed_features:
                    needed_features.append('shenango-chunnel')
            elif 'dpdk' in d:
                if 'dpdk-direct' not in needed_features:
                    needed_features.append('dpdk-direct')
                if dpdk_driver == 'mlx4' and 'cx3_mlx' not in needed_features:
                    needed_features.append('cx3_mlx')
                if dpdk_driver == 'mlx5' and 'cx4_mlx' not in needed_features:
                    needed_features.append('cx4_mlx')
                elif dpdk_driver == 'intel' and 'xl710_intel' not in needed_features:
                    needed_features.append('xl710_intel')
        agenda.subtask(f"building kvserver features={needed_features}")
        ok = conn.run(f"~/.cargo/bin/cargo build --release --features=\"{','.join(needed_features)}\" --bin=\"single-shard\" --bin=\"burrito-lb\"", wd = "~/burrito/kvstore")
        check(ok, "single-shard build", conn.addr)
    except Exception as e:
        msg = f"[{conn.addr}] setup_machine failed: {e}"
        agenda.failure(msg)
        global thread_ok
        thread_ok = False
        raise Exception(msg) from e

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, required=True)
    parser.add_argument('--outdir', type=str, required=True)
    parser.add_argument('--overwrite', action='store_true')
    parser.add_argument('--dpdk_driver', type=str, choices=['mlx4', 'mlx5', 'intel'],  required=False)
    parser.add_argument('--setup_only', action='store_true',  required=False)
    args = parser.parse_args()
    agenda.task(f"reading cfg {args.config}")
    cfg = toml.load(args.config)
    print(cfg)

    outdir = args.outdir

    if len(cfg['machines']['clients']) < 1:
        agenda.failure("Need a client machine")
        sys.exit(1)

    if len(cfg['machines']['server']) < 2:
        agenda.failure("Need at least 2 server machines (one shard server and one sharder)")
        sys.exit(1)

    try:
        for o in cfg['exp']['load']:
            int(o)
    except Exception as e:
        raise Exception("load key must be present and [int]") from e

    try:
        for s in cfg['exp']['shards']:
            int(s)
    except Exception as e:
        raise Exception("shards key must be present and [int]") from e

    try:
        for t in cfg['exp']['wrk']:
            if not t.endswith(".access") or '-' not in t:
                raise Exception(f"Workload file should be <name>-<concurrency>.access, got {t}")
    except Exception as e:
        raise Exception("wrk key must be present and point to workload files") from e

    try:
        for t in cfg['exp']['datapath']:
            if t not in ['shenango_channel', 'kernel', 'dpdkthread', 'dpdkmulti']:
                raise Exception(f"unknown datapath {t}")
    except Exception as e:
        raise Exception("datapath key must be present and known") from e
    if any('dpdk' in d for d in cfg['exp']['datapath']):
        if args.dpdk_driver not in ['mlx4', 'mlx5', 'intel']:
            raise Exception("If using dpdk datapath, must specify mlx4, mlx5, or intel driver.")

    try:
        for t in cfg['exp']['negotiation']:
            if t not in [True,False]:
                raise Exception("Skip-negotiation must be bool")
    except Exception as e:
        raise Exception("negotiation key must be present and bool") from e

    conns = connect_machines(cfg)
    machines = [conns['lb']] + conns['shards'] + conns['clients']

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
    setup_all(machines, cfg, args, setup_machine_lb)

    for w in cfg['exp']['wrk']:
        for conn in machines:
            ok = conn.run(f"ls {w}")
            if ok.exited != 0:
                generate_ycsb_file(conn, w)

    if args.setup_only:
        agenda.task("setup done")
        sys.exit(0)

    for dp in cfg['exp']['datapath']:
        if 'intel' == args.dpdk_driver:
            intel_devbind(machines, dp)
        for neg in cfg['exp']['negotiation']:
            for w in cfg['exp']['wrk']:
                for s in cfg['exp']['shards']:
                    for p in cfg['exp']['poisson-arrivals']:
                        for o in cfg['exp']['load']:
                            do_exp(
                                outdir=outdir,
                                conns=conns,
                                num_shards=s,
                                ops_per_sec=o,
                                datapath=dp,
                                poisson_arrivals=p,
                                wrkload=w,
                                skip_negotiation=not neg,
                                overwrite=args.overwrite,
                                cfg_client=cfg['cfg']['client'],
                                cfg_server=cfg['cfg']['server'],
                            )

    agenda.task("done")
