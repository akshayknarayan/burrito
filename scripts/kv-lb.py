#!/usr/bin/python3

import os
import threading
import argparse
import sys
import time
import subprocess
import agenda
from kv import ConnectionWrapper, check_machine, setup_machine, start_redis, write_shenango_config, run_loads, run_client, get_local

def start_shard(conn, outf_prefix):
    conn.run("sudo pkill -9 single-shard")
    conn.run("sudo pkill -9 iokerneld")
    time.sleep(2)
    write_shenango_config(conn)
    conn.run("./iokerneld", wd="~/burrito/shenango-chunnel/caladan", sudo=True, background=True)
    time.sleep(2)

    outf = f"{outf_prefix}-{conn.addr}"
    conn.run(f"RUST_LOG=info,bertha=debug,kvstore=debug ./target/release/single-shard \
            --addr 0.0.0.0:4242 \
            --internal-addr 0.0.0.0:4342 \
            -s host.config --log",
            wd="~/burrito",
            sudo=True,
            background=True,
            stdout=f"{outf}.out",
            stderr=f"{outf}.err",
            )
    time.sleep(2)
    conn.check_proc(f"single-shard", f"{outf}.err")

def start_lb(conn, redis_addr, shard_addrs, outf):
    conn.run("sudo pkill -9 burrito-lb")
    conn.run("sudo pkill -9 iokerneld")
    time.sleep(2)
    write_shenango_config(conn)
    conn.run("./iokerneld", wd="~/burrito/shenango-chunnel/caladan", sudo=True, background=True)
    time.sleep(2)

    shards_args = ' '.join(f'--shards={s}:4242' for s in shard_addrs)
    conn.run(f"RUST_LOG=info,bertha=debug,kvstore=debug ./target/release/burrito-lb \
            --addr 0.0.0.0:4242 \
            {shards_args} \
            --redis-addr {redis_addr} \
            -s host.config --log",
            wd="~/burrito",
            sudo=True,
            background=True,
            stdout=f"{outf}.out",
            stderr=f"{outf}.err",
            )
    time.sleep(2)
    conn.check_proc(f"burrito-lb", f"{outf}.err")

def do_exp(outdir, lb, shards, clients, shardtype, ops_per_sec):
    num_shards = len(shards)
    server_prefix = f"{outdir}/{shardtype}shard-{ops_per_sec}-lb"
    shard_prefix = f"{outdir}/{shardtype}shard-{ops_per_sec}-shard"
    outf = f"{outdir}/{shardtype}shard-{ops_per_sec}-client"

    agenda.task(f"checking {outf}0-{clients[0].addr}.data")
    if os.path.exists(f"{outf}0-{clients[0].addr}.data"):
        agenda.task(f"skipping: server = {lb.addr}, shardtype = {shardtype}, load = {ops_per_sec} ops/s")
        return True
    else:
        agenda.task(f"running: server = {lb.addr}, shardtype = {shardtype}, load = {ops_per_sec} ops/s")

    # load = (4 (client threads / proc) * 1 (procs/machine) * {len(machines) - 1} (machines))
    #        / {interarrival} (per client thread)
    interarrival_secs = 4 * len(clients) / ops_per_sec
    interarrival_us = int(interarrival_secs * 1e6)

    redis_addr = start_redis(lb)
    time.sleep(5)
    server_addr = lb.addr
    agenda.task(f"starting: server = {server_addr}, shardtype = {shardtype}, load = {ops_per_sec}, ops/s -> interarrival_us = {interarrival_us}, num_clients = {len(clients)}")

    agenda.subtask("starting shards")
    for s in shards:
        start_shard(s, shard_prefix)
    time.sleep(5)

    agenda.subtask("starting lb")
    redis_port = redis_addr.split(":")[-1]
    start_lb(lb, f"127.0.0.1:{redis_port}", [s.addr for s in shards], server_prefix)
    time.sleep(5)

    # prime the server with loads
    # conn, server, redis_addr, outf, wrkload='uniform'
    agenda.task("doing loads")
    run_loads(clients[0], server_addr, redis_addr, outf)
    # others are clients
    agenda.task("starting clients")
    client_threads = [threading.Thread(target=run_client, args=(
            m,
            server_addr,
            redis_addr,
            interarrival_us,
            shardtype,
            outf
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
        agenda.subtask(f"getting {outf}{num}-{c.addr}.data1")
        fn(
            f"burrito/{outf}{num}.data1",
            local=f"{outf}{num}-{c.addr}.data1",
            preserve_mode=False,
        )

    agenda.task("get client files")
    ok = True
    for c in clients:
        try:
            get_files(0)
        except Exception as e:
            agenda.subfailure(f"At least one file missing for {c}: {e}")
            ok = False
    if not ok:
        return ok

    def awk_files(num):
        subprocess.run(f"awk '{{if (!hdr) {{hdr=$1; print \"ShardType NumShards Ops \"$0;}} else {{print \"{shardtype} {num_shards} {ops_per_sec} \"$0}} }}' {outf}{num}-{c.addr}.data1 > {outf}{num}-{c.addr}.data", shell=True, check=True)

    for c in clients:
        agenda.subtask(f"adding experiment info for {c.addr}")
        try:
            awk_files(0)
        except:
            agenda.subfailure(f"At least one file missing")
            return False

    agenda.task("done")
    return True

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--outdir', type=str, required=True)
    parser.add_argument('--lb', type=str, required=True)
    parser.add_argument('--shard', type=str, action='append', required=True)
    parser.add_argument('--client', type=str, action='append', required=True)
    parser.add_argument('--load', type=int, action='append', required=True)
    parser.add_argument('--shardtype', type=str, action='append')
    args = parser.parse_args()

    outdir = args.outdir
    if len(args.shard) < 2:
        agenda.failure("need at least 2 shards")
        sys.exit(1)

    if args.shardtype is None:
        args.shardtype = ['server']
    for t in args.shardtype:
        if t not in ['client', 'server']:
            agenda.failure(f"Unknown shardtype {t}")
            sys.exit(1)

    agenda.task(f"connecting to lb machine")
    lb_conn, lb_commit = check_machine(args.lb.split(':'))
    agenda.task(f"connecting to shard machines")
    shard_conns, shard_commits = zip(*[check_machine(i.split(':')) for i in args.shard])
    shard_conns = list(shard_conns)
    agenda.task(f"connecting to client machines")
    client_conns, client_commits = zip(*[check_machine(i.split(':')) for i in args.client])
    client_conns = list(client_conns)

    commits = shard_commits + client_commits
    if not all(c == lb_commit for c in commits):
        agenda.failure(f"not all commits equal: {lb_commit}, {shard_commits} {client_commits}")
        sys.exit(1)

    if lb_conn.host in ['127.0.0.1', '::1', 'localhost']:
        agenda.subtask(f"Local conn: {lb_conn.host}/{lb_conn.addr}")
        lb_conn.local = True
    else:
        lb_conn.local = False

    for m in shard_conns:
        if m.host in ['127.0.0.1', '::1', 'localhost']:
            agenda.subtask(f"Local conn: {m.host}/{m.addr}")
            m.local = True
        else:
            m.local = False
    for m in client_conns:
        if m.host in ['127.0.0.1', '::1', 'localhost']:
            agenda.subtask(f"Local conn: {m.host}/{m.addr}")
            m.local = True
        else:
            m.local = False

    agenda.task("building burrito...")
    thread_ok = True
    setups = [threading.Thread(target=setup_machine, args=(m,outdir)) for m in [lb_conn] + shard_conns + client_conns]
    [t.start() for t in setups]
    [t.join() for t in setups]
    if not thread_ok:
        agenda.failure("Something went wrong")
        sys.exit(1)
    agenda.task("...done building burrito")

    for t in args.shardtype:
        for o in args.load:
            do_exp(outdir, lb_conn, shard_conns, client_conns, t, o)

    agenda.task("done")
