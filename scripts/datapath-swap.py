#!/usr/bin/python3

import agenda
import subprocess
import os
import time
import argparse
import sys
import toml
from kv import connect_machines, intel_devbind, setup_all, dpdk_ld_var, local_path, check

def run_client(m, server_addr, num_msgs, arrival_pattern, cfg_client, outfile_pfx, bin_root_dir):
    agenda.subtask("running client")
    m.run("sudo pkill -INT switch-datapath")
    use_locks_arg = "--use-locks" if use_locks else ""
    arrivals_arg = ""
    if arrival_pattern[0] == 'closed':
        arrivals_arg = "--closed-loop"
    elif arrival_pattern[0] == 'open':
        arrivals_arg = f"--interarrival-us={arrival_pattern[1]}"
    else:
        raise Exception(f"unknown arrival pattern {arrival_pattern}")
    ok = m.run(f"RUST_LOG=info {dpdk_ld_var} \
        {bin_root_dir}/examples/switch_datapath \
        --port=4242 \
        --ip-addr={server_addr} \
        --threads={cfg_client['num-threads']} \
        --config=dpdk.config \
        --datapath-choice=inline:{cfg_client['num-threads']} \
        --swap-to=inline:{cfg_client['num-threads']} \
        --swap-delay-msgs=999999 \
        --num-msgs=10000 \
        --out-file={outf}.data \
        {use_locks_arg}",
        wd="~/burrito",
        sudo=True,
        stdout=f"{outf}-client.out",
        stderr=f"{outf}-client.err")
    check(ok, "client", m.addr)
    agenda.subtask("client done")

def start_server(m, use_locks, switch_point, cfg_server, outf, bin_root_dir):
    agenda.subtask(f"starting server: {outf}")
    m.run("sudo pkill -INT switch-datapath")
    use_locks_arg = "--use-locks" if use_locks else ""
    ok = m.run(f"RUST_LOG=info {dpdk_ld_var} \
        {bin_root_dir}/examples/switch_datapath \
        --port=4242 \
        --threads={cfg_server['num-threads']} \
        --config=dpdk.config \
        --datapath-choice=inline:{cfg_server['num-threads']} \
        --swap-to=thread \
        --swap-delay-msgs={switch_point} \
        {use_locks_arg}",
        wd="~/burrito",
        sudo=True,
        background=True,
        stdout=f"{outf}-server.out",
        stderr=f"{outf}-server.err")
    check(ok, "spawn server", m.addr)
    agenda.subtask("wait for server check")
    time.sleep(5)
    m.check_proc(f"switch-datapath", [f"{outf}-server.err", f"{outf}-server.out"])
    agenda.subtask("server started")

def do_exp(
    machines=[],
    outdir=None,
    use_locks=False,
    arrival_pattern='open',
    switch_point=None,
    num_msgs=None,
    overwrite=False,
    cfg_client={},
    cfg_server={},
    cloudlab=False,
):
    assert(len(machines) >= 2)
    assert(outdir != None)
    assert(switch_point != None)
    assert(num_msgs != None)

    target_dir = None
    orig_outdir = outdir
    if cloudlab:
        res = machines[0].run("ls /proj/")
        target_dir = f"/proj/{res.stdout.strip().split()[0]}"
        outdir = f"{target_dir}/{outdir}"

    exp_pfx = f"{outdir}/switching-locks={use_locks}-msgs={num_msgs}-switchpoint={switch_point}-arrival={arrival_pattern[0]}:{arrival_pattern[1]}"
    data_file = f"{exp_pfx}.data"

    for m in machines:
        if m.is_local:
            m.run(f"mkdir -p {outdir}", wd="~/burrito")
            continue
        m.run(f"rm -rf {outdir}", wd="~/burrito")
        m.run(f"mkdir -p {outdir}", wd="~/burrito")

    if not overwrite and os.path.exists(data_file):
        agenda.task(f"skipping: {exp_pfx}")
        return True
    else:
        agenda.task(f"running: {exp_pfx}")

    server_addr = machines[0].addr
    bin_root_dir = f"{target_dir}/burrito-target/release" if target_dir is not None else "./target/release"
    start_server(machines[0], use_locks, switch_point, cfg_server, exp_pfx, bin_root_dir)

    run_client(machines[1], server_addr, num_msgs, arrival_pattern, cfg_client, exp_pfx, bin_root_dir)

    # kill the server processes
    machines[0].run("sudo pkill -INT switch-datapath")

    agenda.task("get server files")
    if not machines[0].is_local:
        loc = local_path(f"{exp_pfx}-server", orig_outdir) if cloudlab else f"{exp_pfx}-server"
        machines[0].get(f'{exp_pfx}-server.out', local=loc +'.out', preserve_mode=False)
        machines[0].get(f'{exp_pfx}-server.err', local=loc +'.err', preserve_mode=False)

    agenda.task("get client files")
    if not machines[1].is_local:
        loc = local_path(f"{exp_pfx}-client", orig_outdir) if cloudlab else f"{exp_pfx}-server"
        machines[1].get(f'{exp_pfx}.data', local=loc +'.data', preserve_mode=False)
        machines[1].get(f'{exp_pfx}-client.out', local=loc +'-client.out', preserve_mode=False)
        machines[1].get(f'{exp_pfx}-client.err', local=loc +'-client.err', preserve_mode=False)
    agenda.task("exp done")
    return True


def setup_machine(conn, outdir, datapaths, dpdk_driver, target_dir):
    agenda.task(f"[{conn.addr}] setup machine")
    try:
        agenda.subtask(f"[{conn.addr}] make outdir {outdir}")
        ok = conn.run(f"mkdir -p {target_dir if target_dir is not None else '~/burrito'}/{outdir}")
        check(ok, "mk outdir", conn.addr)

        # dpdk dependencies, rust, and docker
        ok = conn.run("bash scripts/install-kv-deps.sh", wd="~/burrito")
        check(ok, "install dependencies", conn.addr)

        ok = conn.run("./usertools/dpdk-hugepages.py -p 2M --setup 10G", wd = "~/burrito/dpdk-direct/dpdk-wrapper/dpdk", sudo=True)
        check(ok, "reserve hugepages", conn.addr)
    except Exception as e:
        agenda.failure(f"[{conn.addr}] setup_machine failed: {e}")
        global thread_ok
        thread_ok = False
        raise e

def compile_binaries(conn, outdir, datapaths, dpdk_driver, target_dir):
    try:
        needed_features = ['switcher_lock']
        if dpdk_driver == 'mlx4' and 'cx3_mlx' not in needed_features:
            needed_features.append('cx3_mlx')
        if dpdk_driver == 'mlx5' and 'cx4_mlx' not in needed_features:
            needed_features.append('cx4_mlx')
        elif dpdk_driver == 'intel' and 'xl710_intel' not in needed_features:
            needed_features.append('xl710_intel')

        agenda.subtask(f"building switch-datapath features={needed_features}, target-dir {target_dir}")
        target_dir_arg = f"--target-dir {target_dir}/burrito-target" if target_dir is not None else ""
        ok = conn.run(f"~/.cargo/bin/cargo build --examples --release --features=\"{','.join(needed_features)}\" {target_dir_arg}", wd = "~/burrito/dpdk-direct")
        check(ok, "switch-datapath build", conn.addr)
    except Exception as e:
        agenda.failure(f"[{conn.addr}] compile_binaries failed: {e}")
        global thread_ok
        thread_ok = False
        raise e

def main(cfg):
    agenda.task(f"using cfg {cfg}")
    outdir = args.outdir

    if len(cfg['machines']['clients']) < 1:
        agenda.error(f"cfg[machines][clients] = {cfg['machines']['clients']}")
        raise Exception("Need a client machine")

    if len(cfg['machines']['server']) < 1:
        agenda.error(f"cfg[machines][server] = {cfg['machines']['server']}")
        raise Exception("Need at least 1 server machine ")

    uselocks = cfg['exp']['use-locks']
    for u in uselocks:
        if u not in [True,False]:
            raise Exception("use-locks requires boolean values")

    arrivals = []
    for a in cfg['exp']['arrivals']:
        if a != 'closed':
            sp = a.split(':')
            if len(sp) != 2 or sp[0] != 'open':
                raise Exception("arrivals should be in format 'closed|open:<x>' (x in us)")
            arrivals.append((sp[0], int(sp[1])))
        else:
            arrivals.append((a, 0))

    switchpoints = []
    for p in cfg['exp']['switchpoint']:
        switchpoints.append(int(p))

    num_msgs = int(cfg['exp']['num-msgs'])

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

    # fake this
    cfg['exp']['datapath'] = ['dpdkinline']

    # build
    setup_all(machines, cfg, args, setup_machine, compile_binaries)

    if args.setup_only:
        agenda.task("setup done")
        return

    # we are definitely using the dpdk datapaths, so we can just bind once.
    if 'intel' == args.dpdk_driver:
        intel_devbind(machines, 'dpdkinline')

    for l in uselocks:
        for a in arrivals:
            for sp in switchpoints:
                do_exp(
                    machines=machines,
                    outdir=outdir,
                    use_locks=l,
                    arrival_pattern=a,
                    switch_point=sp,
                    num_msgs=num_msgs,
                    overwrite=args.overwrite,
                    cfg_client=cfg['cfg']['client'],
                    cfg_server=cfg['cfg']['server'],
                    cloudlab=args.cloudlab,
                )

    agenda.task("all experiments done")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, required=True)
    parser.add_argument('--outdir', type=str, required=True)
    parser.add_argument('--overwrite', action='store_true')
    parser.add_argument('--dpdk_driver', type=str, choices=['mlx4', 'mlx5', 'intel'],  required=False)
    parser.add_argument('--setup_only', action='store_true',  required=False)
    parser.add_argument('--cloudlab', action='store_true',  required=False)
    args = parser.parse_args()
    agenda.task(f"reading cfg {args.config}")
    cfg = toml.load(args.config)
    main(cfg)
