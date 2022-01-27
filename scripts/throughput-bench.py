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
    #agenda.subtask(f"building burrito on {conn.addr}")
    #ok = conn.run("../../.cargo/bin/cargo b --release", wd = "~/burrito/throughput-bench")
    #check(ok, "build", conn.addr)
    return conn

dpdk_ld_var = "LD_LIBRARY_PATH=/usr/local/lib64:/usr/local/lib:dpdk-direct/dpdk-wrapper/dpdk/install/lib/x86_64-linux-gnu"

def start_server(conn, outf, variant='dpdk', use_bertha=False):
    conn.run("sudo pkill -INT throughput")
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

    no_bertha = '--no-bertha' if not use_bertha else ''
    time.sleep(2)
    ok = conn.run(f"RUST_LOG=info {dpdk_ld_var} ./target/release/throughput-bench -p 4242 --datapath {variant} {no_bertha} --cfg host.config server",
            wd="~/burrito",
            sudo=True,
            background=True,
            stdout=f"{outf}.out",
            stderr=f"{outf}.err",
            )
    check(ok, "spawn server", conn.addr)
    agenda.subtask("wait for server check")
    time.sleep(8)
    conn.check_proc(f"throughput", f"{outf}.err")

def run_client(conn, server, num_clients, file_size, variant, use_bertha, outf):
    conn.run("sudo pkill -INT throughput")
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

    no_bertha = '--no-bertha' if not use_bertha else ''
    time.sleep(2)
    agenda.subtask(f"client starting -> {outf}.out")
    ok = conn.run(
        f"RUST_LOG=info {dpdk_ld_var}  ./target/release/throughput-bench \
        -p 4242 \
        --datapath {variant} \
        --cfg host.config \
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

    if not overwrite and os.path.exists(f"{outf}.data"):
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

    for m in machines:
        m.run("rm ~/burrito/*.config")

    #agenda.task("get server files")
    #if not machines[0].local:
    #    machines[0].get(f"~/burrito/{server_prefix}.out", local=f"{server_prefix}.out", preserve_mode=False)
    #    machines[0].get(f"~/burrito/{server_prefix}.err", local=f"{server_prefix}.err", preserve_mode=False)

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

    agenda.task("get client files")
    for c in machines[1:]:
        try:
            get_files(0)
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
### num_clients = [1, 2, 4, 8, 16, 32, 64, 128]
### file_size = [50000000]
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
    agenda.task("building burrito...")
    thread_ok = True
    setups = [threading.Thread(target=setup_machine, args=(m,outdir)) for m in machines]
    [t.start() for t in setups]
    [t.join() for t in setups]
    if not thread_ok:
        agenda.failure("Something went wrong")
        sys.exit(1)
    agenda.task("...done building burrito")

    # copy config file to outdir
    shutil.copy2(args.config, args.outdir)

    for d in cfg['exp']['datapath']:
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
