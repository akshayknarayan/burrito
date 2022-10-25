#!/usr/bin/python3

from fabric import Connection
import agenda
import argparse
import os
import shutil
import subprocess
import sys
import threading
import time
import toml

class ConnectionWrapper(Connection):
    def __init__(self, addr, user=None, port=None):
        super().__init__(
            addr,
            forward_agent=True,
            user=user,
            port=port,
        )
        self.addr = addr
        self.conn_addr = addr

        # Start the ssh connection
        super().open()

    """
    Run a command on the remote machine

    verbose    : if true, print the command before running it, and any output it produces
                 (if not redirected)
                 if false, capture anything produced in stdout and save in result (res.stdout)
    background : if true, start the process in the background via nohup.
                 if output is not directed to a file or pty=True, this won't work
    stdin      : string of filename for stdin (default /dev/stdin as expected)
    stdout     : ""
    stderr     : ""
    ignore_out : shortcut to set stdout and stderr to /dev/null
    wd         : cd into this directory before running the given command
    sudo       : if true, execute this command with sudo (done AFTER changing to wd)

    returns result struct
        .exited = return code
        .stdout = stdout string (if not redirected to a file)
        .stderr = stderr string (if not redirected to a file)
    """
    def run(self, cmd, *args, stdin=None, stdout=None, stderr=None, ignore_out=False, wd=None, sudo=False, background=False, quiet=False, pty=True, **kwargs):
        self.verbose = True
        # Prepare command string
        pre = ""
        if wd:
            pre += f"cd {wd} && "
        if background:
            pre += "screen -d -m "
        #escape the strings
        cmd = cmd.replace("\"", "\\\"")
        if sudo:
            pre += "sudo "
        pre += "bash -c \""
        if ignore_out:
            stdin="/dev/null"
            stdout="/dev/null"
            stderr="/dev/null"
        if background:
            stdin="/dev/null"

        full_cmd = f"{pre}{cmd}"
        if stdout is not None:
            full_cmd  += f" > {stdout} "
        if stderr is not None:
            full_cmd  += f" 2> {stderr} "
        if stdin is not None:
            full_cmd  += f" < {stdin} "

        full_cmd += "\""

        # Prepare arguments for invoke/fabric
        if background:
            pty=False

        # Print command if necessary
        if not quiet:
            agenda.subtask("[{}]{} {}".format(self.addr.ljust(10), " (bg) " if background else "      ", full_cmd))

        # Finally actually run it
        return super().run(full_cmd, *args, hide=True, warn=True, pty=pty, **kwargs)

    def file_exists(self, fname):
        res = self.run(f"ls {fname}")
        return res.exited == 0

    def prog_exists(self, prog):
        res = self.run(f"which {prog}")
        return res.exited == 0

    def check_proc(self, proc_name, proc_out):
        res = self.run(f"pgrep {proc_name}")
        if res.exited != 0:
            agenda.subfailure(f'failed to find running process with name \"{proc_name}\" on {self.addr}')
            res = self.run(f'tail {proc_out}')
            if res.exited == 0:
                print(res.command)
                print(res.stdout)
            else:
                print(res)
            sys.exit(1)


    def check_file(self, grep, where):
        res = self.run(f"grep \"{grep}\" {where}")
        if res.exited != 0:
            agenda.subfailure(f"Unable to find search string (\"{grep}\") in process output file {where}")
            res = self.run(f'tail {where}')
            if res.exited == 0:
                print(res.command)
                print(res.stdout)
            sys.exit(1)

    def local_path(self, path):
        r = self.run(f"ls {path}")
        return r.stdout.strip().replace("'", "")

    def put(self, local_file, remote=None, preserve_mode=True):
        if remote and remote[0] == "~":
            remote = remote[2:]
        agenda.subtask("[{}] scp localhost:{} -> {}:{}".format(
            self.addr,
            local_file,
            self.addr,
            remote
        ))

        return super().put(local_file, remote, preserve_mode)

    def get(self, remote_file, local=None, preserve_mode=True):
        if local is None:
            local = remote_file

        agenda.subtask("[{}] scp {}:{} -> localhost:{}".format(
            self.addr,
            self.addr,
            remote_file,
            local
        ))

        return super().get(remote_file, local=local, preserve_mode=preserve_mode)

def get_local(filename, local=None, preserve_mode=True):
    assert(local is not None)
    subprocess.run(f"mv {filename} {local}", shell=True)


def check(ok, msg, addr, allowed=[]):
    # exit code 0 is always ok, allowed is in addition
    if ok.exited != 0 and ok.exited not in allowed:
        agenda.subfailure(f"{msg} on {addr}: {ok.exited} not in {allowed}")
        agenda.subfailure("stdout")
        print(ok.stdout)
        agenda.subfailure("stderr")
        print(ok.stderr)
        global thread_ok
        thread_ok = False # something went wrong.
        sys.exit(1)

def check_machine(ip):
    if 'exp' not in ip:
        raise Exception(f"Malformed ips: {ip}")

    addr = ip['exp']
    host = ip['access'] if 'access' in ip else addr
    alt = ip['alt'] if 'alt' in ip else host

    conn = ConnectionWrapper(host)
    if not conn.file_exists("~/burrito"):
        agenda.failure(f"No burrito on {host}")
        raise Exception(f"No burrito on {host}")

    commit = conn.run("git rev-parse --short HEAD", wd = "~/burrito")
    if commit.exited == 0:
        commit = commit.stdout.strip()
    agenda.subtask(f"burrito commit {commit} on {host}")
    conn.addr = addr
    conn.alt = alt
    return (conn, commit)

def setup_machine(conn, outdir, datapaths, dpdk_driver):
    ok = conn.run(f"mkdir -p ~/burrito/{outdir}")
    check(ok, "mk outdir", conn.addr)

    agenda.task("building experiment binaries on {conn.addr}")

    if 'shenango_channel' in datapaths:
        # need to compile iokerneld
        ok = conn.run("make", wd = "~/burrito/shenango-chunnel/caladan")
        check(ok, "build shenango", conn.addr)

    needed_features = ["bin"]
    for d in datapaths:
        if 'shenango_channel' == d:
            if 'shenango-chunnel' not in needed_features:
                needed_features.append('shenango-chunnel')
        elif 'dpdk' in d:
            if 'dpdk-direct' not in needed_features:
                needed_features.append('dpdk-direct')
            if dpdk_driver == 'mlx' and 'cx3_mlx' not in needed_features:
                needed_features.append('cx3_mlx')
            elif dpdk_driver == 'intel' and 'xl710_intel' not in needed_features:
                needed_features.append('xl710_intel')
    agenda.subtask(f"building kvserver features={needed_features}")
    ok = conn.run(f"~/.cargo/bin/cargo build --release --features=\"{','.join(needed_features)}\" --bin=\"kvserver\"", wd = "~/burrito/kvstore")
    check(ok, "kvserver build", conn.addr)

    agenda.subtask(f"building ycsb features={needed_features}")
    ok = conn.run(f"~/.cargo/bin/cargo build --release --features=\"{','.join(needed_features[1:])}\" --bin=\"ycsb\"", wd = "~/burrito/kvstore-ycsb")
    check(ok, "ycsb build", conn.addr)

    return conn

def write_cfg(conn, config):
    from random import randint
    fname = randint(1,1000)
    fname = f"{fname}.config"
    with open(f"{fname}", 'w') as f:
        f.write(config)

    agenda.subtask(f"{conn.addr} config file: {fname}")
    if not conn.local:
        agenda.subtask(f"{conn.addr} config file: {fname}")
        conn.put(f"{fname}", "~/burrito/host.config")
    else:
        subprocess.run(f"rm -f host.config && cp {fname} host.config", shell=True)

def write_shenango_config(conn):
    shenango_config = f"""\
host_addr {conn.addr}
host_netmask 255.255.255.0
host_gateway 10.1.1.1
runtime_kthreads 2
runtime_spininng_kthreads 2
runtime_guaranteed_kthreads 2
"""
    write_cfg(conn, shenango_config)

def write_dpdk_config(conn):
    dpdk_config = f"""\
[dpdk]
eal_init = ["-n", "4", "-l", "0-6", "--allow", "0000:08:00.0", "--proc-type=auto"]

[net]
ip = "{conn.addr}"

  [[net.arp]]
  ip = "10.1.1.2"
  mac = "f4:52:14:76:98:10"

  [[net.arp]]
  ip = "10.1.1.5"
  mac = "f4:52:14:76:98:30"

  [[net.arp]]
  ip = "10.1.1.6"
  mac = "f4:52:14:76:98:a0"

  [[net.arp]]
  ip = "10.1.1.7"
  mac = "f4:52:14:76:a4:40"

  [[net.arp]]
  ip = "10.1.1.8"
  mac = "f4:52:14:76:a4:90"

  [[net.arp]]
  ip = "10.1.1.9"
  mac = "f4:52:14:76:a4:70"
"""
    write_cfg(conn, dpdk_config)


def get_timeout(wrkfile, interarrival_us):
    with open(wrkfile, 'r') as f:
        num_reqs = sum(1 for _ in f)
        total_time_s = num_reqs * interarrival_us / 1e6
        return max(int(total_time_s * 2), 180)

dpdk_ld_var = "LD_LIBRARY_PATH=/usr/local/lib64:/usr/local/lib:dpdk-direct/dpdk-wrapper/dpdk/install/lib/x86_64-linux-gnu"

def start_server(conn, redis_addr, outf, datapath='shenango_channel', shards=1, skip_negotiation=False):
    conn.run("sudo pkill -INT kvserver")
    conn.run("sudo pkill -INT iokerneld")

    if datapath == 'shenango_channel':
        write_shenango_config(conn)
        conn.run("./iokerneld", wd="~/burrito/shenango-chunnel/caladan", sudo=True, background=True)
        datapath = 'shenango'
    elif datapath == 'kernel':
        pass
    elif 'dpdk' in datapath:
        write_dpdk_config(conn)
    else:
        raise Exception(f"unknown datapath {datapath}")

    skip_neg = '--skip-negotiation' if skip_negotiation else ''

    time.sleep(2)
    ok = conn.run(f"RUST_LOG=info {dpdk_ld_var} ./target/release/kvserver --ip-addr {conn.addr} --port 4242 --num-shards {shards} --redis-addr={redis_addr} --datapath={datapath} --cfg host.config {skip_neg} --log",
            wd="~/burrito",
            sudo=True,
            background=True,
            stdout=f"{outf}.out",
            stderr=f"{outf}.err",
            )
    check(ok, "spawn server", conn.addr)
    agenda.subtask("wait for kvserver check")
    time.sleep(8)
    conn.check_proc(f"kvserver", f"{outf}.err")

def run_client(conn, server, redis_addr, interarrival, poisson_arrivals, datapath, shardtype, skip_negotiation, outf, wrkfile):
    conn.run("sudo pkill -INT iokerneld")

    if datapath == 'shenango_channel':
        write_shenango_config(conn)
        conn.run("./iokerneld", wd="~/burrito/shenango-chunnel/caladan", sudo=True, background=True)
        datapath = 'shenango'
    elif datapath == 'kernel':
        pass
    elif 'dpdk' in datapath:
        write_dpdk_config(conn)
    else:
        raise Exception(f"unknown datapath {datapath}")

    if shardtype == 'client':
        shard_arg = '--use-clientsharding'
    elif shardtype == 'server':
        shard_arg = ''
    elif shardtype == 'basicclient':
        shard_arg = '--use-basicclient'
    else:
        raise f"unknown shardtype {shardtype}"

    skip_neg = ''
    while skip_negotiation > 0:
        skip_neg += f' --skip-negotiation={4242 + skip_negotiation} '
        skip_negotiation -= 1

    poisson_arg = "--poisson-arrivals" if poisson_arrivals  else ''
    timeout = get_timeout(wrkfile, interarrival)

    time.sleep(2)
    agenda.subtask(f"client starting, timeout {timeout} -> {outf}0.out")
    ok = conn.run(f"RUST_LOG=info {dpdk_ld_var} ./target/release/ycsb \
            --addr {server}:4242 \
            --redis-addr={redis_addr} \
            -i {interarrival} \
            --accesses {wrkfile} \
            --out-file={outf}0.data \
            --datapath={datapath} \
            --cfg host.config \
            {poisson_arg} \
            {shard_arg} \
            {skip_neg} \
            --logging --skip-loads",
        wd="~/burrito",
        sudo='dpdk' in datapath,
        stdout=f"{outf}0.out",
        stderr=f"{outf}0.err",
        timeout=timeout,
        )
    check(ok, "client", conn.addr)
    conn.run("sudo pkill -INT iokerneld")
    agenda.subtask("client done")

def start_redis(machine, use_sudo=False):
    machine.run("docker rm -f burrito-shard-redis", sudo=True)
    agenda.task("Starting redis")
    ok = machine.run("docker run \
            --name burrito-shard-redis \
            -d -p 6379:6379 redis:6",
            sudo=True,
            wd="~/burrito")
    check(ok, "start redis", machine.addr)
    ok = machine.run("docker ps | grep burrito-shard-redis", sudo=True)
    check(ok, "start redis", machine.addr)
    agenda.subtask(f"Started redis on {machine.host}")
    return f"{machine.alt}:6379"

def run_loads(conn, server, datapath, redis_addr, outf, wrkfile, skip_negotiation=0):
    conn.run("sudo pkill -INT iokerneld")

    if datapath == 'shenango_channel':
        write_shenango_config(conn)
        datapath = 'shenango'
    elif datapath == 'kernel':
        pass
    elif 'dpdk' in  datapath:
        write_dpdk_config(conn)
    else:
        raise Exception(f"unknown datapath {datapath}")

    skip_neg = ''
    while skip_negotiation > 0:
        skip_neg += f' --skip-negotiation={4242 + skip_negotiation} '
        skip_negotiation -= 1

    while True:
        if 'shenango' in datapath:
            conn.run("./iokerneld", wd="~/burrito/shenango-chunnel/caladan", sudo=True, background=True)

        time.sleep(2)
        loads_start = time.time()
        agenda.subtask(f"loads client starting")
        ok = None
        try:
            ok = conn.run(f"RUST_LOG=info,ycsb=debug,bertha=debug {dpdk_ld_var} ./target/release/ycsb \
                    --addr {server}:4242 \
                    --redis-addr={redis_addr} \
                    -i 1000 \
                    --accesses {wrkfile} \
                    --datapath {datapath} \
                    --cfg host.config \
                    {skip_neg} \
                    --logging --loads-only",
                wd="~/burrito",
                sudo='dpdk' in datapath,
                stdout=f"{outf}-loads.out",
                stderr=f"{outf}-loads.err",
                timeout=30,
                )
        except:
            agenda.subfailure(f"loads failed, retrying after {time.time() - loads_start} s")
        finally:
            conn.run("sudo pkill -INT iokerneld")
        if ok is None or ok.exited != 0:
            agenda.subfailure(f"loads failed, retrying after {time.time() - loads_start} s")
            continue
        else:
            agenda.subtask(f"loads client done: {time.time() - loads_start} s")
            break

def do_exp(iter_num,
    outdir=None,
    machines=None,
    num_shards=None,
    shardtype=None,
    ops_per_sec=None,
    datapath=None,
    poisson_arrivals=None,
    skip_negotiation=None,
    wrkload=None,
    overwrite=None
):
    assert(
        outdir is not None and
        machines is not None and
        num_shards is not None and
        shardtype is not None and
        ops_per_sec is not None and
        datapath is not None and
        poisson_arrivals is not None and
        skip_negotiation is not None and
        wrkload is not None and
        overwrite is not None
    )

    wrkname = wrkload.split("/")[-1].split(".")[0]
    noneg = '_noneg' if skip_negotiation else ''
    server_prefix = f"{outdir}/{datapath}{noneg}-{num_shards}-{shardtype}shard-{ops_per_sec}-poisson={poisson_arrivals}-{wrkname}-{iter_num}-kvserver"
    outf = f"{outdir}/{datapath}{noneg}-{num_shards}-{shardtype}shard-{ops_per_sec}-poisson={poisson_arrivals}-{wrkname}-{iter_num}-client"

    for m in machines:
        if m.local:
            m.run(f"mkdir -p {outdir}", wd="~/burrito")
            continue
        m.run(f"rm -rf {outdir}", wd="~/burrito")
        m.run(f"mkdir -p {outdir}", wd="~/burrito")

    if not overwrite and os.path.exists(f"{outf}0-{machines[1].addr}.data"):
        agenda.task(f"skipping: server = {machines[0].addr}, datapath = {datapath}, skip_negotiation = {skip_negotiation} num_shards = {num_shards}, shardtype = {shardtype}, load = {ops_per_sec} ops/s")
        return True
    else:
        agenda.task(f"running: {outf}0-{machines[1].addr}.data")

    # load = (n (client threads / proc) * 1 (procs/machine) * {len(machines) - 1} (machines))
    #        / {interarrival} (per client thread)
    num_client_threads = int(wrkname.split('-')[-1])
    interarrival_secs = num_client_threads * len(machines[1:]) / ops_per_sec
    interarrival_us = int(interarrival_secs * 1e6)

    redis_addr = start_redis(machines[0])
    time.sleep(5)
    server_addr = machines[0].addr
    agenda.task(f"starting: server = {machines[0].addr}, datapath = {datapath}, num_shards = {num_shards}, shardtype = {shardtype}, load = {ops_per_sec} ops/s -> interarrival_us = {interarrival_us}, num_clients = {len(machines)-1}")

    # first one is the server, start the server
    agenda.subtask("starting server")
    redis_port = redis_addr.split(":")[-1]
    start_server(machines[0], f"127.0.0.1:{redis_port}", server_prefix, datapath=datapath, shards=num_shards, skip_negotiation=skip_negotiation)
    time.sleep(5)
    # prime the server with loads
    agenda.task("doing loads")
    run_loads(machines[1], server_addr, datapath, redis_addr, outf, wrkload, skip_negotiation=num_shards if skip_negotiation else 0)
    try:
        machines[1].get(f"{outf}-loads.out", local=f"{outf}-loads.out", preserve_mode=False)
        machines[1].get(f"{outf}-loads.err", local=f"{outf}-loads.err", preserve_mode=False)
    except Exception as e:
        agenda.subfailure(f"Could not get file {outf}-loads.[out,err] from loads client: {e}")

    # others are clients
    agenda.task("starting clients")
    clients = [threading.Thread(target=run_client, args=(
            m,
            server_addr,
            redis_addr,
            interarrival_us,
            poisson_arrivals,
            datapath,
            shardtype,
            num_shards if skip_negotiation else 0,
            outf,
            wrkload
        ),
    ) for m in machines[1:]]

    [t.start() for t in clients]
    [t.join() for t in clients]
    agenda.task("all clients returned")

    # kill the server
    machines[0].run("sudo pkill -INT kvserver")
    machines[0].run("sudo pkill -INT iokerneld")

    for m in machines:
        m.run("rm ~/burrito/*.config")

    agenda.task("get server files")
    if not machines[0].local:
        machines[0].get(f"~/burrito/{server_prefix}.out", local=f"{server_prefix}.out", preserve_mode=False)
        machines[0].get(f"~/burrito/{server_prefix}.err", local=f"{server_prefix}.err", preserve_mode=False)

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
### shardtype = ["client"]
### shards = [6]
### batching = [0, 16, 64, 256]
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

    ops_per_sec = cfg['exp']['load']
    if cfg['exp']['shards'] is None:
        agenda.failure("Need shards arg")
        sys.exit(1)

    for t in cfg['exp']['wrk']:
        if not t.endswith(".access") or '-' not in t:
            agenda.failure(f"Workload file should be <name>-<concurrency>.access, got {t}")
            sys.exit(1)

    for t in cfg['exp']['shardtype']:
        if t not in ['client', 'server', 'basicclient']: # basicclient is a subset of server
            agenda.failure(f"Unknown shardtype {t}")
            sys.exit(1)

    if 'datapath' not in cfg['exp']:
        cfg['exp']['datapath'] = ['kernel']
    for t in cfg['exp']['datapath']:
        if t not in ['shenango_channel', 'kernel', 'dpdkthread', 'dpdkmulti']:
            agenda.failure(f"Unknown datapath {t}")
            sys.exit(1)

    if any('dpdk' in d for d in cfg['exp']['datapath']):
        if args.dpdk_driver not in ['mlx', 'intel']:
            agenda.failure("If using dpdk datapath, must specify mlx or intel driver.")

    if 'negotiation' not in cfg['exp']:
        cfg['exp']['negotiation'] = [True]
    for t in cfg['exp']['negotiation']:
        if t not in [True,False]:
            agenda.failure("Skip-negotiation must be bool")
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
    setups = [threading.Thread(target=setup_machine, args=(m, outdir, cfg['exp']['datapath'], args.dpdk_driver)) for m in machines]
    [t.start() for t in setups]
    [t.join() for t in setups]
    if not thread_ok:
        agenda.failure("Something went wrong")
        sys.exit(1)
    agenda.task("...done building burrito")

    # copy config file to outdir
    shutil.copy2(args.config, args.outdir)

    for dp in cfg['exp']['datapath']:
        for neg in cfg['exp']['negotiation']:
            for w in cfg['exp']['wrk']:
                for s in cfg['exp']['shards']:
                    for t in cfg['exp']['shardtype']:
                        for p in cfg['exp']['poisson-arrivals']:
                            for o in ops_per_sec:
                                do_exp(0,
                                    outdir=outdir,
                                    machines=machines,
                                    num_shards=s,
                                    shardtype=t,
                                    ops_per_sec=o,
                                    datapath=dp,
                                    poisson_arrivals=p,
                                    skip_negotiation=not neg,
                                    wrkload=w,
                                    overwrite=args.overwrite
                                )

    agenda.task("done")
