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

global thread_ok
thread_ok = True

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

    def check_code(self, ret) -> bool:
        return ret.exited == 0

    def file_exists(self, fname, **kwargs):
        res = self.run(f"ls {fname}", **kwargs)
        return res.exited == 0

    def prog_exists(self, prog, **kwargs):
        res = self.run(f"which {prog}", **kwargs)
        return res.exited == 0

    def check_proc(self, proc_name, proc_outs):
        res = self.run(f"pgrep {proc_name}")
        if res.exited != 0:
            agenda.subfailure(f'failed to find running process with name \"{proc_name}\" on {self.addr}')
            for proc_out in proc_outs:
                res = self.run(f'tail {proc_out}')
                if res.exited == 0:
                    print(res.command)
                    print(res.stdout)
                else:
                    print(res)
            raise Exception("Process did not start correctly")


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

    def put(self, local_file, remote=None, preserve_mode=True, quiet=False):
        if remote and remote[0] == "~":
            remote = remote[2:]
        if not quiet:
            agenda.subtask("[{}] scp localhost:{} -> {}:{}".format(
                self.addr,
                local_file,
                self.addr,
                remote
            ))

        return super().put(local_file, remote, preserve_mode)

    def get(self, remote_file, local=None, preserve_mode=True, quiet=False):
        if local is None:
            local = remote_file
        if not quiet:
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
        raise Exception(f"{msg} on {addr}: {ok.exited} not in {allowed}")

def check_machine(ip):
    if 'exp' not in ip:
        raise Exception(f"Malformed ips: {ip}")

    addr = ip['exp']
    host = ip['access'] if 'access' in ip else addr
    alt = ip['alt'] if 'alt' in ip else host

    conn = ConnectionWrapper(host, user=ip['user'] if 'user' in ip else None, port=ip['port'] if 'port' in ip else None)
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

def setup_machine(conn, outdir, datapaths, dpdk_driver, target_dir):
    agenda.task(f"[{conn.addr}] setup machine")
    try:
        agenda.subtask(f"[{conn.addr}] make outdir {outdir}")
        ok = conn.run(f"mkdir -p {target_dir if target_dir is not None else '~/burrito'}/{outdir}")
        check(ok, "mk outdir", conn.addr)

        # dpdk dependencies, rust, and docker
        ok = conn.run("bash scripts/install-kv-deps.sh", wd="~/burrito")
        check(ok, "install dependencies", conn.addr)

        if 'shenango_channel' in datapaths:
            agenda.subtask(f"[{conn.addr}] building shenango")
            # need to compile iokerneld
            ok = conn.run("make", wd = "~/burrito/shenango-chunnel/caladan")
            check(ok, "build shenango", conn.addr)

            ok = conn.run("./scripts/setup_machine.sh", wd = "~/burrito/shenango-chunnel/caladan")
            check(ok, "shenango setup-machine", conn.addr)

        if any('dpdk' in d for d in datapaths):
            ok = conn.run("./usertools/dpdk-hugepages.py -p 2M --setup 10G", wd = "~/burrito/dpdk-direct/dpdk-wrapper/dpdk", sudo=True)
            check(ok, "reserve hugepages", conn.addr)
    except Exception as e:
        agenda.failure(f"[{conn.addr}] setup_machine failed: {e}")
        global thread_ok
        thread_ok = False
        raise e

def compile_binaries(conn, outdir, datapaths, dpdk_driver, target_dir):
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

        nightly = '+nightly' if 'shenango-chunnel' in needed_features else ''
        agenda.subtask(f"building kvserver features={needed_features}, target-dir {target_dir}, {nightly}")
        target_dir_arg = f"--target-dir {target_dir}/burrito-target" if target_dir is not None else ""
        ok = conn.run(f"~/.cargo/bin/cargo {nightly} build --release --features=\"{','.join(needed_features)}\" --bin=\"kvserver\" {target_dir_arg}", wd = "~/burrito/kvstore")
        check(ok, "kvserver build", conn.addr)

        agenda.subtask(f"building ycsb features={needed_features}, {nightly}")
        ok = conn.run(f"~/.cargo/bin/cargo {nightly} build --release --features=\"{','.join(needed_features[1:])}\" --bin=\"ycsb\" {target_dir_arg}", wd = "~/burrito/kvstore-ycsb")
        check(ok, "ycsb build", conn.addr)

        if 'dpdkmulti' in datapaths:
            needed_features = [f for f in needed_features if f == 'xl710_intel' or f == 'cx3_mlx' or f == 'cx4_mlx']
            agenda.subtask(f"building kv-dpdk features={needed_features} target-dir {target_dir}")
            ok = conn.run(f"~/.cargo/bin/cargo build --release --features=\"{','.join(needed_features)}\" {target_dir_arg}", wd = "~/burrito/kv-dpdk")
            check(ok, "kv-dpdk build", conn.addr)
        return conn
    except Exception as e:
        agenda.failure(f"[{conn.addr}] setup_machine failed: {e}")
        global thread_ok
        thread_ok = False
        raise e

def write_cfg(conn, config, name):
    from random import randint
    fname = randint(1,1000)
    fname = f"{fname}.config"
    with open(f"{fname}", 'w') as f:
        f.write(config)

    agenda.subtask(f"[{conn.addr}] {name} config file: {fname}")
    if not conn.is_local:
        conn.put(f"{fname}", f"~/burrito/{name}.config")
    else:
        subprocess.run(f"rm -f {name}.config && cp {fname} {name}.config", shell=True)

def get_pci_addr(conn):
    search = None
    if dpdk_driver is not None and 'mlx' in dpdk_driver:
        search = 'drv=mlx.*Active'
    elif dpdk_driver == 'intel':
        search = 'drv=igb'
    else:
        raise Exception(f"Unknown driver {dpdk_driver}")

    res = conn.run(f"sudo ./usertools/dpdk-devbind.py --status-dev net | grep \"{search}\"", wd="~/burrito/dpdk-direct/dpdk-wrapper/dpdk")
    pci_addr = None
    if res.exited == 0:
        pci_addr = res.stdout.strip().split()[0]
        agenda.subtask(f"[{conn.addr}] pci: {pci_addr}")
    elif dpdk_driver == 'intel':
        res = conn.run("ip -o link")
        check(res, "ip link", conn.addr)
        ip_link_out = res.stdout
        iface = get_iface(ip_link_out, conn.mac)
        conn.exp_iface = iface
        res = conn.run(f"./usertools/dpdk-devbind.py --status-dev net | grep '{iface}' | cut -d' ' -f1", wd="~/burrito/dpdk-direct/dpdk-wrapper/dpdk", sudo=True)
        check(res, "dpdk-devbind show", conn.addr)
        pci_addr = res.stdout.strip()
        agenda.subtask(f"[{conn.addr}] pci: {pci_addr} iface: {iface}")
    else:
        raise Exception(f"Could not get pci address on {conn.addr}")
    return pci_addr


def write_shenango_config(conn, num_threads):
    shenango_config = f"""\
host_addr {conn.addr}
host_netmask 255.255.255.0
host_gateway 10.1.1.1
runtime_kthreads {num_threads}
runtime_spinning_kthreads {num_threads}
runtime_guaranteed_kthreads {num_threads}
"""
    write_cfg(conn, shenango_config, 'shenango')


dpdk_driver = None
def write_dpdk_config(conn, machines, lcores):
    pci_addr = conn.pci_addr
    agenda.subtask(f"dpdk configuration file pci_addr={pci_addr} lcore_cfg={lcores}")
    # compile arp entries
    arp_cfg = '\n\n'.join([f"""\
  [[net.arp]]
  ip = "{m.addr}"
  mac = "{m.mac}"
""" for m in machines])
    dpdk_config = f"""\
[dpdk]
eal_init = ["-n", "4", "-l", "{lcores}", "--allow", "{pci_addr}", "--proc-type=auto"]

[net]
ip = "{conn.addr}"

{arp_cfg}
"""
    write_cfg(conn, dpdk_config, 'dpdk')

def get_timeout_inner(num_reqs, interarrival_us) -> int:
    total_time_s = num_reqs * interarrival_us / 1e6
    return max(int(total_time_s * 2), 180)

def get_timeout_local(wrkfile, interarrival_us) -> int:
    with open(wrkfile, 'r') as f:
        num_reqs = sum(1 for _ in f)
        return get_timeout_inner(num_reqs, interarrival_us)

def get_timeout_remote(conn, wrkfile, interarrival_us) -> int:
    res = conn.run(f"wc -l {wrkfile}", wd="~/burrito")
    if res.exited != 0:
        raise Exception(f"Unable to read {wrkfile}")

    num_reqs = int(res.stdout.strip().split()[0])
    return get_timeout_inner(num_reqs, interarrival_us)

dpdk_ld_var = "LD_LIBRARY_PATH=/usr/local/lib64:/usr/local/lib:dpdk-direct/dpdk-wrapper/dpdk/build/lib/x86_64-linux-gnu"

def start_server(conn, redis_addr, outf, datapath='kernel', shards=1, skip_negotiation=False, bin_root="./target/release"):
    conn.run("sudo pkill -INT kvserver")
    conn.run("sudo pkill -INT iokerneld")

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
    ok = conn.run(f"RUST_LOG=info {dpdk_ld_var} {bin_root}/kvserver --ip-addr {conn.addr} --port 4242 --num-shards {shards} --redis-addr={redis_addr} --datapath={datapath} {cfg} {skip_neg} --log",
            wd="~/burrito",
            sudo=True,
            background=True,
            stdout=f"{outf}.out",
            stderr=f"{outf}.err",
            )
    check(ok, "spawn server", conn.addr)
    agenda.subtask("wait for kvserver check")
    time.sleep(8)
    conn.check_proc(f"kvserver", [f"burrito/{outf}.err", f"burrito/{outf}.out"])

def start_server_no_chunnels(conn, outf, no_chunnels, shards=1, bin_root="./target/release"):
    conn.run("sudo pkill -INT kvserver")
    conn.run("sudo pkill -INT iokerneld")
    time.sleep(2)
    no_chunnels_arg = '--conns' if no_chunnels == 'conns' else ''
    ok = conn.run(f"RUST_LOG=info {dpdk_ld_var} {bin_root}/kvserver-dpdk \
            --addr {conn.addr}:4242 \
            --num-shards {shards} \
            --cfg=dpdk.config \
            {no_chunnels_arg}",
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


def run_client(conn, cfg_client, server, redis_addr, interarrival, poisson_arrivals, datapath, shardtype, skip_negotiation, outf, wrkfile, bin_root="./target/release"):
    conn.run("sudo pkill -INT iokerneld")

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

    if shardtype == 'client':
        shard_arg = '--use-clientsharding'
    elif shardtype == 'server' or shardtype == 'remote':
        shard_arg = ''
    elif shardtype == 'basicclient':
        shard_arg = '--use-basicclient'
    else:
        raise Exception(f"unknown shardtype {shardtype}")

    skip_neg = ''
    while skip_negotiation > 0:
        skip_neg += f' --skip-negotiation={4242 + skip_negotiation} '
        skip_negotiation -= 1

    poisson_arg = "--poisson-arrivals" if poisson_arrivals  else ''

    timeout = None
    try:
        timeout = get_timeout_local(wrkfile, interarrival)
    except:
        try:
            timeout = get_timeout_remote(conn, wrkfile, interarrival)
        except Exception as e:
            global thread_ok
            thread_ok = False
            raise e

    extra_cfg = ' '.join(f"--{key}={cfg_client[key]}" for key in cfg_client)
    outf = f"{outf}-{conn.addr}"

    time.sleep(2)
    agenda.subtask(f"client starting, timeout {timeout} -> {outf}.out")
    ok = conn.run(f"RUST_LOG=info {dpdk_ld_var} {bin_root}/ycsb \
            --addr {server}:4242 \
            --redis-addr={redis_addr} \
            -i {interarrival} \
            --accesses {wrkfile} \
            --out-file={outf}.data \
            --datapath={datapath} \
            {cfg} \
            {extra_cfg} \
            {poisson_arg} \
            {shard_arg} \
            {skip_neg} \
            --skip-loads",
        wd="~/burrito",
        sudo='dpdk' in datapath,
        stdout=f"{outf}.out",
        stderr=f"{outf}.err",
        timeout=timeout,
        )
    check(ok, "client", conn.addr)
    conn.run("sudo pkill -INT iokerneld")
    agenda.subtask("client done")

def start_redis(machine):
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

def run_client_no_chunnels(conn, cfg_client, server, num_shards, interarrival, poisson_arrivals, outf, wrkfile, bin_root="./target/release"):
    conn.run("sudo pkill -INT iokerneld")
    poisson_arg = "--poisson-arrivals" if poisson_arrivals  else ''
    timeout = None
    try:
        timeout = get_timeout_local(wrkfile, interarrival)
    except:
        try:
            timeout = get_timeout_remote(conn, wrkfile, interarrival)
        except Exception as e:
            global thread_ok
            thread_ok = False
            raise e

    extra_cfg = ' '.join(f"--{key}={cfg_client[key]}" for key in cfg_client)
    outf = f"{outf}-{conn.addr}"

    time.sleep(2)
    agenda.subtask(f"client starting, timeout {timeout} -> {outf}.out")
    ok = conn.run(f"RUST_LOG=info {dpdk_ld_var} {bin_root}/ycsb-dpdk \
            --addr {server}:4242 \
            --num-shards {num_shards} \
            -i {interarrival} \
            --accesses {wrkfile} \
            --out-file={outf}0.data \
            --config dpdk.config \
            {extra_cfg} \
            {poisson_arg} \
            --skip-loads",
        wd="~/burrito",
        sudo=True,
        stdout=f"{outf}.out",
        stderr=f"{outf}.err",
        timeout=timeout,
        )
    check(ok, "client", conn.addr)
    agenda.subtask("client done")
    pass

def run_loads(conn, cfg_client, server, datapath, redis_addr, outf, wrkfile, skip_negotiation=0, bin_root="./target/release"):
    conn.run("sudo pkill -INT iokerneld")

    skip_neg = ''
    while skip_negotiation > 0:
        skip_neg += f' --skip-negotiation={4242 + skip_negotiation} '
        skip_negotiation -= 1

    if datapath == 'shenango_channel':
        conn.run(f"./iokerneld ias nicpci {conn.pci_addr}", wd="~/burrito/shenango-chunnel/caladan", sudo=True, background=True)
        datapath = 'shenango'
        cfg = '--cfg=shenango.config'
    elif datapath == 'kernel':
        cfg = ''
    elif 'dpdk' in  datapath:
        cfg = '--cfg=dpdk.config'
    else:
        raise Exception(f"unknown datapath {datapath}")

    extra_cfg = ' '.join(f"--{key}={cfg_client[key]}" for key in cfg_client)

    while True:
        if 'shenango' in datapath:
            conn.run("./iokerneld", wd="~/burrito/shenango-chunnel/caladan", sudo=True, background=True)

        time.sleep(2)
        loads_start = time.time()
        agenda.subtask(f"loads client starting")
        ok = None
        try:
            ok = conn.run(f"RUST_LOG=debug {dpdk_ld_var} {bin_root}/ycsb \
                    --addr {server}:4242 \
                    --redis-addr={redis_addr} \
                    -i 1000 \
                    --accesses {wrkfile} \
                    --datapath {datapath} \
                    {cfg} \
                    {extra_cfg} \
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

def run_loads_no_chunnels(conn, cfg_client, server, num_shards, outf, wrkfile, bin_root="./target/release"):
    conn.run("sudo pkill -INT iokerneld")
    extra_cfg = ' '.join(f"--{key}={cfg_client[key]}" for key in cfg_client)

    while True:
        time.sleep(2)
        loads_start = time.time()
        agenda.subtask(f"loads client starting")
        ok = None
        try:
            ok = conn.run(f"RUST_LOG=info {dpdk_ld_var} {bin_root}/ycsb-dpdk \
                    --addr {server}:4242 \
                    -i 1000 \
                    --accesses {wrkfile} \
                    --num-shards {num_shards} \
                    --config dpdk.config \
                    {extra_cfg} \
                    --loads-only",
                wd="~/burrito",
                sudo=True,
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

def local_path(outf, orig_outdir):
    sp = outf.split('/')
    i = 0
    for dr in sp:
        i += 1
        if dr == orig_outdir:
            return '/'.join([dr] + sp[i:])
    raise Exception(f"{orig_outdir} not found in {outf}")

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
    overwrite=None,
    cfg_client=None,
    no_chunnels=False,
    cloudlab=False,
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

    target_dir = None
    orig_outdir = outdir
    if cloudlab:
        res = machines[0].run("ls /proj/")
        target_dir = f"/proj/{res.stdout.strip().split()[0]}"
        outdir = f"{target_dir}/{outdir}"

    if (no_chunnels != 'off' and no_chunnels != False) and (datapath != 'dpdkmulti' or shardtype != 'client'):
        agenda.task("skipping: no_chunnels mode supported only for dpdkmulti + client sharding")
        return

    wrkname = wrkload.split("/")[-1].split(".")[0]
    noneg = '_noneg' if skip_negotiation else ''
    if no_chunnels == 'full' or no_chunnels == True:
       nochunnels  = '_nochunnels'
    elif no_chunnels == 'conns':
       nochunnels = '_nochunnels_conns'
    else:
       nochunnels = ''

    server_prefix = f"{outdir}/{datapath}{noneg}{nochunnels}-{num_shards}-{shardtype}shard-{ops_per_sec}-poisson={poisson_arrivals}-{wrkname}-{iter_num}-kvserver"
    outf = f"{outdir}/{datapath}{noneg}{nochunnels}-{num_shards}-{shardtype}shard-{ops_per_sec}-poisson={poisson_arrivals}-{wrkname}-{iter_num}-client"

    for m in machines:
        if m.is_local:
            m.run(f"mkdir -p {outdir}", wd="~/burrito")
            continue
        m.run(f"rm -rf {outdir}", wd="~/burrito")
        m.run(f"mkdir -p {outdir}", wd="~/burrito")

    if not overwrite and os.path.exists(f"{outf}0-{machines[1].addr}.data"):
        agenda.task(f"skipping: server = {machines[0].addr}, datapath = {datapath}, skip_negotiation = {skip_negotiation}, no_chunnels = {no_chunnels}, num_shards = {num_shards}, shardtype = {shardtype}, load = {ops_per_sec} ops/s")
        return True
    else:
        agenda.task(f"running: {outf}0-{machines[1].addr}.data")

    # load = (n (client threads / proc) * 1 (procs/machine) * {len(machines) - 1} (machines))
    #        / {interarrival} (per client thread)
    num_client_threads = int(wrkname.split('-')[-1])
    interarrival_secs = num_client_threads * len(machines[1:]) / ops_per_sec
    interarrival_us = int(interarrival_secs * 1e6)

    if not no_chunnels:
        redis_addr = start_redis(machines[0])
        time.sleep(5)
    else:
        redis_addr = ""

    server_addr = machines[0].addr
    agenda.task(f"starting: server = {machines[0].addr}, datapath = {datapath}, num_shards = {num_shards}, shardtype = {shardtype}, load = {ops_per_sec} ops/s -> interarrival_us = {interarrival_us}, num_clients = {len(machines)-1}")

    bin_root_dir = f"{target_dir}/burrito-target/release" if target_dir is not None else "./target/release"
    # first one is the server, start the server
    agenda.subtask("starting server")
    if len(nochunnels) > 0:
        start_server_no_chunnels(machines[0], server_prefix, no_chunnels, shards=num_shards, bin_root=bin_root_dir)
        time.sleep(5)
    else:
        redis_port = redis_addr.split(":")[-1]
        start_server(machines[0], f"127.0.0.1:{redis_port}", server_prefix, datapath=datapath, shards=num_shards, skip_negotiation=skip_negotiation, bin_root=bin_root_dir)
        time.sleep(5)

    # prime the server with loads
    agenda.task("doing loads")
    if no_chunnels:
        run_loads_no_chunnels(machines[1], cfg_client, server_addr, num_shards, outf, wrkload, bin_root=bin_root_dir)
    else:
        run_loads(
            machines[1],
            cfg_client,
            server_addr,
            datapath,
            redis_addr,
            outf,
            wrkload,
            skip_negotiation=num_shards if skip_negotiation else 0,
            bin_root=bin_root_dir
        )

    get_fn = f"{'' if cloudlab else 'burrito/'}{outf}-loads"
    loc = local_path(f"{outf}-loads", orig_outdir) if cloudlab else f"{outf}-loads"
    try:
        machines[1].get(get_fn+".out", local=loc+".out", preserve_mode=False)
        machines[1].get(get_fn+".err", local=loc+".err", preserve_mode=False)
    except Exception as e:
        agenda.subfailure(f"Could not get file {loc}.[out,err] from loads client: {e}")

    # others are clients
    agenda.task("starting clients")
    if no_chunnels:
        clients = [threading.Thread(target=run_client_no_chunnels, args=(
                m,
                cfg_client,
                server_addr,
                num_shards,
                interarrival_us,
                poisson_arrivals,
                outf,
                wrkload
            ),
            kwargs={'bin_root': bin_root_dir}
        ) for m in machines[1:]]
    else:
        clients = [threading.Thread(target=run_client, args=(
                m,
                cfg_client,
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
            kwargs={'bin_root': bin_root_dir}
        ) for m in machines[1:]]

    [t.start() for t in clients]
    [t.join() for t in clients]
    agenda.task("all clients returned")

    # kill the server
    machines[0].run("sudo pkill -INT kvserver")
    machines[0].run("sudo pkill -INT iokerneld")

    agenda.task("get server files")
    if not machines[0].is_local:
        get_fn = f"{'' if cloudlab else 'burrito/'}{server_prefix}"
        loc = local_path(f"{server_prefix}", orig_outdir) if cloudlab else f"{server_prefix}"
        machines[0].get(get_fn +'.out', local=loc +'.out', preserve_mode=False)
        machines[0].get(get_fn +'.err', local=loc +'.err', preserve_mode=False)

    def get_files(c):
        fn = c.get
        if c.is_local:
            agenda.subtask(f"Use get_local: {c.host}")
            fn = get_local

        get_fn = f"{'' if cloudlab else 'burrito/'}{outf}-{c.addr}"
        loc = local_path(f"{outf}-{c.addr}", orig_outdir) if cloudlab else f"{outf}-{c.addr}"

        agenda.subtask(f"getting {get_fn}.err")
        fn(
            f"{get_fn}.err",
            local=f"{loc}.err",
            preserve_mode=False,
        )
        agenda.subtask(f"getting {get_fn}.out")
        fn(
            f"{get_fn}.out",
            local=f"{loc}.out",
            preserve_mode=False,
        )
        agenda.subtask(f"getting {get_fn}.data")
        fn(
            f"{get_fn}.data",
            local=f"{loc}.data",
            preserve_mode=False,
        )
        agenda.subtask(f"getting {get_fn}.trace")
        fn(
            f"{get_fn}.trace",
            local=f"{loc}.trace",
            preserve_mode=False,
        )

    agenda.task("get client files")
    for c in machines[1:]:
        try:
            get_files(c)
            #agenda.subtask("getting perf")
            #if no_chunnels:
            #    c.get(f"~/burrito/perf.data", "./nochunnels.perf.data")
            #else:
            #    c.get(f"~/burrito/perf.data", "./chunnels.perf.data")
        except Exception as e:
            agenda.subfailure(f"At least one file missing for {c}: {e}")

    agenda.task("done")
    return True

def generate_ycsb_file(conn, wrkfile):
    # "./kvstore-ycsb/ycsbc-mock/wrkloadb-4.access" -> wrkloadb-4
    slug = wrkfile.strip().split('/')[-1].split('.')[0]
    wrkload, threads = slug.split('-')

    ok = conn.run(f"ls {wrkload}-{threads}.access", wd="~/burrito/kvstore-ycsb/ycsbc-mock")
    if ok.exited != 0:
        ok = conn.run(f"ls {wrkload}.out", wd="~/burrito/kvstore-ycsb/ycsbc-mock")
        if ok.exited != 0:
           ok = conn.run(f"./ycsbc -db mock -threads 1 -P workloads/{wrkload}.spec > {wrkload}.out", wd="~/burrito/kvstore-ycsb/ycsbc-mock")
           if ok.exited != 0:
               raise Exception(f"Problem running ycsb. Either ycsbc not compiled, or dependency not installed, or spec file not available. {ok}")

        ok = conn.run(f"python3 augment.py {wrkload}.out {wrkload}-{threads} {threads}", wd = "~/burrito/kvstore-ycsb/ycsbc-mock")
        if ok.exited != 0:
            raise Exception(f"augment script failed: {ok}")

def intel_setup(machines):
    agenda.task("setup intel-driver machine for DPDK")
    for conn in machines:
        ok = conn.run("lsmod | grep uio", wd = "~/burrito/dpdk-direct/dpdk-wrapper/dpdk")
        if ok.exited != 0:
            agenda.subtask("igb_uio kmod not loaded")

            # igb_uio
            ok = conn.run("modprobe uio", wd = "~/burrito/dpdk-direct/dpdk-wrapper/dpdk", sudo=True)
            check(ok, "modprobe uio", conn.addr)

            ok = conn.run("ls ./dpdk-kmods/linux/igb_uio/igb_uio.ko", wd = "~")
            if ok.exited != 0:
                agenda.subtask("igb_uio kmod not cloned or built")
                ok = conn.run("ls ./dpdk-kmods", wd = "~")
                if ok.exited != 0:
                    # need to clone
                    ok = conn.run("git clone https://dpdk.org/git/dpdk-kmods", wd = "~")
                    check(ok, "clone dpdk-kmods", conn.addr)

                agenda.subtask("compiling igb_uio")
                # now can make
                ok = conn.run("make", wd = "~/dpdk-kmods/linux/igb_uio")
                check(ok, "build igb_uio", conn.addr)

            agenda.subtask("inserting igb_uio kmod")
            ok = conn.run("insmod ./igb_uio.ko", wd = "~/dpdk-kmods/linux/igb_uio", sudo=True)
            check(ok, "modprobe igb_uio", conn.addr)

def get_iface(ip_link_out, mac):
    for line in ip_link_out.split('\n'):
        if mac in line:
            return line.split()[1][:-1]
    raise Exception(f"interface for {ip_link_out}/{mac} not found")

def mlx_ofed_install_one(conn):
    agenda.task(f"checking for mlx ofed install on {conn.addr}")
    res = conn.run(f"ofed_info", wd="~/burrito/dpdk-direct/dpdk-wrapper/dpdk", sudo=True)
    if res.exited == 0:
        # already installed
        agenda.subtask("mlx ofed already installed")
        return

    agenda.task(f"installing mlx ofed on {conn.addr}")
    res = conn.run("./scripts/install-mlx5.sh ~/burrito", sudo=True, wd="~/burrito")
    if res.exited != 0:
        agenda.failure(f"mlx ofed install failed:\n{res.stdout}\n{res.stderr}")
        global thread_ok
        thread_ok = False
    agenda.subtask("mlx ofed installed")

def mlx_ofed_install(machines):
    installs = [threading.Thread(target=mlx_ofed_install_one, args=(m,)) for m in machines]
    [i.start() for i in installs]
    [i.join() for i in installs]
    if not thread_ok:
        raise Exception("mlx install failed")

def intel_devbind(machines, datapath):
    agenda.task(f"bind interfaces to correct driver for {datapath}")
    for conn in machines:
        # first determine the current driver assignment
        res = conn.run(f"./usertools/dpdk-devbind.py --status-dev net | grep 'drv=igb_uio'", wd="~/burrito/dpdk-direct/dpdk-wrapper/dpdk", sudo=True)
        if res.exited != 0 and 'dpdk' in datapath:
            agenda.subtask(f"[{conn.addr}] bind to DPDK driver")
            # Kernel -> DPDK
            # 0. get the iface and pci_addr
            res = conn.run("ip -o link")
            check(res, "ip link", conn.addr)
            ip_link_out = res.stdout
            iface = get_iface(ip_link_out, conn.mac)
            conn.exp_iface = iface
            res = conn.run(f"./usertools/dpdk-devbind.py --status-dev net | grep '{iface}' | cut -d' ' -f1", wd="~/burrito/dpdk-direct/dpdk-wrapper/dpdk", sudo=True)
            check(res, "dpdk-devbind show", conn.addr)
            pci_addr = res.stdout.strip()
            agenda.subtask(f"[{conn.addr}] pci: {pci_addr} iface: {iface}")

            # 1. ip link set dev {interface} down
            ok = conn.run(f"ip link set dev {iface} down", sudo=True)
            check(ok, "ip link set down", conn.addr)

            # 2. dpdk-devbind --bind igb_uio {pci_addr}
            ok = conn.run(f"./usertools/dpdk-devbind.py --bind=\"igb_uio\" {pci_addr}", wd="~/burrito/dpdk-direct/dpdk-wrapper/dpdk", sudo=True)
            check(ok, "dpdk-devbind bind", conn.addr)
        elif res.exited == 0 and datapath == 'kernel':
            agenda.subtask(f"[{conn.addr}] bind to kernel driver")
            # DPDK -> Kernel
            pci_addr = res.stdout.strip().split()[0]
            # 1. dpdk-devbind --bind i40e {pci_addr}
            ok = conn.run(f"./usertools/dpdk-devbind.py --bind=\"i40e\" {pci_addr}", wd="~/burrito/dpdk-direct/dpdk-wrapper/dpdk", sudo=True)
            check(ok, "dpdk-devbind bind", conn.addr)

            # 2. ip link set dev {interface} up
            res = conn.run("ip -o link")
            check(res, "ip link", conn.addr)
            ip_link_out = res.stdout
            iface = get_iface(ip_link_out, conn.mac)
            conn.exp_iface = iface

            ok = conn.run(f"ip link set dev {iface} up", sudo=True)
            check(ok, "ip link set up", conn.addr)

            # 3. ip addr add dev {interface} {addr}/24
            ok = conn.run(f"ip addr add dev {iface} {conn.addr}/24", sudo=True)
            check(ok, "ip addr add", conn.addr)
        else:
            agenda.subtask("correct driver already bound")
        agenda.subtask(f"[{conn.addr}] done")

def connect_machines(cfg):
    agenda.task(f"Checking for connection vs experiment ip")
    ips = [cfg['machines']['server']] + cfg['machines']['clients']
    agenda.task(f"connecting to {ips}")
    machines, commits = zip(*[check_machine(ip) for ip in ips])
    # check all the commits are equal
    if not all(c == commits[0] for c in commits):
        agenda.subfailure(f"not all commits equal: {commits}")
        raise Exception("Commits mismatched on machines")
    return machines

def setup_all(machines, cfg, args, setup_fn, compile_fn):
    global thread_ok
    thread_ok = True

    target_dir = None
    if args.cloudlab:
        res = machines[0].run("ls /proj/")
        target_dir = f"/proj/{res.stdout.strip().split()[0]}"

    fn_args = (args.outdir, cfg['exp']['datapath'] if 'datapath' in cfg['exp'] else [], args.dpdk_driver, target_dir)
    agenda.task("setup machines")

    setups = [threading.Thread(target=setup_fn, args=(m, *fn_args)) for m in machines]
    [t.start() for t in setups]
    [t.join() for t in setups]
    if not thread_ok:
        agenda.failure("Something went wrong")
        raise Exception("setup error")

    if 'intel' == args.dpdk_driver and 'datapath' in cfg['exp'] and any('dpdk' in d for d in cfg['exp']['datapath']):
        intel_setup(machines)

    if 'mlx' in args.dpdk_driver:
        mlx_ofed_install(machines)

    agenda.task("build binaries")
    if args.cloudlab:
        compile_fn(machines[0], *fn_args)
    else:
        setups = [threading.Thread(target=compile_fn, args=(m, *fn_args)) for m in machines]
        [t.start() for t in setups]
        [t.join() for t in setups]
        if not thread_ok:
            agenda.failure("Something went wrong")
            raise Exception("setup error")
    agenda.task("done building")

    ms = ([cfg['machines']['server']] if not type(cfg['machines']['server']) == list else cfg['machines']['server']) + cfg['machines']['clients']
    for m in ms:
        for x in machines:
            if x.addr == m['exp']:
                x.mac = m['mac']
                break

    # copy config file to outdir
    shutil.copy2(args.config, args.outdir)

    global dpdk_driver
    dpdk_driver = args.dpdk_driver
    if 'intel' == args.dpdk_driver:
        intel_devbind(machines, 'kernel')

    if any('shenango' in d or 'dpdk' in d for d in cfg['exp']['datapath']):
        for m in machines:
            pci_addr = get_pci_addr(m)
            m.pci_addr = pci_addr

    agenda.task("writing configuration files")
    if any('shenango' in d for d in cfg['exp']['datapath']):
        for m in machines:
            lcores = cfg['cfg']['lcores'].split(',')
            agenda.subtask(f"shenango config num_threads={lcores}")
            #write_shenango_config(m, max(len(lcores)-1, 2))
            write_shenango_config(m, 8)
    if any('dpdk' in d for d in cfg['exp']['datapath']):
        for m in machines:
            write_dpdk_config(m, machines, cfg['cfg']['lcores'])

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
### wrk = ["./kvstore-ycsb/ycsbc-mock/wrkloadbunf1-4.access"]
### datapath = ['shenango_channel', 'dpdkmulti', 'dpdkthread', 'kernel']
### load = [10000, 20000, 40000, 60000, 80000, 100000, 120000, 140000, 160000, 180000, 200000]
### poisson-arrivals = [true]
### shardtype = ["client"]
### shards = [4]
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
        if args.dpdk_driver not in ['mlx4', 'mlx5', 'intel']:
            agenda.failure("If using dpdk datapath, must specify mlx4, mlx5, or intel driver.")

    if 'negotiation' not in cfg['exp']:
        cfg['exp']['negotiation'] = [True]
    for t in cfg['exp']['negotiation']:
        if t not in [True,False]:
            agenda.failure("Skip-negotiation must be bool")
            sys.exit(1)

    if 'no-chunnels' not in cfg['exp']:
        agenda.subfailure("no-chunnels not found, using default false")
        cfg['exp']['no-chunnels'] = [False]
    for t in cfg['exp']['no-chunnels']:
        if t not in [True,False,'full','conns','off']:
            agenda.failure("invalid no-chunnels value")
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
    setup_all(machines, cfg, args, setup_machine, compile_binaries)

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

        for noch in cfg['exp']['no-chunnels']:
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
                                        overwrite=args.overwrite,
                                        cfg_client=cfg['cfg']['client'],
                                        no_chunnels=noch,
                                        cloudlab=args.cloudlab,
                                    )

    agenda.task("done")
