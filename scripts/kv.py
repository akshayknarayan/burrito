#!/usr/bin/python3

from fabric import Connection
import agenda
import argparse
import os
import subprocess
import sys
import threading
import time

class ConnectionWrapper(Connection):
    def __init__(self, addr, user=None, port=None):
        super().__init__(
            addr,
            forward_agent=True,
            user=user,
            port=port,
        )
        self.addr = addr

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
    def run(self, cmd, *args, stdin=None, stdout=None, stderr=None, ignore_out=False, wd=None, sudo=False, background=False, pty=True, **kwargs):
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

def check(ok, msg, addr, allowed=[]):
    # exit code 0 is always ok, allowed is in addition
    if ok.exited != 0 and ok.exited not in allowed:
        agenda.subfailure(f"{msg} on {addr}: {ok.exited} not in {allowed}")
        agenda.subfailure("stdout")
        print(ok.stdout)
        agenda.subfailure("stderr")
        print(ok.stderr)
        sys.exit(1)

def check_machine(ip):
    conn = ConnectionWrapper(ip)
    if not conn.file_exists("~/burrito"):
        agenda.failure(f"No burrito on {ip}")
        sys.exit(1)

    commit = conn.run("git rev-parse --short HEAD", wd = "~/burrito")
    if commit.exited == 0:
        commit = commit.stdout.strip()
    agenda.subtask(f"burrito commit {commit} on {ip}")
    return (conn, commit)

def setup(conn, outdir):
    ok = conn.run(f"mkdir -p ~/burrito/{outdir}")
    check(ok, "mk outdir", conn.addr)
    agenda.subtask(f"building burrito on {conn.addr}")
    ok = conn.run("~/.cargo/bin/cargo build --release", wd = "~/burrito")
    check(ok, "build", conn.addr)
    return conn

def start_server(conn, outf, shards=0, use_burrito_shard=True):
    conn.run("sudo pkill -9 kvserver")

    #ok = conn.run("cset shield --userset=kv --reset", sudo=True)
    #check(ok, "reset cpuset", conn.addr, allowed = [2])
    #cpus = max(3, shards + 1)
    #agenda.subtask(f"make cpu shield kv for cpus 0-{cpus} on {conn.addr}")
    #ok = conn.run(f"cset shield --userset=kv --cpu=0-{cpus} --kthread=on", sudo=True)
    #check(ok, "make cpuset", conn.addr)

    shard_arg = f"-n {shards}" if shards > 0 else ""
    burrito_shard_arg = f"" if use_burrito_shard else "--no-shard-ctl"
    ok = conn.run(f"./target/release/kvserver -i {conn.addr} -p 4242 {shard_arg} {burrito_shard_arg}",
            wd="~/burrito",
            sudo=True,
            background=True,
            stdout=f"{outf}.out",
            stderr=f"{outf}.err",
            )
    #ok = conn.run(f"cset shield --userset=kv --exec ./target/release/kvserver -- -i {conn.addr} -p 4242 {shard_arg}",
    #        wd="~/burrito",
    #        sudo=True,
    #        background=True,
    #        stdout=f"{outf}.out",
    #        stderr=f"{outf}.err",
    #        )
    check(ok, "spawn server", conn.addr)
    time.sleep(2)
    conn.check_proc("kvserver", f"{outf}.err")

# one machine can handle 2 client processes
def run_client(conn, server, interarrival, outf, clientsharding=0):
    if not conn.file_exists("~/burrito/kvstore-ycsb/ycsbc-mock/wrkloadb1-100.access"):
        agenda.failure(f"No ycsb trace on {conn.addr}")
        sys.exit(1)
    if not conn.file_exists("~/burrito/kvstore-ycsb/ycsbc-mock/wrkloadb2-100.access"):
        agenda.failure(f"No ycsb trace on {conn.addr}")
        sys.exit(1)

    if clientsharding > 4:
        agenda.failure(f"Not enough cores for 2x {clientsharding} client shards")

    ok = conn.run("cset shield --userset=kv --reset", sudo=True)
    check(ok, "reset cpuset", conn.addr, allowed = [2])
    cpus = max(3, clientsharding + 1)
    agenda.subtask(f"make cpu shield kv for cpus 0-{cpus} on {conn.addr}")
    ok = conn.run(f"cset shield --userset=kv --cpu=0-{cpus} --kthread=on", sudo=True)
    check(ok, "make cpuset", conn.addr)

    shard_arg = f"-n {clientsharding}" if clientsharding > 0 else ""

    conn.run(f"cset shield --userset=kv --exec ./target/release/ycsb -- \
            --burrito-root=/tmp/burrito \
            --addr \"kv\" \
            --accesses ./kvstore-ycsb/ycsbc-mock/wrkloadb1-100.access \
            -o {outf}.data \
            {shard_arg} \
            -i {interarrival}",
            wd = "~/burrito",
            sudo = True,
            background = True,
            stdout=f"{outf}.out",
            stderr=f"{outf}.err",
            )
    ok = conn.run(f"./target/release/ycsb \
            --burrito-root=/tmp/burrito \
            --addr \"kv\" \
            --accesses ./kvstore-ycsb/ycsbc-mock/wrkloadb2-100.access \
            -o {outf}2.data \
            {shard_arg} \
            -i {interarrival}",
            wd = "~/burrito",
            stdout=f"{outf}2.out",
            stderr=f"{outf}2.err",
            )
    check(ok, "ycsb client", conn.addr)
    # wait for the other one
    while True:
        ok = conn.run(f"ls {outf}.data", wd="~/burrito")
        if ok.exited == 0:
            break
        else:
            time.sleep(1)

    conn.run("sudo chmod 644 {outf}.data")
    conn.run("sudo chmod 644 {outf}.out")
    conn.run("sudo chmod 644 {outf}.err")

def start_burrito_shard_ctl(machines, outdir):
    for m in machines:
        m.run("sudo pkill -INT burrito-shard")
        m.run("rm -rf /tmp/burrito/*", sudo=True)
    machines[0].run("DOCKER_HOST=unix:///var/run/burrito-docker.sock docker rm -f burrito-shard-redis", sudo=True)

    agenda.task("Starting burrito-shard-ctl")
    # redis runs on first machine
    ok = machines[0].run("DOCKER_HOST=unix:///var/run/burrito-docker.sock \
            docker run \
            --name burrito-shard-redis \
            -d -p 6379:6379 redis:5",
            sudo=True,
            stdout=f"{outdir}-docker-redis.out",
            stderr=f"{outdir}-docker-redis.err",
            wd="~/burrito")
    check(ok, "start redis", machines[0].addr)
    ok = machines[0].run("DOCKER_HOST=unix:///var/run/burrito-docker.sock docker ps | grep burrito-shard-redis", sudo=True)
    check(ok, "start redis", machines[0].addr)

    agenda.subtask(f"Started redis on {machines[1].addr}")

    machines[0].run(
        "./target/release/burrito-shard -r redis://localhost:6379",
        wd="~/burrito", background=True, stderr=f"{outdir}-{machines[0].addr}.err")
    agenda.subtask(f"Started burrito-shard-ctl on {machines[0].addr}")
    redis_addr = machines[0].addr

    # burrito-shard-ctl runs on all
    for m in machines[1:]:
        m.run(f"./target/release/burrito-shard -r redis://{redis_addr}:6379",
            wd="~/burrito", background=True, stderr=f"{outdir}-{m.addr}.err")
        agenda.subtask(f"Started burrito-shard-ctl on {m.addr}")

def do_exp(outdir, machines, num_shards, shardtype, ops_per_sec):
    server_prefix = f"{outdir}/{num_shards}-{shardtype}shard-{ops_per_sec}-kvserver"
    outf = f"{outdir}/{num_shards}-{shardtype}shard-{ops_per_sec}-ycsb"

    for m in machines:
        m.run(f"mkdir -p {outdir}", wd="~/burrito")

    if os.path.exists(f"{outf}-{machines[1].addr}.data"):
        agenda.task(f"skipping: server = {machines[0].addr}, num_shards = {num_shards}, shardtype = {shardtype}, load = {ops_per_sec} ops/s")
        return

    # load = 100 (client threads / proc) * 2 (procs/machine) * {len(machines) - 1} (machines) / {interarrival} (per client thread)
    # interarrival = 100 * 2 * {len(machines) - 1} / load
    interarrival_secs = 200 * len(machines[1:]) / ops_per_sec
    interarrival_us = int(interarrival_secs * 1e6)

    start_burrito_shard_ctl(machines, f"{outdir}/{num_shards}-{shardtype}shard-{ops_per_sec}-burrito-shard")

    server_addr = machines[0].addr
    agenda.task(f"starting: server = {server_addr}, num_shards = {num_shards}, shardtype = {shardtype}, load = {ops_per_sec} ops/s -> interarrival_us = {interarrival_us}")

    # first one is the server, start the server
    agenda.subtask("starting server")
    if shardtype == "xdpserver":
        start_server(machines[0], server_prefix, shards=num_shards)
    elif shardtype == "client" or shardtype == "server":
        start_server(machines[0], server_prefix, shards=num_shards, use_burrito_shard=False)
    else:
        assert(False)

    # others are clients
    agenda.subtask("starting clients")
    if shardtype == "client":
        clients = [threading.Thread(target=run_client, args=(m, server_addr, interarrival_us, outf), kwargs={'clientsharding':num_shards}) for m in machines[1:]]
    elif shardtype == "xdpserver" or shardtype == "server":
        clients = [threading.Thread(target=run_client, args=(m, server_addr, interarrival_us, outf)) for m in machines[1:]]
    else:
        assert(False)

    [t.start() for t in clients]
    [t.join() for t in clients]
    agenda.task("done")

    # kill the server
    machines[0].run("sudo pkill -9 kvserver")

    machines[0].get(f"burrito/{server_prefix}.out", local=f"{server_prefix}.out", preserve_mode=False)
    machines[0].get(f"burrito/{server_prefix}.err", local=f"{server_prefix}.err", preserve_mode=False)
    for c in machines[1:]:
        if c.addr in ['127.0.0.1', '::1', 'localhost']:
            c.run(f"mv burrito/{outf}.data burrito/{outf}-{c.addr}.data1")
            c.run(f"mv burrito/{outf}2.data burrito/{outf}2-{c.addr}.data1")
            continue
        c.get(f"burrito/{outf}.err", local=f"{outf}-{c.addr}.err", preserve_mode=False)
        c.get(f"burrito/{outf}2.err", local=f"{outf}2-{c.addr}.err", preserve_mode=False)
        c.get(f"burrito/{outf}.out", local=f"{outf}-{c.addr}.out", preserve_mode=False)
        c.get(f"burrito/{outf}2.out", local=f"{outf}2-{c.addr}.out", preserve_mode=False)
        c.get(f"burrito/{outf}.data", local=f"{outf}-{c.addr}.data1", preserve_mode=False)
        c.get(f"burrito/{outf}2.data", local=f"{outf}2-{c.addr}.data1", preserve_mode=False)

    # make sure *.data1 is there
    ok = subprocess.run(f"ls {outf}-{c.addr}.data1")
    if not ok.returncode == 0:
        agenda.failure(f"Could not get ycsb output {outf}-{c.addr}.data1")
    ok = subprocess.run(f"ls {outf}2-{c.addr}.data1")
    if not ok.returncode == 0:
        agenda.failure(f"Could not get ycsb output {outf}2-{c.addr}.data1")

    for c in machines[1:]:
        agenda.subtask(f"adding experiment info for {c.addr}")
        subprocess.run(f"awk '{{if (!hdr) {{hdr=$0; print \"ShardType NumShards \"$0;}} else {{print \"{shardtype}shard {num_shards}shards \"$0}} }}' {outf}-{c.addr}.data1 > {outf}-{c.addr}.data", shell=True)
        subprocess.run(f"awk '{{if (!hdr) {{hdr=$0; print \"NumShards \"$0;}} else {{print \"{shardtype}shard {num_shards}shards \"$0}} }}' {outf}2-{c.addr}.data1 > {outf}2-{c.addr}.data", shell=True)


parser = argparse.ArgumentParser()
parser.add_argument('--outdir', type=str, required=True)
parser.add_argument('--load', type=int, action='append', required=True)
parser.add_argument('--server', type=str, required=True)
parser.add_argument('--client', type=str, action='append', required=True)
parser.add_argument('--shards', type=int, action='append')
parser.add_argument('--shardtype', type=str, action='append')

if __name__ == '__main__':
    args = parser.parse_args()
    outdir = args.outdir
    ops_per_sec = args.load
    ips = [args.server] + args.client

    if len(args.client) < 1:
        agenda.failure("Need more machines")
        sys.exit(1)

    if args.shards is None:
        args.shards = [1]

    if args.shardtype is None:
        args.shardtype = ['server']
    for t in args.shardtype:
        if t not in ['client', 'server', 'xdpserver']:
            agenda.failure(f"Unknown shardtype {t}")
            sys.exit(1)

    agenda.task(f"connecting to {ips}")
    machines, commits = zip(*[check_machine(ip) for ip in ips])
    # check all the commits are equal
    if not all(c == commits[0] for c in commits):
        agenda.subfailure(f"not all commits equal: {commits}")
        sys.exit(1)

    # build
    agenda.task("building burrito...")
    setups = [threading.Thread(target=setup, args=(m,outdir)) for m in machines]
    [t.start() for t in setups]
    [t.join() for t in setups]
    agenda.task("...done building burrito")

    for s in args.shards:
        for t in args.shardtype:
            for o in ops_per_sec:
                do_exp(outdir, machines, s, t, o)
                time.sleep(5)

    agenda.task("done")
    #subprocess.run(f"rm -f {outdir}/*.data1", shell=True)
    #subprocess.run(f"rm -f {outdir}/*.data2", shell=True)
    subprocess.run(f"tail ./{outdir}/*ycsb*err >> {outdir}/exp.log", shell=True)
    subprocess.run(f"cat ./{outdir}/*ycsb*.data | awk '{{if (!hdr) {{hdr=$0; print $0;}} else if (hdr != $0) {{print $0}};}}' > {outdir}/exp.data", shell=True)
    agenda.task("plotting")
    subprocess.run(f"./scripts/kv.R {outdir}/exp.data {outdir}/exp.pdf", shell=True)
