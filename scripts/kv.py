#!/usr/bin/python3

from fabric import Connection
import agenda
import argparse
import os
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
    ok = conn.run("cset shield --userset=kv --reset", sudo=True)
    check(ok, "reset cpuset", conn.addr, allowed = [2])
    agenda.subtask(f"building burrito on {conn.addr}")
    ok = conn.run("~/.cargo/bin/cargo build --release", wd = "~/burrito")
    check(ok, "build", conn.addr)
    agenda.subtask(f"make cpu shield kv for cpus 0-3 on {conn.addr}")
    ok = conn.run("cset shield --userset=kv --cpu=0-3 --kthread=on", sudo=True)
    check(ok, "make cpuset", conn.addr)
    return conn

def start_server(conn, outf):
    conn.run("sudo pkill -9 kvserver")
    ok = conn.run("cset shield --userset=kv --exec ./target/release/kvserver -- -p 4242",
            wd="~/burrito",
            sudo=True,
            background=True,
            stdout=f"{outf}.out",
            stderr=f"{outf}.err",
            )
    check(ok, "spawn server", conn.addr)
    time.sleep(2)
    conn.check_proc("kvserver", f"{outf}.err")

# one machine can handle 2 client processes
def run_client(conn, server, interarrival, outf):
    if not conn.file_exists("~/burrito/kvstore-ycsb/ycsbc-mock/wrkloadb-100.access"):
        agenda.failure(f"No ycsb trace on {ip}")
        sys.exit(1)
    if not conn.file_exists("~/burrito/kvstore-ycsb/ycsbc-mock/wrkloadb2-100.access"):
        agenda.failure(f"No ycsb trace on {ip}")
        sys.exit(1)

    conn.run(f"cset shield --userset=kv --exec ./target/release/ycsb -- \
            --addr \"http://{server}:4242\" \
            --accesses ./kvstore-ycsb/ycsbc-mock/wrkloadb-100.access \
            -o {outf}.data \
            -i {interarrival}",
            wd = "~/burrito",
            sudo = True,
            background = True,
            stdout=f"{outf}.out",
            stderr=f"{outf}.err",
            )
    ok = conn.run(f"./target/release/ycsb \
            --addr \"http://{server}:4242\" \
            --accesses ./kvstore-ycsb/ycsbc-mock/wrkloadb2-100.access \
            -o {outf}2.data \
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

def do_exp(outdir, machines, ops_per_sec):
    # load = 100 (client threads / proc) * 2 (procs/machine) * {len(machines) - 1} (machines) / {interarrival} (per client thread)
    # interarrival = 100 * 2 * {len(machines) - 1} / load
    interarrival_secs = 200 * len(machines[1:]) / ops_per_sec
    interarrival_us = int(interarrival_secs * 1e6)

    # first one is the server, start the server
    server_addr = machines[0].addr
    agenda.task("starting server")
    start_server(machines[0], f"{outdir}/{ops_per_sec}-kvserver")

    # others are clients
    agenda.task(f"starting clients, server = {server_addr}, load = {ops_per_sec} ops/s -> interarrival_us = {interarrival_us}")
    clients = [threading.Thread(target=run_client, args=(m, server_addr, interarrival_us, f"{outdir}/{ops_per_sec}-ycsb")) for m in machines[1:]]
    [t.start() for t in clients]
    [t.join() for t in clients]
    agenda.task("done")

    # kill the server
    machines[0].run("sudo pkill -9 kvserver")

    machines[0].get(f"burrito/{outdir}/kvserver.out", local=f"{outdir}/kvserver.out", preserve_mode=False)
    machines[0].get(f"burrito/{outdir}/kvserver.err", local=f"{outdir}/kvserver.err", preserve_mode=False)
    for c in machines[1:]:
        if c.addr in ['127.0.0.1', '::1', 'localhost']:
            continue
        c.get(f"burrito/{outdir}/ycsb.err", local=f"{outdir}/{c.addr}-ycsb.err", preserve_mode=False)
        c.get(f"burrito/{outdir}/ycsb2.err", local=f"{outdir}/{c.addr}-ycsb2.err", preserve_mode=False)
        c.get(f"burrito/{outdir}/ycsb.out", local=f"{outdir}/{c.addr}-ycsb.out", preserve_mode=False)
        c.get(f"burrito/{outdir}/ycsb2.out", local=f"{outdir}/{c.addr}-ycsb2.out", preserve_mode=False)
        c.get(f"burrito/{outdir}/ycsb.data", local=f"{outdir}/{c.addr}-ycsb.data", preserve_mode=False)
        c.get(f"burrito/{outdir}/ycsb2.data", local=f"{outdir}/{c.addr}-ycsb2.data", preserve_mode=False)

    import subprocess
    subprocess.run(f"tail ./{outdir}/*ycsb*err >> {outdir}/exp.log", shell=True)
    subprocess.run(f"tail ./{outdir}/*ycsb*.data >> {outdir}/exp.data", shell=True)

parser = argparse.ArgumentParser()
parser.add_argument('--outdir', type=str, required=True)
parser.add_argument('--load', type=int, action='append', required=True)
parser.add_argument('--server', type=str, required=True)
parser.add_argument('--client', type=str, action='append', required=True)

if __name__ == '__main__':
    args = parser.parse_args()
    outdir = args.outdir
    ops_per_sec = args.load
    ips = [args.server] + args.client

    if len(args.client) < 2:
        agenda.failure("Need more machines")
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

    for o in ops_per_sec:
        do_exp(outdir, machines, o)
