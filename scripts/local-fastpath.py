#!/usr/bin/python3

import time
import sys
import agenda
import subprocess as sh
import argparse

def start_localnamectl(srv_addr, burrito_root):
    sp = srv_addr.split(":")
    srv_addr = sp[0]
    if burrito_root is not None:
        cmd = f"sudo RUST_LOG=debug ./target/release/burrito-localname -f &"
        if srv_addr == "127.0.0.1":
            agenda.task("starting localname-ctl")
            sh.run(cmd, shell=True)
        else:
            agenda.task(f"no localname-ctl for {srv_addr}")

# server:
# RUST_LOG=debug,tls_tunnel=trace ./target/release/bincode-pingserver -p 4242 --encr-ghostunnel-root ~/ghostunnel
def start_server(srv_addr, srv_port, ghostunnel, burrito_root):
    sp = srv_addr.split(":")
    srv_addr = sp[0]
    encr_arg = f"{ghostunnel}" if ghostunnel is not None else ""
    burrito_root_arg = f"--burrito-root=/burrito" if burrito_root is not None else ""
    if srv_addr == '127.0.0.1':
        agenda.task("local rpcbench-server")
        if ghostunnel is not None:
            cmd = f"./scripts/start-rpcbench-server-encr.sh ./target/release {srv_port} {encr_arg} {burrito_root_arg} &"
        else:
            cmd = f"./scripts/start-rpcbench-server-noencr.sh ./target/release {srv_port} {burrito_root_arg} &"
    else:
        agenda.task("remote rpcbench-server")
        agenda.subtask("copying binary + script")
        sh.run(f"scp ./target/release/bincode-pingserver {srv_addr}:", shell=True)
        if ghostunnel is not None:
            sh.run(f"scp ./scripts/start-rpcbench-server-encr.sh {srv_addr}:", shell=True)
            cmd = f"ssh {srv_addr} ./start-rpcbench-server-encr.sh . {srv_port} {encr_arg} {burrito_root_arg} &"
        else:
            sh.run(f"scp ./scripts/start-rpcbench-server-noencr.sh {srv_addr}:", shell=True)
            cmd = f"ssh {srv_addr} ./start-rpcbench-server-noencr.sh . {srv_port} {burrito_root_arg} &"

    agenda.subtask("running launch script")
    sh.run(cmd, shell=True)
    agenda.subtask("launched")

def exp(srv_addr, mode, args):
    sp = srv_addr.split(":")
    srv_addr = sp[0]
    if len(sp) == 2:
        exp_addr = sp[1]
    else:
        exp_addr = srv_addr
    encr_arg = f"{args.ghostunnel}" if args.ghostunnel and mode != 'rel'  else "none"
    burrito_root_arg = f"--burrito-root=/burrito" if args.burrito_root and mode == 'fp' else "none"
    is_local = 'local' if '127.0.0.1' == exp_addr else 'remote'
    outfile_arg = f"{is_local}-mode:{mode}-msgs:{args.reqs}-perconn:{args.perconn}"
    addr_arg = f"{args.server_port}" if is_local == 'local' else f"{exp_addr}:{args.server_port}"
    cmd = f"./scripts/run-rpcbench-client.sh \
        {args.outdir} \
        {addr_arg} \
        -i={args.reqs} \
        --reqs-per-iter={args.perconn} \
        {outfile_arg} \
        {encr_arg} \
        {burrito_root_arg} \
        "
    agenda.task(f"run client: mode {mode} \
        {exp_addr}:{args.server_port} ({is_local}), \
        {args.reqs} reqs, \
        {args.perconn} /conn, \
        encrypt {encr_arg != 'none'}, \
        burrito {burrito_root_arg != 'none'}")
    agenda.subtask(f"outfile: {outfile_arg}")
    sh.run(cmd, shell=True)

parser = argparse.ArgumentParser()
parser.add_argument('--outdir', type=str, required=True)
parser.add_argument('--server', type=str, action='append', required=True)
parser.add_argument('--server-port', type=int, required=True)
parser.add_argument('--ghostunnel', type=str)
parser.add_argument('--reqs', type=int, required=True)
parser.add_argument('--perconn', type=int, required=True)
parser.add_argument('--burrito-root', type=str)
parser.add_argument('--mode', type=str, action='append', choices=['encr', 'rel', 'fp', 'all'])

args = parser.parse_args()
if 'all' in args.mode:
    args.mode = ['encr', 'rel', 'fp']

for srv in args.server:
    is_remote = '127.0.0.1' in srv
    for m in args.mode:
        if m not in ['encr', 'rel', 'fp']:
            agenda.failure(f"unknown mode {m}")
            break
        if m != 'rel' and args.ghostunnel is None:
            agenda.failure("need ghostunnel arg for non-rel exp")
            break
        agenda.task(f"mode: {m}")
        start_localnamectl(srv, args.burrito_root if m == 'fp' else None)
        start_server(
            srv,
            args.server_port,
            args.ghostunnel if m != 'rel' else None,
            args.burrito_root if m == 'fp' else None)
        time.sleep(30)
        exp(srv, m, args)
