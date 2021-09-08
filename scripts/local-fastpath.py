#!/usr/bin/python3

import os
import time
import sys
import agenda
import subprocess as sh
import shlex
import argparse

def start_localnamectl(srv_addr, burrito_root):
    sp = srv_addr.split(":")
    srv_addr = sp[0]
    if burrito_root is not None:
        cmd = f"RUST_LOG=warn ./target/release/burrito-localname -f &"
        if srv_addr == "127.0.0.1":
            agenda.task("starting localname-ctl")
            sh.run(cmd, shell=True)
        else:
            agenda.task(f"no localname-ctl for {srv_addr}")

# server:
# RUST_LOG=debug,tls_tunnel=trace ./target/release/bincode-pingserver -p 4242 --encr-ghostunnel-root ~/ghostunnel
def start_server(srv_addr, srv_port, ghostunnel, burrito_root, n):
    agenda.subtask(f"encr arg: {ghostunnel}")
    agenda.subtask(f"bertha arg: {burrito_root}")
    agenda.subtask(f"neg arg: {n}")
    neg = '' if n != 'off' else '--no-negotiation'
    sp = srv_addr.split(":")
    srv_addr = sp[0]
    encr_arg = f"{ghostunnel}" if ghostunnel is not None else "none"
    burrito_root_arg = f"--burrito-root=/tmp/burrito" if burrito_root is not None else ""
    if srv_addr == '127.0.0.1':
        agenda.task("local rpcbench-server")
        cmd = f"./scripts/start-rpcbench-server.sh \
            ./target/release \
            {srv_port} \
            {encr_arg} \
            {burrito_root_arg} \
            {neg} \
            &"
    else:
        agenda.task("remote rpcbench-server")
        agenda.subtask("copying binary + script")
        sh.run(f"scp ./target/release/bincode-pingserver {srv_addr}:", shell=True)
        sh.run(f"scp ./scripts/start-rpcbench-server.sh {srv_addr}:", shell=True)
        cmd = f"ssh {srv_addr} ./start-rpcbench-server.sh \
            . \
            {srv_port} \
            '{encr_arg}' \
            {burrito_root_arg} \
            {neg} \
            &"

    agenda.subtask("running launch script")
    agenda.subtask(cmd)
    sh.run(cmd, shell=True)
    agenda.subtask("launched")

def start_server_unix(neg_arg):
    agenda.task("local rpcbench-server unix")
    neg = '' if neg_arg != 'off' else '--no-negotiation'
    cmd = f"./scripts/start-rpcbench-unix-server.sh ./target/release {neg} &"
    agenda.subtask("running launch script")
    sh.run(cmd, shell=True)
    agenda.subtask("launched")

def exp(srv_addr, mode, args, n):
    sp = srv_addr.split(":")
    srv_addr = sp[0]
    if len(sp) == 2:
        exp_addr = sp[1]
    else:
        exp_addr = srv_addr
    neg = f'--negotiation={n}'
    encr_arg = f"{args.ghostunnel}" if args.ghostunnel and 'rel' not in mode else "none"
    burrito_root_arg = f"--burrito-root=/tmp/burrito" if args.burrito_root and 'fp' in mode else "none"
    if '127.0.0.1' == exp_addr:
        is_local = 'local'
    elif '10.1' in exp_addr:
        is_local = 'remote'
    else:
        is_local = 'farther'
    outfile_arg = f"{is_local}-mode:{mode}-msgs:{args.reqs}-perconn:{args.perconn}-neg:{n}"
    if '@' in exp_addr:
        exp_addr = exp_addr.split('@')[-1]
    addr_arg = f"{args.server_port}" if is_local == 'local' else f"{exp_addr}:{args.server_port}"
    cmd = f"./scripts/run-rpcbench-client.sh \
        {args.outdir} \
        {addr_arg} \
        -i={args.reqs} \
        --reqs-per-iter={args.perconn} \
        {outfile_arg} \
        {encr_arg} \
        {burrito_root_arg} \
        {neg} \
        "
    agenda.task(f"run client: mode {mode} \
        {exp_addr}:{args.server_port} ({is_local}), \
        {args.reqs} reqs, \
        {args.perconn} /conn, \
        encrypt {encr_arg != 'none'}, \
        negotiation {neg} \
        burrito {burrito_root_arg != 'none'}")
    agenda.subtask(f"outfile: {outfile_arg}")
    sh.run(cmd, shell=True)

def exp_unix(args, n):
    neg = f'--negotiation={n}'
    outfile_arg = f"local-mode:rel-ux-msgs:{args.reqs}-perconn:{args.perconn}-neg:{n}"
    cmd = f"./scripts/run-rpcbench-unix-client.sh \
        {args.outdir} \
        -i={args.reqs} \
        --reqs-per-iter={args.perconn} \
        {outfile_arg} \
        {neg}"
    agenda.subtask(f"outfile: {outfile_arg}")
    sh.run(cmd, shell=True)

parser = argparse.ArgumentParser()
parser.add_argument('--outdir', type=str, required=True)
parser.add_argument('--server', type=str, action='append', required=True)
parser.add_argument('--server-port', type=int, required=True)
parser.add_argument('--ghostunnel', type=str)
neg_opts = ['off', 'one', 'zero']
parser.add_argument('--negotiate', type=str, action='append', choices=neg_opts + ['all'])
parser.add_argument('--reqs', type=int, required=True)
parser.add_argument('--perconn', type=int, required=True)
parser.add_argument('--burrito-root', type=str)
modes = ['rel-ux', 'encr', 'rel', 'fp', 'fp-rel']
parser.add_argument('--mode', type=str, action='append', choices=modes + ['all'])

args = parser.parse_args()
if 'all' in args.mode:
    args.mode = modes

if 'all' in args.negotiate:
    args.negotiate = neg_opts

os.makedirs(args.outdir, exist_ok = True)

for n in args.negotiate:
    for srv in args.server:
        is_remote = '127.0.0.1' not in srv
        for m in args.mode:
            if m not in modes:
                agenda.failure(f"unknown mode {m}")
                break
            if m != 'rel' and args.ghostunnel is None:
                agenda.failure("need ghostunnel arg for non-rel exp")
                break
            agenda.task(f"mode: {m}, negotiate {n}")
            if is_remote and m == 'rel-ux':
                agenda.subfailure("No remote for unix mode")
                continue
            start_localnamectl(srv, args.burrito_root if 'fp' in m else None)
            if m == 'rel-ux':
                start_server_unix(n)
                time.sleep(15)
                exp_unix(args, n)
            else:
                start_server(
                    srv,
                    args.server_port,
                    args.ghostunnel if 'rel' not in m else None,
                    args.burrito_root if 'fp' in m else None,
                    n)
                agenda.task("waiting for server")
                time.sleep(15)
                agenda.subtask("waited")
                exp(srv, m, args, n)
            agenda.task("done")
