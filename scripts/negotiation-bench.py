#!/usr/bin/python3

import subprocess as sh
import sys

outdir = sys.argv[1]

sh.run(f'mkdir -p {outdir}', shell=True)

cmd = "target/release/negotiation-bench -n1000 -m {} -o {}"

commit = sh.run("git rev-parse --short HEAD", shell=True, capture_output=True)
commit = commit.stdout.decode('utf8').strip()
outfile = f"{outdir}/{commit}-negbench.data"
print(outfile)

for mode in ['0r', '1r', 'rd:"redis://127.0.0.1:6379"', 'b']:
    print(mode, outfile)
    sh.run(cmd.format(mode, outfile), env = { 'RUST_LOG': 'info' }, shell=True)
