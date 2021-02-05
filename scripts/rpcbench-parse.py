#!/usr/bin/python3

import sys
import re

file_re = re.compile(r"work_(.+)_([0-9]+)-iters_([0-9]+)_periter_([0-9]+)_(\S+?)_(\w+)_(\w+)\.data")
def parse_filename(fn: str):
    m = file_re.search(fn)
    if m is None:
        print(fn)
    return {
        'work_type': m[1],
        'work_amount': m[2],
        'iters': m[3],
        'reqs_per_iter': m[4],
        'proto': m[5],
        'machine': m[6],
        'container': m[7],
    }

def read_file(fn: str, first: bool):
    exp = parse_filename(fn)
    file_first = True
    with open(fn) as f:
        for line in f:
            if file_first and line != "Elapsed_us,Total_us,Server_us\n":
                continue

            if line.strip() == "":
                continue

            if first and file_first:
                print(f"{','.join(exp.keys())},{line}", end='')
                first = False
                file_first = False
            elif file_first:
                file_first = False
            else:
                print(f"{','.join(exp.values())},{line}", end='')

first = True
for f in sys.argv[1:]:
    read_file(f, first)
    if first:
        first = False
