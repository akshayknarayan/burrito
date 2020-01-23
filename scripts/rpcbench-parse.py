#!/usr/bin/python3

import sys
import re

file_re = re.compile(r"work_(sqrts)_([0-9]+)-iters_([0-9]+)_(\w+)_(\w+)_(\w+)\.data")
def parse_filename(fn: str):
    m = file_re.search(fn)
    return {
        'work_type': m[1],
        'work_amount': m[2],
        'iters': m[3],
        'proto': m[4],
        'machine': m[5],
        'container': m[6],
    }

def read_file(fn: str, first: bool):
    exp = parse_filename(fn)
    file_first = True
    with open(fn) as f:
        for line in f:
            if file_first and line != "Total_us,Server_us\n":
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
