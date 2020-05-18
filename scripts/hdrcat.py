#!/usr/bin/env python3

import sys
import pandas as pd

def do_file(fn):
    itername = fn.split('/')[0]
    df = pd.read_csv(fn, sep=" ")
    df['iter'] = itername
    return df

df = pd.concat(do_file(exp) for exp in sys.argv[2:])
df.to_csv(sys.argv[1])
