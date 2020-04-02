import sys

out = {}
for line in sys.stdin:
    try:
        a,b,c,d = line.strip().split()
        d = int(float(d))
        if (a,b,c) in out:
            out[(a,b,c)] = max(d, out[(a,b,c)])
        else:
            out[(a,b,c)] = d
    except:
        print(line.strip())

for (a,b,c) in out:
    print(a,b,c, out[(a,b,c)])
