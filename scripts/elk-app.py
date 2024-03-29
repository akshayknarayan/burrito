import argparse
import random
import string
import shutil
import threading
import toml
import os
import agenda
from kv import ConnectionWrapper
from localcn import LocalCn
import time
import itertools
import subprocess as sh
from rich.console import Console

def rand_string():
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(10))

def start_redis(cfg):
    m = cfg['machines']['redis']['conn']
    m.run("microk8s kubectl apply -f ./scripts/elk-app/redis.yaml", wd=m.dir, quiet=True)
    console = Console()
    with console.status("waiting for redis") as status:
        while True:
            try:
                s = m.run("microk8s kubectl get -o jsonpath='{.status.phase}' pod/redis", quiet=True)
                st = None
                if type(s.stdout) == bytes:
                    st = s.stdout.decode('utf-8')
                elif type(s.stdout) == str:
                    st = s.stdout
                else:
                    raise Exception(f"Unknown process type {s}")
                assert st == 'Running'
                break
            except Exception as e:
                if type(e) != AssertionError:
                    agenda.failure(e)
                    raise e
                time.sleep(1)
    agenda.subtask("started redis")

def stop_redis(cfg):
    m = cfg['machines']['redis']['conn']
    console = Console()
    with console.status(f"stopping redis") as status:
        m.run("microk8s kubectl delete --wait=true -f ./scripts/elk-app/redis.yaml", wd=m.dir, quiet=True)
    agenda.subtask("stopped redis")

kafka_yaml_template = '''
---
apiVersion: v1
kind: Service
metadata:
  name: kafka-service
  labels:
    app: kafka
spec:
  type: NodePort
  selector:
    app: kafka
  ports:
    - port: 9092
      targetPort: 9092
      nodePort: {port}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-deployment
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka
  template:
    metadata:
      labels:
        app: kafka
    spec:
      containers:
      - name: kafka
        image: ubuntu/kafka:edge
        env:
          - name: ZOOKEEPER_HOST
            value: zookeeper-service
        args: ["/etc/kafka/server.properties", "--override", "advertised.listeners=PLAINTEXT://{ip}:{port}"]
        ports:
        - containerPort: 9092
          name: kafka
          protocol: TCP
---
apiVersion: v1
kind: Service
metadata:
  name: zookeeper-service
  labels:
    app: zookeeper
spec:
  ports:
  - port: 2181
  selector:
    app: zookeeper
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: zookeeper-deployment
spec:
  replicas: 1
  selector:
    matchLabels:
      app: zookeeper
  template:
    metadata:
      labels:
        app: zookeeper
    spec:
      containers:
      - name: zookeeper
        image: ubuntu/zookeeper:edge
        ports:
        - containerPort: 2181
          name: zookeeper
          protocol: TCP
'''

def start_kafka(cfg):
    if not cfg['exp']['pubsub-kafka']:
        stop_kafka(cfg)
        agenda.subtask("skipping kafka")
        return
    m = cfg['machines']['kafka']['conn']
    kafka_access = cfg['machines']['kafka']['access']
    is_local = 'localhost' in kafka_access

    out = None
    if is_local:
        out = m.run("microk8s kubectl apply -f ./scripts/elk-app/kafka-deployment.yml", wd=m.dir, quiet=True)
    else:
        ip, port = kafka_access.split(':')
        kafka_yml = kafka_yaml_template.format(ip=ip, port=port)
        out = m.run(f"echo \"{kafka_yml}\" > ./scripts/elk-app/{ip}-kafka-deployment.yml", wd=m.dir, quiet=True)
        out = m.run(f"microk8s kubectl apply -f ./scripts/elk-app/{ip}-kafka-deployment.yml", wd=m.dir, quiet=True)
    assert m.check_code(out), f"kubectl apply kafka failed:\nstdout:\n{out.stdout}\nstderr:\n{out.stderr}"
    console = Console()
    with console.status("waiting for kafka") as status:
        while True:
            try:
                s = m.run("microk8s kubectl get -o jsonpath='{.status.readyReplicas}' deployment.apps/kafka-deployment", quiet=True)
                st = None
                if type(s.stdout) == bytes:
                    st = s.stdout.decode('utf-8')
                elif type(s.stdout) == str:
                    st = s.stdout
                else:
                    raise Exception(f"Unknown process type {s}")
                assert st == '1'
                break
            except Exception as e:
                if type(e) != AssertionError:
                    agenda.failure(e)
                    raise e
                time.sleep(1)
    agenda.subtask("started kafka")

    if is_local:
        kafka_port_forward = m.run("microk8s kubectl port-forward deployment.apps/kafka-deployment 9092:9092", background=True, quiet=True)
        start = time.time()
        with console.status("waiting for kafka port-forward") as status:
            while True:
                time.sleep(1)
                if not m.check_code(kafka_port_forward):
                    agenda.subtask("restarting kafka port-forward")
                    if time.time() - start > 5:
                        m.run("pkill -9 kubectl", shell=True, quiet=True)
                    kafka_port_forward = m.run("microk8s kubectl port-forward deployment.apps/kafka-deployment 9092:9092", background=True, quiet=True)
                ok = m.run("ps aux | grep \"kubectl port-forward deployment.apps/kafka-deployment 9092:9092\" | grep -q -v 'grep'", quiet=True)
                if m.check_code(ok):
                    break
        agenda.subtask("started kafka port-forward")

def stop_kafka(cfg):
    m = cfg['machines']['redis']['conn']
    m.run("pkill -INT kubectl", quiet=True)
    console = Console()
    with console.status(f"stopping kafka") as status:
        m.run("microk8s kubectl delete --wait=true -f ./scripts/elk-app/kafka-deployment.yml", wd=m.dir, quiet=True)
    agenda.subtask("stopped kafka")

def wait_ready(m: ConnectionWrapper, outf, search_string=" ready "):
    console = Console()
    with console.status(f"[{m.name}] wait for '{search_string}' in {m.dir}/{outf}") as status:
        now = time.time()
        while True:
            sc = m.run(f"grep -q '{search_string}' {outf}", wd=m.dir, quiet=True)
            if m.check_code(sc):
                break
            if time.time() - now > 15:
                raise Exception(f"timed out waiting on {outf}")
            time.sleep(1)
    agenda.subtask(f"[{m.name}] waited for {search_string} in {outf}")

def run_producer(cfg, outf, bin_root = "./target/release"):
    m = cfg['machines']['producer']['conn']
    agenda.task("start producer")
    m.run("pkill -INT producer", quiet=True)

    redis = ""
    if cfg['exp']['producer']['allow-client-sharding']:
        redis = f"--redis-addr={cfg['machines']['redis']['access']}"
    connect_ip = cfg['machines']['logingest']['ip']
    connect_port = cfg['machines']['logingest']['port']
    msg_limit = cfg['exp']['producer']['msg-limit']
    msg_interarrival_ms = cfg['exp']['producer']['msg-interarrival-ms']
    encr_spec = cfg["exp"]["encrypt"]
    assert type(encr_spec) == str
    ok = m.run(f"RUST_LOG=info {bin_root}/producer \
        --logging \
        --connect-addr={connect_ip}:{connect_port} \
        {redis} \
        --produce-interarrival-ms={msg_interarrival_ms} \
        --tot-message-limit={msg_limit} \
        --encr-spec={encr_spec} \
        --stats-file={outf}.data",
        background=False,
        wd=m.dir,
        stdout=f"{outf}.out",
        stderr=f"{outf}.err"
        )
    if not m.check_code(ok):
        raise Exception("producer failed to run")
    agenda.task("producer completed")

def start_logingest(cfg, outf, bin_root = "./target/release"):
    m = cfg['machines']['logingest']['conn']
    agenda.task("start logingest")
    m.run("pkill -INT logingest", quiet=True)

    kafka = ""
    if cfg["exp"]["pubsub-kafka"]:
        kafka_addr = cfg["machines"]["kafka"]["access"]
        kafka = f"--kafka-addr={kafka_addr}"
    redis_addr = cfg['machines']['redis']['access']
    listen_ip = cfg['machines']['logingest']['ip']
    listen_port = cfg['machines']['logingest']['port']
    hostname = cfg["machines"]["logingest"]["hostname"]
    topic_name = cfg['conf']['topic-name']
    gcp_project = cfg['conf']['gcp-project-name']
    gcp_key_path = cfg['conf']['gcp-credentials-path']
    num_workers = cfg['exp']['logingest']['workers']
    encr_spec = cfg["exp"]["encrypt"]
    assert type(encr_spec) == str
    ok = m.run(f"RUST_LOG=info GOOGLE_APPLICATION_CREDENTIALS={gcp_key_path} {bin_root}/logingest \
        --logging \
        --gcp-project-name={gcp_project} \
        --redis-addr={redis_addr} \
        {kafka} \
        --topic-name={topic_name} \
        --listen-addr={listen_ip}:{listen_port} \
        --hostname={hostname} \
        --num-workers={num_workers} \
        --encr-spec={encr_spec} \
        --stats-file={outf}.data",
        background=True,
        wd=m.dir,
        stdout=f"{outf}.out",
        stderr=f"{outf}.err"
        )
    if not m.check_code(ok):
        raise Exception("failed to start logingest")
    agenda.subtask(f"[{m.name}] wait for logingest start")
    wait_ready(m, f"{outf}.out")

def start_logparser(m, cfg, outf, bin_root = "./target/release"):
    agenda.task("start logparser")
    m.run("pkill -INT logparser", quiet=True)

    use_local = cfg["exp"]["local-fastpath"]
    local_root = ""
    if use_local:
        local_root = "--local-root"
    kafka = ""
    if cfg["exp"]["pubsub-kafka"]:
        kafka_addr = cfg["machines"]["kafka"]["access"]
        kafka = f"--kafka-addr={kafka_addr}"
    redis_addr = cfg['machines']['redis']['access']
    forward_ip = cfg['machines']['consumer']['ip']
    forward_port = cfg['machines']['consumer']['port']
    topic_name = cfg['conf']['topic-name']
    gcp_project = cfg['conf']['gcp-project-name']
    gcp_key_path = cfg['conf']['gcp-credentials-path']
    interval_ms = cfg['exp']['logparser']['interval-ms']
    encr_spec = cfg["exp"]["encrypt"]
    assert type(encr_spec) == str

    num_parsers = cfg['exp']['logparser']['processes-per-machine']
    for proc_num in range(num_parsers):
        ok = m.run(f"RUST_LOG=info GOOGLE_APPLICATION_CREDENTIALS={gcp_key_path} {bin_root}/logparser \
            --logging \
            --gcp-project-name={gcp_project} \
            --redis-addr={redis_addr} \
            {kafka} \
            {local_root} \
            --topic-name={topic_name} \
            --forward-addr={forward_ip}:{forward_port} \
            --interval-ms={interval_ms} \
            --encr-spec={encr_spec} \
            --stats-file={outf}-{proc_num}.data",
            background=True,
            wd=m.dir,
            stdout=f"{outf}-{proc_num}.out",
            stderr=f"{outf}-{proc_num}.err"
            )
        if not m.check_code(ok):
            raise Exception(f"failed to start logparser {proc_num}")
        wait_ready(m, f"{outf}-{proc_num}.out")

def start_consumer(cfg, outf, bin_root = "./target/release"):
    m = cfg['machines']['consumer']['conn']
    agenda.task("start consumer")
    m.run("pkill -INT consumer", quiet=True)

    use_local = cfg["exp"]["local-fastpath"]
    local_root = ""
    if use_local:
        local_root = "--local-root"
    hostname = cfg["machines"]["consumer"]["hostname"]
    port = cfg["machines"]["consumer"]["port"]
    encr_spec = cfg["exp"]["encrypt"]
    assert type(encr_spec) == str
    ok = m.run(f"env RUST_LOG=info {bin_root}/consumer \
        --logging \
        --listen-addr={m.addr}:{port} \
        --hostname={hostname} \
        {local_root} \
        --encr-spec={encr_spec} \
        --out-file={outf}.data",
        background=True,
        wd=m.dir,
        stdout=f"{outf}.out",
        stderr=f"{outf}.err",
        )
    if not m.check_code(ok):
        raise Exception("failed to start consumer")
    wait_ready(m, f"{outf}.out")

def drain_consumer(cfg, outdir):
    m = cfg['machines']['consumer']['conn']
    desc = cfg['exp']['desc']
    # wait for some messages to drain from the consumer,
    # but don't wait longer than 60 seconds overall
    cons_data_out_lines = 0
    last_increase = None
    start_drain = time.time()
    console = Console()
    with console.status("draining consumer") as status:
        while True:
            o = m.run(f"wc -l {outdir}/{desc}-consumer.data", wd=m.dir, quiet=True)
            if not m.check_code(o):
                raise Exception(f"could not find consumer data file: {o}")
            out = o.stdout if type(o.stdout) == str else o.stdout.decode('utf-8')
            curr_lines = int(out.strip().split()[0])
            now = time.time()
            if curr_lines > cons_data_out_lines:
                last_increase = now
                cons_data_out_lines = curr_lines
            if (last_increase == None or cons_data_out_lines < 5) and (now - start_drain) > 60:
                raise Exception("Over 60 seconds without consumer output")
            if cons_data_out_lines > 5 and (last_increase != None and (now - last_increase) > 15):
                break
            time.sleep(5)
    agenda.subtask(f"consumer drained after {time.time() - start_drain}s")

def exp(cfg, outdir, overwrite=False, setup_only=False, debug=False):
    desc = cfg['exp']['desc']
    assert (
        cfg["exp"]["logparser"]['machines']
        <= len(cfg['machines']['logparser'])
        ), 'not enough logparser machines'
    logparser_fls = []
    for i in range(cfg["exp"]["logparser"]['machines']):
        for j in range(cfg['exp']['logparser']['processes-per-machine']):
            logparser_fls.append(f'logparser-{i}-{j}')
    fls = [
        f"{outdir}/{desc}-{n}.data"
        for n in
        ["consumer", "logingest", "producer"] + logparser_fls
    ]
    if not overwrite and all(os.path.exists(f) for f in fls):
        agenda.task(f"skipping: {desc}")
        return

    agenda.task(f"running: {desc}")
    failed = None
    try:
        start_redis(cfg)
        start_kafka(cfg)
        if setup_only:
            raise Exception("only doing setup")
        start_consumer(cfg, f"{outdir}/{desc}-consumer", bin_root='./target/debug' if debug else "./target/release")
        time.sleep(5)
        i = 0
        for logparser_machine_idx in range(cfg["exp"]["logparser"]["machines"]):
            c = cfg['machines']['logparser'][logparser_machine_idx]
            start_logparser(c['conn'], cfg, f"{outdir}/{desc}-logparser-{i}", bin_root='./target/debug' if debug else "./target/release")
            i += 1
        start_logingest(cfg, f"{outdir}/{desc}-logingest")
        run_producer(cfg, f"{outdir}/{desc}-producer", bin_root='./target/debug' if debug else "./target/release")
        drain_consumer(cfg, outdir)
    except Exception as e:
        agenda.failure(f"failed: {desc}")
        failed = e
    agenda.subtask("stopping processes")
    cfg['machines']['logingest']['conn'].run("pkill -INT logingest")
    for c in cfg["machines"]["logparser"]:
        c["conn"].run("pkill -INT logparser")
    cfg["machines"]["consumer"]["conn"].run("pkill -INT consumer")
    stop_kafka(cfg)
    stop_redis(cfg)
    if failed != None:
        raise failed

    if setup_only:
        return
    # are there at least 5 lines in the out file?
    # get file if necessary
    for fl in fls:
        while True:
            try:
                with open(fl, 'r') as f:
                    assert 3 <= len(list(itertools.islice(f, 0, 10))), f"{fl} had < 3 lines"
                agenda.subtask(f"{fl} has at least 3 lines")
                break
            except AssertionError as e:
                raise e # if we got to the assertion, the file is local
            except Exception as e:
                pass # try getting the file
            name = fl.split('.')[0].split('-')
            if name[-1] in ['consumer', 'logingest', 'producer']:
                c = cfg['machines'][name[-1]]['conn']
                root = c.dir + '/'
                c.get(root + fl, local=fl) #.data
                out = fl.replace('.data', '.out')
                c.get(root + out, local=out) #.out
                err = fl.replace('.data', '.err')
                c.get(root + err, local=err) #.err
            elif name[-3] == 'logparser':
                c = cfg['machines']['logparser'][int(name[-2])]['conn']
                root = c.dir + '/'
                c.get(root + fl, local=fl)
                out = fl.replace('.data', '.out')
                c.get(root + fl.replace('.data', '.out'), local=out) #.out
                err = fl.replace('.data', '.err')
                c.get(root + fl.replace('.data', '.err'), local=err) #.err
            else:
                raise Exception(f"Unknown file {fl}")
    agenda.task(f"exp done: {desc}")

def connect(machine_cfg):
    conn_cfg = machine_cfg['connect']
    ip = conn_cfg['ip']
    agenda.task(f"connect to {ip}")
    if ip in ["127.0.0.1", 'local', 'localhost', '::1']:
        conn = LocalCn(ip)
    else:
        conn = ConnectionWrapper(
            ip,
            user=conn_cfg['user'] if 'user' in conn_cfg else None,
            port=conn_cfg['port'] if 'port' in conn_cfg else None
        )
    conn.dir = conn_cfg["dir"] if "dir" in conn_cfg else "~/burrito"
    if not conn.file_exists(conn.dir, quiet=True):
        agenda.failure(f"No burrito on {ip}")
        raise Exception(f"No burrito on {ip}")
    commit = conn.run("git rev-parse --short HEAD", wd = conn.dir, quiet=True)
    if conn.check_code(commit):
        commit = commit.stdout.strip()
    else:
        raise Exception(f"Could not get commit on {ip}")
    agenda.subtask(f"burrito commit {commit} on {ip}")
    conn.name = ip
    conn.addr = machine_cfg['ip'] if 'ip' in machine_cfg else ip
    return (conn, commit)

def do_compile(m, idx, out):
    ok = m.run("cargo build --release", wd=f"{m.dir}/elk-app-logparser")
    if not m.check_code(ok):
        out[idx] = Exception(f"could not compile on {m.name}")
    out[idx] = None

def num_confs(cfg):
    exp = cfg['exp']
    tot = exp['iterations']
    tot *= len(exp['pubsub-kafka'])
    tot *= len(exp['local-fastpath'])
    tot *= len(exp['encrypt'])
    for k in exp['producer']:
        tot *= len(exp['producer'][k])
    for k in exp['logingest']:
        tot *= len(exp['logingest'][k])
    for k in exp['logparser']:
        tot *= len(exp['logparser'][k])
    return tot

def iter_confs(cfg):
    exp = cfg['exp']
    i = exp['iterations']
    prod = exp['producer']
    ing = exp['logingest']
    par = exp['logparser']
    confs = itertools.product(
        exp['pubsub-kafka'],
        exp['local-fastpath'],
        exp['encrypt'],
        prod['allow-client-sharding'],
        prod['msg-limit'],
        prod['msg-interarrival-ms'],
        ing['workers'],
        par['machines'],
        par['processes-per-machine'],
        par['interval-ms'],
        range(i),
    )
    for conf in confs:
        exp = {
                'pubsub-kafka': conf[0],
                'local-fastpath': conf[1],
                'encrypt': conf[2],
                'producer': {
                    'allow-client-sharding': conf[3],
                    'msg-limit': conf[4],
                    'msg-interarrival-ms': conf[5],
                },
                'logingest': {
                    'workers': conf[6],
                },
                'logparser': {
                    'machines': conf[7],
                    'processes-per-machine': conf[8],
                    'interval-ms': conf[9],
                },
                'iteration': conf[10],
        }
        if type(exp['producer']['msg-limit']) == str:
            assert exp['producer']['msg-limit'][-1] == 's', 'time-based msg-limit must be in seconds'
            target_seconds = int(exp['producer']['msg-limit'][:-1])
            inter_seconds = float(int(exp['producer']['msg-interarrival-ms'])) / 1000
            exp['producer']['msg-limit'] = int(target_seconds / inter_seconds) * 16
            agenda.subtask(f"set producer msg-limit to {exp['producer']['msg-limit']} from target {target_seconds}")
        template = (
            "kfka={kafka}-"
            + "fp={localfp}-"
            + "cshd={client_shard}-"
            + "enc={encrypt}-"
            + "nmsg={num_msg}-"
            + "intms={msg_inter_ms}-"
            + "iwrk={ingest_workers}-"
            + "pn={parser_machines}-"
            + "pps={parser_procs}-"
            + "prpintms={parser_report_interval_ms}-"
            + "i={i}")
        desc = template.format(
            kafka=exp['pubsub-kafka'],
            localfp=exp['local-fastpath'],
            encrypt=exp['encrypt'].replace('-',''),
            client_shard=exp['producer']['allow-client-sharding'],
            num_msg=exp['producer']['msg-limit'],
            msg_inter_ms=exp['producer']['msg-interarrival-ms'],
            ingest_workers=exp['logingest']['workers'],
            parser_machines=exp['logparser']['machines'],
            parser_procs=exp['logparser']['processes-per-machine'],
            parser_report_interval_ms=exp['logparser']['interval-ms'],
            i=exp['iteration'],
        )
        exp['desc'] = desc
        cfg['exp'] = exp
        name_base = '-'.join(cfg['conf']['topic-name'].split('-')[:-1])
        cfg['conf']['topic-name'] = f"{name_base}-{rand_string()}"
        yield cfg

# # Sample config
# [machines]
# producer = { connect = { ip = "127.0.0.1", dir = "/Users/akshay/research/burrito" } }
# logparser = [
#     { connect = { ip = "127.0.0.1", dir = "/Users/akshay/research/burrito" } },
# ]
#
# [machines.logingest]
# connect = { ip = "127.0.0.1", dir = "/Users/akshay/research/burrito" }
# port = 21441
# hostname = "localhost"
# [machines.consumer]
# connect = { ip = "127.0.0.1", dir = "/Users/akshay/research/burrito" }
# port = 15151
# hostname = "localhost"
# [machines.kafka]
# connect = { ip = "127.0.0.1", dir = "/Users/akshay/research/burrito" }
# access = "localhost:9092"
# [machines.redis]
# connect = { ip = "127.0.0.1", dir = "/Users/akshay/research/burrito" }
# access = "redis://192.168.64.2"
#
# [conf]
# gcp-project-name = "WTF-marx"
# gcp-credentials-path="..." # optional, can pass as argument
# topic-name = "server-logs"
#
# # total number of experiments is [iterations] * [cartesian product of all other keys]
# [exp]
# iterations = 1
# pubsub-kafka = [true, false]
# local-fastpath = [true, false]
# encrypt = ["allow-none", "auto-only", "rustls-only", "quic-only"]
#
# [exp.producer]
# allow-client-sharding = [true, false]
# msg-limit = [2500]
# msg-interarrival-ms = [1000]
#
# [exp.logingest]
# workers = [2]
#
# [exp.logparser]
# machines = [1]
# processes-per-machine = [1]
# interval-ms = [1000]
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, required=True)
    parser.add_argument('--outdir', type=str, required=True)
    parser.add_argument('--gcp_creds_path', type=str, required=False)
    parser.add_argument('--overwrite', action='store_true')
    parser.add_argument('--setup_only', action='store_true',  required=False)
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()
    agenda.task(f"reading cfg {args.config}")
    cfg = toml.load(args.config)
    print(cfg)

    if 'gcp-credentials-path' not in cfg['conf']:
        if args.gcp_creds_path == None:
            raise Exception("need GCP credential file path as argument or in config file")
        cfg['conf']['gcp-credentials-path'] = args.gcp_creds_path

    outdir = args.outdir
    all_conns = {}
    (cfg["machines"]["producer"]["conn"], producer_commit) = connect(cfg["machines"]["producer"])
    all_conns[cfg["machines"]["producer"]["conn"].addr] = cfg["machines"]["producer"]["conn"]
    (cfg["machines"]["logingest"]["conn"], logingest_commit) = connect(cfg["machines"]["logingest"])
    all_conns[cfg["machines"]["logingest"]["conn"].addr] = cfg["machines"]["logingest"]["conn"]
    if len(cfg["machines"]["logparser"]) < 1:
        raise Exception("no logparser machines found")
    lp = [connect(c) for c in cfg["machines"]["logparser"]]
    (lp_conns, logparser_commits) = zip(*lp)
    for c in lp_conns:
        all_conns[c.addr] = c
    for (l, cn) in zip(cfg["machines"]["logparser"], lp_conns):
        l['conn'] = cn
    (cfg["machines"]["consumer"]["conn"], consumer_commit) = connect(cfg["machines"]["consumer"])
    all_conns[cfg["machines"]["consumer"]["conn"].addr] = cfg["machines"]["consumer"]["conn"]
    local_commit = sh.run("git rev-parse --short HEAD", shell=True, capture_output=True, text=True, check=True).stdout.strip()
    if not all(c == local_commit for c in [x.decode('utf-8') if type(x) == bytes else x for x in [producer_commit, logingest_commit, consumer_commit] + list(logparser_commits)]):
        agenda.subfailure(f"not all commits equal: {[local_commit, producer_commit, logingest_commit, consumer_commit] + list(logparser_commits)}")
        raise Exception("Commits mismatched on machines")

    if not args.debug:
        console = Console()
        with console.status("compiling elk-app") as status:
            ms = [cfg['machines']['producer']['conn'],
                cfg['machines']['logingest']['conn'],
                cfg['machines']['consumer']['conn'],
                ] + [p['conn'] for p in cfg['machines']['logparser']]
            out = [None for _ in range(len(ms))]
            ts = [
                threading.Thread(target=do_compile, args=(m,i,out))
                for (i,m) in zip(range(len(ms)), ms)
            ]
            [t.start() for t in ts]
            [t.join() for t in ts]
            if any(o != None for o in out):
                raise Exception(f"{out}")
        agenda.task("done compiling")

    # connect to the redis and kafka machines.
    (cfg['machines']['redis']['conn'], _) = connect(cfg['machines']['redis'])
    (cfg['machines']['kafka']['conn'], _) = connect(cfg['machines']['kafka'])

    agenda.task("Connected to machines")
    for c in all_conns:
        m = all_conns[c]
        m.run(f"mkdir -p {args.outdir}", wd=m.dir, quiet=True)
    if not os.path.exists(f"{args.outdir}"):
        sh.run(f"mkdir -p {args.outdir}", shell=True)
    # copy config file to outdir
    shutil.copy2(args.config, args.outdir)

    stop_kafka(cfg)
    stop_redis(cfg)

    num_remaining = num_confs(cfg)
    cfgs = iter_confs(cfg)
    for c in cfgs:
        agenda.task(f"{num_remaining} experiments remaining")
        exp(c, args.outdir, setup_only=args.setup_only, debug=args.debug)
        num_remaining -= 1
