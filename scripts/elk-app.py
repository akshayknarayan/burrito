import argparse
import shutil
import toml
import os
import agenda
from kv import ConnectionWrapper
from localcn import LocalCn
import time
import itertools
import subprocess as sh

def start_redis(cfg):
    assert cfg['machines']['redis']['connect']['ip'] == '127.0.0.1', "non-local redis not implemented"
    sh.run("microk8s kubectl apply -f ./scripts/elk-app/redis.yaml", shell=True)
    while True:
        try:
            s = sh.run("microk8s kubectl get -o jsonpath='{.status.phase}' pod/redis", shell=True, capture_output=True)
            st = s.stdout.decode('utf-8')
            assert st == 'Running'
            break
        except Exception:
            sh.run("microk8s kubectl get all", shell=True)
            time.sleep(1)
    agenda.subtask("started redis")

def stop_redis(cfg):
    assert cfg['machines']['redis']['connect']['ip'] == '127.0.0.1', "non-local redis not implemented"
    sh.run("microk8s kubectl delete -f ./scripts/elk-app/redis.yaml", shell=True)
    agenda.subtask("stopped redis")

def start_kafka(cfg):
    assert cfg['machines']['kafka']['connect']['ip'] == '127.0.0.1', "non-local kafka not implemented"
    if not cfg['exp']['pubsub-kafka']:
        agenda.subtask("skipping kafka")
        return None
    sh.run("microk8s kubectl apply -f ./scripts/elk-app/kafka-deployment.yml", shell=True)
    while True:
        try:
            s = sh.run("microk8s kubectl get -o jsonpath='{.status.readyReplicas}' deployment.apps/kafka-deployment", shell=True, capture_output=True)
            st = s.stdout.decode('utf-8')
            assert st == '1'
            break
        except Exception:
            sh.run("microk8s kubectl get all", shell=True)
            time.sleep(1)
    agenda.subtask("kafka port forward")
    kafka_port_forward = sh.Popen("microk8s kubectl port-forward deployment.apps/kafka-deployment 9092:9092", shell=True, stdout=sh.PIPE, text=True)
    start = time.time()
    while True:
        time.sleep(1)
        if kafka_port_forward.poll() != None:
            agenda.subtask("restarting kafka port-forward")
            if time.time() - start > 5:
                sh.run("pkill -9 kubectl", shell=True)
            kafka_port_forward = sh.Popen("microk8s kubectl port-forward deployment.apps/kafka-deployment 9092:9092", shell=True, stdout=sh.PIPE, text=True)
        agenda.subtask("waiting for kafka port-forward")
        out = kafka_port_forward.stdout.readline()
        if "Forwarding" in out:
            break
    agenda.subtask("started kafka")
    return kafka_port_forward

def stop_kafka(cfg, kafka_port_forward):
    assert cfg['machines']['kafka']['connect']['ip'] == '127.0.0.1', "non-local kafka not implemented"
    if kafka_port_forward != None:
        agenda.subtask("wait for kafka port-forward to stop")
        kafka_port_forward.kill()
        kafka_port_forward.wait()
        agenda.subtask("kafka port-forward stopped")
    else:
        agenda.subtask("kafka port-forward process handle not provided")
        sh.run("pkill -9 kubectl", shell=True)
    sh.run("microk8s kubectl delete -f ./scripts/elk-app/kafka-deployment.yml", shell=True)
    agenda.subtask("stopped kafka")

def wait_ready(m: ConnectionWrapper, outf, search_string="ready"):
    agenda.subtask(f"[{m.addr}] wait for {search_string} in {m.dir}/{outf}")
    now = time.time()
    while True:
        sc = m.run(f"grep -q {search_string} {outf}", wd=m.dir, quiet=True)
        if m.check_code(sc):
            break
        if time.time() - now > 15:
            raise Exception(f"timed out waiting on {outf}")
        time.sleep(1)
    agenda.subtask(f"[{m.addr}] waited for {search_string} in {outf}")

def run_producer(cfg, outf, bin_root = "./target/release"):
    m = cfg['machines']['producer']['conn']
    agenda.task("start producer")
    m.run("pkill -INT producer")

    redis = ""
    if cfg['exp']['producer']['allow-client-sharding']:
        redis = f"--redis-addr={cfg['machines']['redis']['access']}"
    connect_ip = cfg['machines']['logingest']['connect']['ip']
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
    m.run("pkill -INT logingest")

    kafka = ""
    if cfg["exp"]["pubsub-kafka"]:
        kafka_addr = cfg["machines"]["kafka"]["access"]
        kafka = f"--kafka-addr={kafka_addr}"
    redis_addr = cfg['machines']['redis']['access']
    listen_ip = cfg['machines']['logingest']['connect']['ip']
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
    agenda.subtask(f"[{m.addr}] wait for logingest start")
    wait_ready(m, f"{outf}.out")

def start_logparser(m, cfg, outf, bin_root = "./target/release"):
    agenda.task("start logparser")
    m.run("pkill -INT logparser")

    use_local = cfg["exp"]["local-fastpath"]
    local_root = ""
    if use_local:
        local_root = "--local-root"
    kafka = ""
    if cfg["exp"]["pubsub-kafka"]:
        kafka_addr = cfg["machines"]["kafka"]["access"]
        kafka = f"--kafka-addr={kafka_addr}"
    redis_addr = cfg['machines']['redis']['access']
    forward_ip = cfg['machines']['consumer']['connect']['ip']
    forward_port = cfg['machines']['consumer']['port']
    topic_name = cfg['conf']['topic-name']
    gcp_project = cfg['conf']['gcp-project-name']
    gcp_key_path = cfg['conf']['gcp-credentials-path']
    interval_ms = cfg['exp']['logparser']['interval-ms']
    encr_spec = cfg["exp"]["encrypt"]
    assert type(encr_spec) == str
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
        --stats-file={outf}.data",
        background=True,
        wd=m.dir,
        stdout=f"{outf}.out",
        stderr=f"{outf}.err"
        )
    if not m.check_code(ok):
        raise Exception("failed to start logparser")
    agenda.subtask(f"[{m.addr}] wait for logparser start")
    wait_ready(m, f"{outf}.out")

def start_consumer(cfg, outf, bin_root = "./target/release"):
    m = cfg['machines']['consumer']['conn']
    agenda.task("start consumer")
    m.run("pkill -INT consumer")

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
    agenda.subtask("[{m.addr}] wait for consumer start")
    wait_ready(m, f"{outf}.out")

def exp(cfg, outdir, overwrite=False):
    desc = cfg['exp']['desc']
    assert (
        cfg["exp"]["logparser"]['machines']
        <= len(cfg['machines']['logparser'])
        ), 'not enough logparser machines'
    fls = [
        f"{outdir}/{desc}-{i}.data"
        for i in
        ["consumer", "logingest", "producer"]
        + [f'logparser-{i}' for i in range(cfg["exp"]["logparser"]['machines'])]
    ]
    if not overwrite and all(os.path.exists(f) for f in fls):
        agenda.task(f"skipping: {desc}")
        return

    agenda.task(f"running: {desc}")
    failed = None
    kafka_pf = None
    try:
        start_redis(cfg)
        kafka_pf = start_kafka(cfg)
        start_consumer(cfg, f"{outdir}/{desc}-consumer")
        i = 0
        for c in cfg["machines"]["logparser"]:
            start_logparser(c['conn'], cfg, f"{outdir}/{desc}-logparser-{i}")
            i += 1
        start_logingest(cfg, f"{outdir}/{desc}-logingest")
        run_producer(cfg, f"{outdir}/{desc}-producer")

        # wait for some messages to drain from the consumer,
		# but don't wait longer than 60 seconds overall
        start_wait = time.time()
        cons_data_out_lines = 0
        with open(fls[0], 'r') as f:
            cons_data_out_lines = len(list(itertools.islice(f, 0, 10)))
        agenda.subtask("drain consumer")
        while cons_data_out_lines < 5 or time.time() - os.path.getmtime(fls[0]) < 20:
            time.sleep(2)
            if time.time() - start_wait > 60:
                break
            with open(fls[0], 'r') as f:
                cons_data_out_lines = len(list(itertools.islice(f, 0, 10)))
        agenda.subtask(f"consumer drained after {time.time() - start_wait}s")
    except Exception as e:
        agenda.failure(f"failed: {desc}")
        failed = e
    agenda.subtask("stopping processes")
    cfg['machines']['logingest']['conn'].run("pkill -INT logingest")
    for c in cfg["machines"]["logparser"]:
        c["conn"].run("pkill -INT logparser")
    cfg["machines"]["consumer"]["conn"].run("pkill -INT consumer")
    stop_kafka(cfg, kafka_pf)
    stop_redis(cfg)
    if failed != None:
        raise failed

    # are there at least 5 lines in the out file?
    for fl in fls:
        agenda.subtask(f"checking {fl}")
        with open(fl, 'r') as f:
            assert 5 <= len(list(itertools.islice(f, 0, 10)))
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
    if not conn.file_exists(conn.dir):
        agenda.failure(f"No burrito on {ip}")
        raise Exception(f"No burrito on {ip}")
    commit = conn.run("git rev-parse --short HEAD", wd = conn.dir)
    if conn.check_code(commit):
        commit = commit.stdout.strip()
    else:
        raise Exception(f"Could not get commit on {ip}")
    agenda.subtask(f"burrito commit {commit} on {ip}")
    conn.addr = ip
    return (conn, commit)

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
        template = (
            "kafka={kafka}-"
            + "localfp={localfp}-"
            + "clshrd={client_shard}-"
            + "enc={encrypt}-"
            + "nmsg={num_msg}-"
            + "msg_inter_ms={msg_inter_ms}-"
            + "ing_wrk={ingest_workers}-"
            + "par_num={parser_machines}-"
            + "par_pcs={parser_procs}-"
            + "par_rep_int_ms={parser_report_interval_ms}-"
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
            i=0,
        )
        exp['desc'] = desc
        cfg['exp'] = exp
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
    parser.add_argument('--cloudlab', action='store_true',  required=False)
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
    if not all(c == producer_commit for c in [logingest_commit, consumer_commit] + list(logparser_commits)):
        agenda.subfailure(f"not all commits equal: {[logingest_commit, consumer_commit] + list(logparser_commits)}")
        raise Exception("Commits mismatched on machines")

    agenda.task("Connected to machines")
    for c in all_conns:
        all_conns[c].run(f"mkdir -p {args.outdir}")
    # copy config file to outdir
    shutil.copy2(args.config, args.outdir)

    stop_kafka(cfg, None)
    stop_redis(cfg)

    num_remaining = num_confs(cfg)
    cfgs = iter_confs(cfg)
    for c in cfgs:
        agenda.task(f"{num_remaining} experiments remaining")
        exp(c, args.outdir)
        num_remaining -= 1
