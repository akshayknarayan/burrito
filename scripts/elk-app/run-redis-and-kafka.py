#/usr/bin/python3

import signal
import subprocess as sh
import sys
import time

def start_redis():
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
    print("started redis")

def stop_redis():
    sh.run("microk8s kubectl delete -f ./scripts/elk-app/redis.yaml", shell=True)

def start_kafka():
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
    print("started kafka")

def stop_kafka():
    sh.run("microk8s kubectl delete -f ./scripts/elk-app/kafka-deployment.yml", shell=True)

if __name__ == '__main__':
    start_redis()
    start_kafka()

    def sig(signal_num, stack_frame):
        print(f"received {signal_num}")
        stop_redis()
        stop_kafka()
        sys.exit(0)
    signal.signal(signal.SIGINT, sig)
    while True:
        try:
            #sh.run("microk8s kubectl port-forward deployment.apps/kafka-deployment 9093:9092", shell=True, check=True)
            sh.run("microk8s kubectl port-forward deployment.apps/kafka-deployment 9092:9092", shell=True, check=True)
            #signal.pause()
        except Exception as e:
            print("port-forward failed to start")
            time.sleep(1)
            sh.run("microk8s kubectl get all", shell=True)
