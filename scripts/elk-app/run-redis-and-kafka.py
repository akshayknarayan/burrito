#/usr/bin/python3

import signal
import subprocess as sh
import sys
import time

def cleanup(signal_num, stack_frame):
    print(f"received {signal_num}")
    sh.run("microk8s kubectl delete -f ./scripts/elk-app/redis.yaml", shell=True)
    sh.run("microk8s kubectl delete -f ./scripts/elk-app/kafka-deployment.yml", shell=True)
    sys.exit(0)

if __name__ == '__main__':
    sh.run("microk8s kubectl apply -f ./scripts/elk-app/redis.yaml", shell=True)
    sh.run("microk8s kubectl apply -f ./scripts/elk-app/kafka-deployment.yml", shell=True)

    signal.signal(signal.SIGINT, cleanup)

    while True:
        try:
            sh.run("microk8s kubectl port-forward deployment.apps/kafka-deployment 9092:9092", shell=True, check=True)
        except Exception as e:
            print("port-forward failed to start")
            time.sleep(1)
        sh.run("microk8s kubectl get all", shell=True)

