#!/usr/bin/env python
import sys
import os
from multiprocessing import Process, cpu_count

from rq import Connection, Worker
from redis import Redis

import cluster

def worker():
    with Connection(Redis("jobqueue.local")):
        qs = sys.argv[1:] or ['default']
        print("foo")
        w = Worker(qs)
        w.work()

def main():
    processes = []
    for i in range(int(cpu_count())):
        p = Process(target=worker)
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

if __name__ == "__main__":
    main()
