#!/usr/bin/python
import time

from gearman import libgearman


def toUpper(job):
    r = job.get_workload().upper()
    print r
    return r

def main():
    worker = libgearman.Worker()
    worker.add_server("127.0.0.1", 4730)
    worker.add_function("ToUpper", toUpper)
    while True:
        worker.work()

if __name__ == '__main__':
    main()

