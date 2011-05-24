#!/usr/bin/python
import time

import gearman


def toUpper(worker, job):
    r = job.data.upper()
    print r
    return r

def main():
    worker = gearman.GearmanWorker(['localhost:4730'])
    worker.register_task('ToUpper', toUpper)
    worker.work()

if __name__ == '__main__':
    main()

