#!/usr/bin/env python

import forkcore
import cache

stats = cache.Cache()

def time_worker(line):
    try:
        context = line.split(' ')
        key = context[0]
        count = int(context[1])
        value = float(context[2])
        timestamp = int(context[3])
        stats.timing(key, timestamp, value, count)
    except Exception:
        pass

def qps_worker(line):
    try:
        context = line.split(' ')
        key = context[0]
        value = int(context[1])
        timestamp = int(context[2])
        stats.incr(key, timestamp, value)
    except Exception:
        pass

if __name__ == "__main__":
    forkcore.Forkcore(time_worker, qps_worker)
