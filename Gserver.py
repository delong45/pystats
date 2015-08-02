#!/usr/bin/env python

import forkcore
import cache

stats = cache.Cache()

def time_worker(line):
    context = line.split(' ')
    key = context[0]
    count = context[1]
    value = context[2]
    timestamp = context[3]
    stats.timing(key, timestamp, value, count)

def qps_worker(line):
    context = line.split(' ')
    key = context[0]
    value = context[1]
    timestamp = context[2]
    stats.incr(key, timestamp, value)

if __name__ == "__main__":
    forkcore.Forkcore(time_worker, qps_worker)
