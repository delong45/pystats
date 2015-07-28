#!/usr/bin/env python

import socket
import sys
import Queue
import time

class Graphite(object):

    def __init__(self, size=60, sock=None):
        if sock is None:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            self.sock = sock
        self.keeper = False
        self.queue = Queue.Queue(size)

    def connect(host='127.0.0.1', port=2003):
        self.sock.connect((host, prot))

    def incr(self, key, timestamp):
        if not self.keeper:
            counter = {}
            counter[timestamp] = {}
            counter[timestamp][key] = 1
            self.keeper = counter
            print counter
        else:
            counter = self.keeper
            print counter
            if counter.has_key(timestamp):
                if counter[timestamp].has_key(key):
                    counter[timestamp][key] += 1
                else:
                    counter[timestamp][key] = 1
                self.keeper = counter
            else:
                if self.queue.full():
                    self.send()
                    self.queue.put(counter)
                else:
                    self.queue.put(counter)

                new_counter = {}
                new_counter[timestamp] = {}
                new_counter[timestamp][key] = 1
                self.keeper = new_counter
                print self.queue.qsize()

    def timing(self):
        pass

    def format(self):
        pass

    def send(self):
        pass

gra = Graphite()
for i in range(1,100):
    gra.incr('wb_feeds', int(time.time()))
    gra.incr('single_page', int(time.time()))
    time.sleep(0.001)
