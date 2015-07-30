#!/usr/bin/env python

import socket
import sys
import time

class Cache(object):

    def __init__(self, host='127.0.0.1', port=2003, max_size=120):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.max_size = max_size
        self.counter_cache = {}
        self.timer_cache = {}
        self.connect()

    def connect(self):
        self.sock.connect((self.host, self.port))

    def reconnect(self):
        self.sock.close()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect()

    def incr(self, key, timestamp, value=1):
        if self.counter_cache.has_key(timestamp):
            if self.counter_cache[timestamp].has_key(key):
                self.counter_cache[timestamp][key] += value
            else:
                self.counter_cache[timestamp][key] = value
        else:
            if self.is_full('counter'):
                self.send('counter')
            self.counter_cache[timestamp] = {}
            self.counter_cache[timestamp][key] = value

    def create_timer(self, value):
        timer = {}
        timer['count'] = 1
        timer['sum'] = value
        timer['list'] = []
        timer['list'].append(value)
        return timer

    def timing(self, key, timestamp, value):
        if self.timer_cache.has_key(timestamp):
            if self.timer_cache[timestamp].has_key(key):
                self.timer_cache[timestamp][key]['count'] += 1
                self.timer_cache[timestamp][key]['sum'] += value
                self.timer_cache[timestamp][key]['list'].append(value)
            else:
                new_timer = self.create_timer(value)
                self.timer_cache[timestamp][key] = {}
                self.timer_cache[timestamp][key] = new_timer
        else:
            if self.is_full('timer'):
                self.send('timer')
            new_timer = self.create_timer(value)
            self.timer_cache[timestamp] = {}
            self.timer_cache[timestamp][key] = {}
            self.timer_cache[timestamp][key] = new_timer

    def is_full(self, category):
        if category == 'counter':
            if len(self.counter_cache) == self.max_size:    
                return True
        elif category == 'timer':
            if len(self.timer_cache) == self.max_size:
                return True
        return False

    def counter_format(self, timestamp, item):
        msg = ''
        keys = item.keys()
        for key in keys:
            value = item[key]
            line = key + ' ' + str(value) + ' ' + str(timestamp) + '\n'
            if msg:
                msg += line
            else:
                msg = line
        return msg

    def timer_format(self, timestamp, item):
        msg = ''
        keys = item.keys()
        for key in keys:
            count = item[key]['count']
            sum = item[key]['sum']
            value = float(sum) / float(count)
            line = key + '.mean ' + str(value) + ' ' + str(timestamp) + '\n'
            if msg:
                msg += line
            else:
                msg = line
        return msg

    def send(self, category):
        if category == 'counter':
            cache = self.counter_cache
        elif category == 'timer':
            cache = self.timer_cache

        keys = cache.keys()
        keys.sort()
        for key in keys:
            if len(cache) <= (self.max_size/2):
                break
            item = cache.pop(key)
            if category == 'counter':
                msg = self.counter_format(key, item)
            elif category == 'timer':
                msg = self.timer_format(key, item)
            try:
                self.sock.send(msg)
            except Exception:
                self.reconnect()
