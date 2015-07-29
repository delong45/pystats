#!/usr/bin/env python

import socket
import sys
import Queue
import time

class Graphite(object):

    def __init__(self, host='127.0.0.1', port=2003, size=60):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.host = host
        self.port = port
        self.counter_keeper = False
        self.counter_queue = Queue.Queue(size)
        self.timer_keeper = False
        self.timer_queue = Queue.Queue(size)
        self.connect()

    def connect(self):
        self.sock.connect((self.host, self.port))

    def reconnect(self):
        self.sock.close()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect()

    def incr(self, key, timestamp):
        if not self.counter_keeper:
            counter = {}
            counter[timestamp] = {}
            counter[timestamp][key] = 1
            self.counter_keeper = counter
        else:
            counter = self.counter_keeper
            if counter.has_key(timestamp):
                if counter[timestamp].has_key(key):
                    counter[timestamp][key] += 1
                else:
                    counter[timestamp][key] = 1
                self.counter_keeper = counter
            else:
                if self.counter_queue.full():
                    self.send('counter')
                    self.counter_queue.put(counter)
                else:
                    self.counter_queue.put(counter)

                new_counter = {}
                new_counter[timestamp] = {}
                new_counter[timestamp][key] = 1
                self.counter_keeper = new_counter

    def timing(self, key, value, timestamp):
        if not self.timer_keeper:
            timer = {}
            timer[timestamp] = {}
            timer[timestamp][key] = {}
            timer[timestamp][key]['count'] = 1
            timer[timestamp][key]['sum'] = value
            timer[timestamp][key]['list'] = []
            timer[timestamp][key]['list'].append(value)
            self.timer_keeper = timer
        else:
            timer = self.timer_keeper
            if timer.has_key(timestamp):
                if timer[timestamp].has_key(key):
                    timer[timestamp][key]['count'] += 1
                    timer[timestamp][key]['sum'] += value
                    timer[timestamp][key]['list'].append(value)
                else:
                    timer[timestamp][key] = {}
                    timer[timestamp][key]['count'] = 1
                    timer[timestamp][key]['sum'] = value
                    timer[timestamp][key]['list'] = []
                    timer[timestamp][key]['list'].append(value)
                self.timer_keeper = timer
            else:
                if self.timer_queue.full():
                    self.send('timer')
                    self.timer_queue.put(timer)
                else:
                    self.timer_queue.put(timer)

                new_timer = {}
                new_timer[timestamp] = {}
                new_timer[timestamp][key] = {}
                new_timer[timestamp][key]['count'] = 1
                new_timer[timestamp][key]['sum'] = value
                new_timer[timestamp][key]['list'] = []
                new_timer[timestamp][key]['list'].append(value)
                self.timer_keeper = new_timer

    def counter_format(self, element):
        msg = ''
        time = element.keys()
        timestamp = time[0]
        key_list = element[timestamp].keys()
        for key in key_list:
            value = element[timestamp][key]
            line = key + ' ' + str(value) + ' ' + str(timestamp) + '\n'
            if msg:
                msg += line
            else:
                msg = line
        return msg

    def timer_format(self, element):
        msg = ''
        time = element.keys()
        timestamp = time[0]
        key_list = element[timestamp].keys()
        for key in key_list:
            count = element[timestamp][key]['count']
            sum = element[timestamp][key]['sum']
            value = float(sum) / float(count)
            line = key + '.mean ' + str(value) + ' ' + str(timestamp) + '\n'
            if msg:
                msg += line
            else:
                msg = line
        return msg

    def send(self, category):
        if category == 'counter':
            queue = self.counter_queue
        elif category == 'timer':
            queue = self.timer_queue
            
        while not queue.empty():
            element = queue.get()
            if category == 'counter':
                msg = self.counter_format(element)
            elif category == 'timer':
                msg = self.timer_format(element)

            try:
                n = self.sock.send(msg)
            except Exception:
                self.reconnect()
