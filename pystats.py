#!/usr/bin/env python

import optparse
import os
import sys
import time
import re
import statsd

class Error(Exception): pass
class FileError(Error): pass

class Tail(object):

    def __init__(self, path, begin, sleep=0.01, reopen_count=5):
        self.path = path
        self.begin = begin
        self.sleep = sleep
        self.reopen_count = reopen_count

    def __iter__(self):
        while True:
            pos = self.file.tell()
            line = self.file.readline()
            if not line:
                self.wait(pos)
            else:
                yield line
    
    def open(self, tail=True):
        try:
            self.real_path = os.path.realpath(self.path)
            self.inode = os.stat(self.path).st_ino
        except OSError, error:
            raise FileError(error)
        try:
            self.file = open(self.real_path)
        except IOError, error:
            raise FileEreror(error)

        if tail:
            self.file.seek(0, self.begin)
    
    def close(self):
        try:
            self.file.close()
        except Exception:
            pass

    def check(self, pos):
        try:
            if self.real_path != os.path.realpath(self.path):
                return True
            stat = os.stat(self.path)
            if self.inode != stat.st_ino:
                return True
            if pos > stat.st_size:
                return True
        except OSError:
            return True

        return False

    def reopen(self):
        self.close() 
        reopen_count = self.reopen_count
        while reopen_count >= 0:
            try:
                self.open(tail=False)
                return True
            except FileError:
                time.sleep(self.sleep*10)
            reopen_count -= 1
        return False

    def wait(self, pos):
        if self.check(pos):
            if not self.reopen():
                time.sleep(self.sleep*10)
        else:
            self.file.seek(pos)
            time.sleep(self.sleep)

class Parser(object):

    def __init__(self, category):
        self.category = category
        self.operator = {'access':self.__access,
                         'error':self.__error,
                         'response':self.__response,
                         'subreq':self.__subreq}
        process = self.operator.get(self.category, False)
        if not process:
            print('no such category')
            sys.exit(0)
        else:
            self.process = process

    def __access(self, line):
        result = re.search('(?<=GET /)[^?]+', line)
        if result == None:
            result = re.search('(?<=POST /)[^?]+', line)
        if result != None:
            interface = result.group(0)

        result = re.search('(?<=\^")[^"]+(?="\^ \^")', line)
        if result != None:
            response_time = result.group(0)

    def __error(self, line):
        result = re.search('(?<=GET /)[^?]+', line)
        if result == None:
            result = re.search('(?<=POST /)[^?]+', line)
        if result != None:
            interface = result.group(0)

        result = re.search('(?<=subrequest: "/)[^"]+', line)
        if result != None:
            subrequest = result.group(0)

    def __response(self, line):
        result = re.search('(?<=GET /)[^?]+', line)
        if result == None:
            result = re.search('(?<=POST /)[^?]+', line)
        if result != None:
            interface = result.group(0)

    def __subreq(self, line):
        pass

def transport(line, host, port):
    result = re.search('(?<=GET /)[\w/]+', line)
    if result == None:
        result = re.search('(?<=POST /)[\w/]+', line)
    if result != None:
        interface = result.group(0)
    else:
        return False

    stats = statsd.StatsClient(host, port, prefix=None, maxudpsize=512)
    stats.incr(interface)

def handle(path, begin, category, host='127.0.0.1', port=8125):
    tail = Tail(path, begin)
    try: 
        tail.open()
        for line in tail:
            transport(line, host, port)
    finally:
        tail.close()

if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('-f', '--file', 
                      dest='file',
                      help='file to tail into statsD',
                      metavar='FILE',)
    parser.add_option('-H', '--host',
                      dest='host',
                      default='127.0.0.1',
                      help='destination StatsD host server',
                      metavar='HOST',)
    parser.add_option('-p', '--port',
                      dest='port',
                      type='int',
                      default=8125,
                      help='destination StatsD port',
                      metavar='PORT',)
    parser.add_option('-b', '--begin',
                      dest='begin',
                      type='int',
                      default='2',
                      help='where does tail begin, 0 means beginning, 1 means current, 2 means end',)
    parser.add_option('-c', '--category',
                      dest='category',
                      default='access',
                      help='which category of file to collect',)
    options, args = parser.parse_args()

    if options.file:
        try:
            handle(path=options.file, 
                   begin=options.begin,
                   category=options.category,
                   host=options.host,
                   port=options.port)
        except KeyboardInterrupt:
            sys.exit(0)
    else:
        parser.print_help()
