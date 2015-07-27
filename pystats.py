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

    def __init__(self, path, begin, sleep=0.001, reopen_count=5):
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

    def __init__(self, stats, category):
        self.stats = stats
        self.category = category
        self.operator = {'access':self.__access,
                         'error':self.__error,
                         'subreq':self.__subreq}
        process = self.operator.get(self.category, False)
        if not process:
            print('No such category: [access]|[error]|[subreq]')
            sys.exit(0)
        else:
            self.process = process

        self.access_reg = re.compile('(GET|POST) /([^ "\?]+).* HTTP/1\.1.*\^"([^"]+)"\^ \^"')
        self.error_reg = re.compile('(GET|POST) /([^ "\?]+).* HTTP/1\.1')
        self.error_subreq_reg = re.compile('subrequest: "/([^ "\?]+).*upstream')
        self.subreq_reg = re.compile('\[.*\] /([^ ]+) ([^ ]+) ([^ ]+) \[')

    def adjust(self, interface):
        if interface[:1] == '/':
            interface = interface[1:]
        interface = interface.replace('.', '-')
        return interface

    def __access(self, line):
        result = self.access_reg.search(line)
        if result != None:
            interface = result.group(2)
            interface = self.adjust(interface)
            qps_key = 'query_per_second.' + interface
            self.stats.incr(qps_key)

            time = float(result.group(3))
            time_key = 'response_time.' + interface
            self.stats.timing(time_key, time*1000)

    def __error(self, line):
        result = self.error_reg.search(line)
        if result != None:
            interface = result.group(2)
            interface = self.adjust(interface)
            key = 'error.' + interface
            self.stats.incr(key)
        else:
            return

        result = self.error_subreq_reg.search(line)
        if result != None:
            subrequest = result.group(1)
            subrequest = self.adjust(subrequest)
            key = key + '_subreqs.' + subrequest 
            self.stats.incr(key)

    def __subreq(self, line):
        result = self.subreq_reg.search(line)
        if result != None:
            interface = result.group(1)
            interface = self.adjust(interface)
            status = int(result.group(2))
            time = float(result.group(3))

            qps_key = 'subreq.query_per_second.' + interface
            self.stats.incr(qps_key)
            time_key = 'subreq_response_time.' + interface
            self.stats.timing(time_key, time*1000)
            if status != 300:
                exception_status_key = 'subreq.exception_status.' + interface
                self.stats.incr(exception_status_key)

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
    stats = statsd.StatsClient(host, port, prefix=None, maxudpsize=512)
    log_parser = Parser(stats, category)
    tail = Tail(path, begin)
    try: 
        tail.open()
        for line in tail:
            #transport(line, host, port)
            log_parser.process(line)
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
