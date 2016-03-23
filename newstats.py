#!/usr/bin/env python

import optparse
import os
import sys
import time
import re
import cache

class Error(Exception): pass
class FileError(Error): pass

class Tail(object):

    def __init__(self, path, begin, sleep=1, reopen_count=5):
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
                time.sleep(self.sleep)
            reopen_count -= 1
        return False

    def wait(self, pos):
        if self.check(pos):
            if not self.reopen():
                time.sleep(self.sleep)
        else:
            self.file.seek(pos)
            time.sleep(self.sleep)

class Parser(object):

    def __init__(self, stats, category, ip):
        self.stats = stats
        self.category = category
        self.ip = ip
        self.operator = {'access':self.__access,
                         'error':self.__error,
                         'subreq':self.__subreq}
        process = self.operator.get(self.category, False)
        if not process:
            print('No such category: [access]|[error]|[subreq]')
            sys.exit(0)
        else:
            self.process = process

        self.access_reg = re.compile('\[([^ ]+) .*\].*(GET|POST) /([^ "\?]+).* HTTP/1\.1.*\^"([^"]+)"\^ \^"')
        self.error_reg = re.compile('(.+) \[error\].*(GET|POST) /([^ "\?]+).* HTTP/1\.1.*subrequest: "/([^ "\?]+).*upstream')
        self.subreq_reg = re.compile('\[([^ ]+) .*\] /([^ ]+) ([^ ]+) ([^ ]+) \[')

    def adjust(self, interface):
        interface = interface.replace('.', '-')
        interface = interface.replace('/', '-')
        return interface

    def __access(self, line):
        result = self.access_reg.search(line)
        if result != None:
            nowtime = result.group(1)
            timearray = time.strptime(nowtime, "%d/%b/%Y:%H:%M:%S")
            timestamp = int(time.mktime(timearray))

            interface = result.group(3)
            interface = self.adjust(interface)
            qps_key = 'uve.access.qps.' + interface
            self.stats.incr(qps_key, timestamp)

            rtime = float(result.group(4))
            time_key = 'uve.access.response_time.' + interface
            self.stats.timing(time_key, timestamp, rtime*1000)

    def __error(self, line):
        result = self.error_reg.search(line)
        if result != None:
            nowtime = result.group(1)
            timearray = time.strptime(nowtime, "%Y/%m/%d %H:%M:%S")
            timestamp = int(time.mktime(timearray))

            interface = result.group(3)
            interface = self.adjust(interface)
            subrequest = result.group(4)
            subrequest = self.adjust(subrequest)
            key = 'uve.error.' + interface + '_subreqs_qps.' + subrequest
            self.stats.incr(key, timestamp)

    def __subreq(self, line):
        result = self.subreq_reg.search(line)
        if result != None:
            nowtime = result.group(1)
            timearray = time.strptime(nowtime, "%d/%b/%Y:%H:%M:%S")
            timestamp = int(time.mktime(timearray))

            interface = result.group(2)
            interface = self.adjust(interface)
            status = int(result.group(3))
            rtime = float(result.group(4))

            qps_key = 'uve.subreq.qps.' + interface
            self.stats.incr(qps_key, timestamp)
            time_key = 'uve.subreq.response_time.' + interface
            self.stats.timing(time_key, timestamp, rtime*1000)
            if status != 200:
                exception_status_key = 'uve.subreq.exception_status_qps.' + interface
                self.stats.incr(exception_status_key, timestamp)

            if self.ip != 'localhost':
                ip = self.ip.replace('.', '_')
                percentile_key = 'uve.subreq.percentile.' + interface + '.' + ip
                self.stats.percentile(percentile_key, timestamp, rtime*1000)


def handle(path, begin, category, host='127.0.0.1', port=2003, ip='locahost'):
    stats = cache.vCache(host, port)
    log_parser = Parser(stats, category, ip)
    tail = Tail(path, begin)
    try: 
        tail.open()
        for line in tail:
            log_parser.process(line)
    finally:
        tail.close()

if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('-f', '--file', 
                      dest='file',
                      help='file to tail into Server',
                      metavar='FILE',)
    parser.add_option('-H', '--host',
                      dest='host',
                      default='10.77.96.122',
                      help='destination Server host server',
                      metavar='HOST',)
    parser.add_option('-p', '--port',
                      dest='port',
                      type='int',
                      default=33333,
                      help='destination Server port',
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
    parser.add_option('-i', '--ip',
                     dest='ip',
                     default='localhost',
                     help='ip of local host',)
    options, args = parser.parse_args()

    if options.file:
        try:
            handle(path=options.file, 
                   begin=options.begin,
                   category=options.category,
                   host=options.host,
                   port=options.port,
                   ip=options.ip)
        except KeyboardInterrupt:
            sys.exit(0)
    else:
        parser.print_help()
