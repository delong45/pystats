#!/usr/bin/env python

import os
import threading
import select
import socket

class Forkcore(object):

    def __init__(self, time_worker, qps_worker, port=3333):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("", port))
        s.listen(5000)
        self.s = s
        self.connections = {}
        self.incomplete_line = ''
        self.unread_line = ''
        self.left_line = ''
        self.time_worker = time_worker
        self.qps_worker = qps_worker
        self.process()
    
    def process(self, child_process_num=1):
        pid = os.getpid()
        for _ in range(0, child_process_num):
            if pid == os.getpid():
                if os.fork():
                    pass
                else:
                    self.thread()
    
    def thread(self, thread_num=1):
        for _ in range(0, thread_num):
            t = threading.Thread(target=self.epoll)
            t.setDaemon(1)
            t.start()
            t.join()
    
    def epoll(self):
        self.epoll = select.epoll()
        self.epoll.register(self.s.fileno(), select.EPOLLIN|select.EPOLLET)
        while True:
            epoll_list = self.epoll.poll()
            for fd,events in epoll_list:
                if fd == self.s.fileno():
                    conn,addr = self.s.accept()
                    self.epoll.register(conn.fileno(), select.EPOLLIN)
                    self.connections[conn.fileno()] = conn
                else:
                    conn = self.connections[fd]
                    self.worker(conn)

    def worker_process(self, line):
        if line.find('qps') != -1:
            self.qps_worker(line)
        if line.find('time') != -1:
            self.time_worker(line)

    def worker(self, conn):
        msg = conn.recv(1024)
        if msg == '':
            self.epoll.unregister(conn.fileno())
            self.connections.pop(conn.fileno())    
            conn.close()

        if self.incomplete_line:
            pos = msg.find('\n')
            self.unread_line = msg[:pos]
            msg = msg[pos+1:]
            self.left_line = self.incomplete_line + self.unread_line
            
        pos = msg.rfind('\n')
        self.incomplete_line = msg[pos+1:]
        msg = msg[:pos]
        lines = msg.split('\n')
        for line in lines:
            self.worker_process(line)
        self.worker_process(self.left_line)
