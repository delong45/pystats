#!/usr/bin/env python

import os
import threading
import select
import socket

class Forkcore(object):

    def __init__(self, worker, port=3333):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("", port))
        s.listen(5000)
        self.s = s
        self.conn_list = []
        self.worker = worker
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
        epoll = select.epoll()
        epoll.register(self.s.fileno(), select.EPOLLIN|select.EPOLLET)
        while True:
            epoll_list = epoll.poll()
            for fd,events in epoll_list:
                if fd == self.s.fileno():
                    conn,addr = self.s.accept()
                    epoll.register(conn.fileno(), select.EPOLLIN|select.EPOLLET)
                    self.conn_list.append(conn)
                else:
                    for conn in self.conn.list:
                        if fd == conn.fileno():
                            self.worker(conn)
