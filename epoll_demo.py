"""Server using epoll method"""

import os
import select
import socket
import time

addr = ('localhost', 8989)

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(addr)
s.listen(8)
s.setblocking(0) # Non blocking socket server
epoll = select.epoll()
epoll.register(s.fileno(), select.EPOLLIN) # Level triggerred

cs = {}
data = ''
while True:
    time.sleep(1)
    events = epoll.poll(1) # Timeout 1 second
    print 'Polling %d events' % len(events)
    for fileno, event in events:
        if fileno == s.fileno():
            sk, addr = s.accept()
            sk.setblocking(0)
            print addr
            cs[sk.fileno()] = sk
            epoll.register(sk.fileno(), select.EPOLLIN)

        elif event & select.EPOLLIN:
            data = cs[fileno].recv(4)
            print 'recv ', data
            epoll.modify(fileno, select.EPOLLOUT)
        elif event & select.EPOLLOUT:
            print 'send ', data
            cs[fileno].send(data)
            data = ''
            epoll.modify(fileno, select.EPOLLIN)

        elif event & select.EPOLLHUP:
            print 'hup'
            epoll.unregister(fileno)

