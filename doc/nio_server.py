#!/usr/bin/python
# coding: utf8

import os
import sys
from server import Server



class NIOServer(Server):
    """
    NIO server
    """
    def request_handler(self, conn):
        f, addr = conn
        print 'Connected from', addr
        while True:
                data = f.recv(1024)
                if not data:
                    break
                f.send(data)
                print data,
        f.close()
        print 'Bye', addr


MetaServer
ChunkServer
Ed2kServer

if __name__ == '__main__':
    server = NIOServer()
    server.bind('localhost', 1984)
    server.mainloop()
