#!/usr/bin/python
# coding: utf8

import os
import sys
from server import Server
from protocal import *

from meta import MetaService
from storage import StorageManager
from chunk import ChunkService

class NIOServer(Server):
    """
    NIO server
    """
    def __init__(self):
        Server.__init__(self)
        self.services = {}
        pass

    def register(self, name, waiter):
        self.services[name] = waiter

    def request_handler(self, conn):
        f, addr = conn
        print 'Connected from', addr
        while True:

                req = read_message(f)
                # Let client care about erros, retrans as needed
                if not req: 
                    break
                service, method = req.method.split('.')

                r = OODict()

                if service not in self.services:
                    r.error = 'unknown serice %s' % service
                else:
                    handler = getattr(self.services[service], method)
                    if not handler:
                        r.error = 'unknown method %s' % method
                    else:
                        r = handler(req)

                r._id = req._id
                send_message(f, r) 

        f.close()
        print 'Bye', addr

if __name__ == '__main__':
    server = NIOServer()
    server.register('meta', MetaService('/data/sda'))
    server.register('chunk', ChunkService())
    server.register('storage', StorageManager())
    # Meta service and Storage Service are on the same node
    server.services['meta'].storage_pool = server.services['storage']
    
    server.bind('localhost', 1984)
    server.mainloop()
