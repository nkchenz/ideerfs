#!/usr/bin/python
# coding: utf8

import os
import sys
from server import Server
from protocal import *

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
                r.id = req.id
                r.return_value = OODict()
                if service not in self.services:
                    r.return_value.error = 'unknown serice %s' % service
                else:
                    handler = getattr(self.services[service], method)
                    if not handler:
                        r.return_value.error = 'unknown method %s' % method
                    else:
                        r.return_value = handler(req)
                payload = None
                if 'payload' in r.return_value:
                    payload = r.return_value.payload
                    del r.return_value.payload
                send_message(f, r, payload) 
        f.close()
        print 'Bye', addr


class MetaService:

    def ls(self, req):
        r = OODict()
        f = req.args.file
        if not os.path.exists(f):
            r.error = 'no such file'
        else:
            r.files = os.listdir(f)
        return r

class ChunkService:
    pass

if __name__ == '__main__':
    server = NIOServer()
    server.register('meta', MetaService())
    server.register('chunk', ChunkService())
    server.bind('localhost', 1984)
    server.mainloop()
