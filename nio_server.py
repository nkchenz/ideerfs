#!/usr/bin/python
# coding: utf8

import os
import sys
from server import Server
from protocal import *
from util import *
from exception import *

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
                
                debug(req)
                
                service, method = req.method.split('.')
                r = OODict()
                if service not in self.services:
                    r.error = 'unknown serice %s' % service
                else:
                    try:
                        handler = getattr(self.services[service], method)
                        try:
                            returns = handler(req)
                            # For complicated calls, read for example, should return a tuple: value, payload
                            if isinstance(returns, tuple):
                                r.value, r.payload = returns
                            else:
                                r.value = returns
                        except RequestHandleError, err:
                            #print dir(err)
                            r.error = err.message
                    except AttributeError:
                        r.error = 'unknown method %s' % method
                        
                r._id = req._id
                print r
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
