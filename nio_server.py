"""
Network IO server, based on the 'Server' class
"""

import os
import sys
from server import Server
from protocol import *
from util import *
from exception import *

from meta import MetaService
from storage import StorageService
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
        debug('%s service registered' % name)

    def request_handler(self, conn):
        f, addr = conn
        print 'Connected from', addr
        while True:

                req = read_message(f)
                # Let client care about errors, retrans as needed
                if not req: 
                    break
                
                debug('Request:', req)
                
                service, method = req.method.split('.')
                r = OODict()
                error = ''
                if service not in self.services:
                    error = 'unknown service %s' % service
                else:
                    try:
                        handler = getattr(self.services[service], method)
                        if not callable(handler):
                            error = 'not callable method %s' % method
                    except AttributeError:
                        error = 'unknown method %s' % method

                if not error:
                    try:
                        returns = handler(req)
                        # For complicated calls, read for example, should return a tuple: value, payload
                        if isinstance(returns, tuple):
                            r.value, r.payload = returns
                        else:
                            r.value = returns
                    except RequestHandleError, err:
                        #print dir(err)
                        error = err.message

                if error:
                    r.error = error
                r._id = req._id # Response has the same id as request
                debug('Response:', r)
                send_message(f, r) 

        f.close()
        print 'Bye', addr
