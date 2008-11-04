#!/usr/bin/python
# coding: utf8

import os
import sys
from server import Server
from protocal import *
from util import *

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


class MetaService:
    """
    object id: sha1 of path, everything is object, include attrs, metas, dir, file
    
    all object except data chunk object are stored on meta dev. There are only 
    one metadev in the whole system.
    
    """

    def ls(self, req):
        r = OODict()
        f = req.file
        if not os.path.exists(f):
            r.error = 'no such file'
        else:
            r.files = os.listdir(f)
            r.payload = 'payload1234567890'
        return r

class ChunkService:
    pass

class StorageManager:
    """
    Load storage pool cache informations from ~/.ideerfs/storage_pool.cache at
    startup.
    
    """
    
    def __init__(self):
        self.cache_file = 'storage_pool.cache'
        self.cm = ConfigManager(os.path.expanduser('~/.ideerfs/'))
        self.cache = self.cm.load(self.cache_file, OODict())
        self.meta_dev = None
        
        # Compute status
        self.statistics = OODict()
        self.statistics.size = 0
        self.statistics.used = 0
        if self.cache:
            for k, v in self.cache.items():
                # Change dev dict to OODict for later convenience
                if not isinstance(v, OODict):
                    v = OODict(v)
                    self.cache[k] = v
                
                if v.data_type == 'meta' and self.meta_dev is None:
                    self.meta_dev = v
                self.statistics.size += v.size
                self.statistics.used += v.used
        
    def online(self, req):
        """Add dev into pool, return error if it's added already"""
        # Make sure you add one and only one meta dev        
        response = OODict()
        dev = req.dev
        if dev.id in self.cache:
            response.error = 'dev exists'
            return response
            
        # Set meta dev
        if dev.data_type == 'meta':
            if self.meta_dev is not None:
                response.error = 'meta_dev exists'
                return response
            self.meta_dev = dev
            
        # Update statistics
        self.cache[dev.id] = dev
        self.statistics.size += dev.size
        self.statistics.used += dev.used
        dev.status = 'online'
        self.cm.save(self.cache, self.cache_file)
        return response
        
    def offline(self, req):
        """offline dev which id matches dev_id, data on it not available"""
        response = OODict()
        if req.dev_id not in self.cache:
            response.error = 'dev not exists'
            return response
        
        dev = self.cache[req.dev_id]
        if dev.data_type == 'meta':
            self.meta_dev = None

        self.statistics.size -= dev.size
        self.statistics.used -= dev.used
        del self.cache[req.dev_id]
        self.cm.save(self.cache, self.cache_file)
        return response
        
    def remove(self, req):
        pass
        
    def frozen(self, req):
        response = OODict()
        if req.dev_id in self.cache:
            self.cache[req.dev_id].status = 'frozen'
        else:
            response.error = 'dev not exists'
        self.cm.save(self.cache, self.cache_file)
        return response
        
    def stat(self, req):
        response = OODict()
        response.statistics = self.statistics
        response.meta_dev = self.meta_dev
        return response

    

if __name__ == '__main__':
    server = NIOServer()
    server.register('meta', MetaService())
    server.register('chunk', ChunkService())
    server.register('storage', StorageManager())
    server.bind('localhost', 1984)
    server.mainloop()
