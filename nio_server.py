#!/usr/bin/python
# coding: utf8

import os
import sys
from server import Server
from protocal import *
from util import *
from dev import *
import time
import hashlib

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

    def __init__(self, path):
        self.storage_pool = None
        self.dev = Dev(path)
        if not self.dev.config:
            print 'not formatted', path
            sys.exit(-1) # Fatal error
        
        self.seq_file = 'seq'
        self.meta_dir = 'META'
        
        self.root_id = 1

    def _next_seq(self):
        # We should get a multi thread lock here to protect 'SEQ' file
        seq = self.dev.config_manager.load(self.seq_file)
        if seq is None:
            return None
        seq += 1
        self.dev.config_manager.save(seq, self.seq_file)
        # Make sure successfully saved
        #return hashlib.sha1.hexdigest(str(seq))
        return seq
    
    def _id2path(self, id):
        hash = hashlib.sha1(str(id)).hexdigest()
        return os.path.join(hash[:3], hash[3:6], hash[6:])

    def _get_object(self, id):
        return self.dev.config_manager.load(os.path.join(self.meta_dir, self._id2path(id)))

    def _save_object(self, obj):
        self.dev.config_manager.save(obj, os.path.join(self.meta_dir, self._id2path(obj.id)))

    def _lookup(self, file):
        """Find a absolute path"""
        if file == '/':
            return self._get_object(self.root_id)
        names = file.split('/')
        names.pop(0) # Remove
        parent_id = self.root_id
        for name in names:
            parent = self._get_object(parent_id)
            if not parent or parent.type != 'dir':
                return None
            if name not in parent.children:
                return None
            id = parent.children[name]
            parent = id
        return self._get_object(id)

    
    def _new_obj(self, id, type, parent):
        obj = OODict()
        obj.id = id
        obj.type = type
        obj.meta = {
            'ctime': '%d' % time.time(),
            #'name': dir
        }
        if type == 'dir':
            obj.children = {
                '.': id,
                '..': parent
            }
        return obj
        

    def mkdir(self, req):
        """how to deal with root dir /"""
        response = OODict()
        dir = os.path.normpath(req.dir)
        if dir == '/':
            if self._lookup(dir):
                response.error = 'root exists'
                return response
            else:
                # Make root
                id = self._next_seq()
                if not id:
                    response.error = 'next seq error'
                    return response
                obj = self._new_obj(id, 'dir', id)
                self._save_object(obj)
                response.id = id
                return response

        parent_name = os.path.dirname(dir)
        myname = os.path.basename(dir)
        
        parent = self._lookup(parent_name)
        if parent is None:
            response.error = 'no such file or directory: %s' % parent_name
            return response
        if parent.type != 'dir':
            response.error = 'not a directory: %s' % parent_name
            return response
        if myname in parent.children:
            response.error = 'dir exists'
            return response
 
        id = self._next_seq()
        if not id:
            response.error = 'next seq error'
            return response
        
        obj = self._new_obj(id, 'dir', parent.id)
        parent.children[myname] = id
        self._save_object(parent)
        self._save_object(obj)
        response.id = id
        return response

    def exists(self, req):
        response = OODict()
        response.file = self._lookup(req.file)
        return response

    def create(self, req):
        # Create files
        pass
        

    def lsdir(self, req): 
        response = OODict()
        obj = self._lookup(req.dir)
        if obj is None:
            response.error = 'no such directory'
            return response
        if obj.type != 'dir':
            response.error = 'not a directory'
        # This might be very large!
        response.children = obj.children
        return response          


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
        return response

    

if __name__ == '__main__':
    server = NIOServer()
    server.register('meta', MetaService('/data/sda'))
    server.register('chunk', ChunkService())
    server.register('storage', StorageManager())
    # Meta service and Storage Service are on the same node
    server.services['meta'].storage_pool = server.services['storage']
    
    server.bind('localhost', 1984)
    server.mainloop()
