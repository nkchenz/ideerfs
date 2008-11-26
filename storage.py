#!/usr/bin/python
# coding: utf8

from util import *
from dev import *
import time
import hashlib

import random

class StorageService(Service):
    """
    Load storage pool cache informations from ~/.ideerfs/storage_pool.cache at
    startup.
    
    """
    
    def __init__(self, ip, port):
        self.cache_file = 'storage_pool.cache'
        self.cm = ConfigManager(os.path.expanduser('~/.ideerfs/'))
        self.cache = self.cm.load(self.cache_file, OODict())
        
        self.meta_service_ip = ip
        self.meta_service_port = port
        
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
    
    def _flush(self):
        self.cm.save(self.cache, self.cache_file)

    def online(self, req):
        """Add dev into pool, return error if it's added already"""
        # Make sure you add one and only one meta dev        
        dev = req.dev
        if dev.id in self.cache:
            self._error('dev exists')
        # Update statistics
        self.cache[dev.id] = dev
        self.statistics.size += dev.size
        self.statistics.used += dev.used
        dev.status = 'online'
        self._flush()
        return 0
        
    def offline(self, req):
        """offline dev which id matches dev_id, data on it not available"""
        if req.dev_id not in self.cache:
            self._error('dev not exists')
        dev = self.cache[req.dev_id]
        self.statistics.size -= dev.size
        self.statistics.used -= dev.used
        del self.cache[req.dev_id]
        self._flush()
        return 0
        
    def remove(self, req):
        pass
        
    def frozen(self, req):
        if req.dev_id not in self.cache:
            self._error('dev not exists')
            
        dev = self.cache[req.dev_id]
        dev.status = 'frozen'
        # The free space on frozen device is usable
        self.statistics.size -= (dev.size - dev.used)
        self._flush()
        return 0
        
    def stat(self, req):
        # Iterate dev cache to get realtime stat here, disk stat is carried with
        # chunk server's heart-beat message
        return  {'summary': self.statistics, 'disks': self.cache}
    
    def get_free_dev(self, req):
        """
        return n devices each having free space larger than size bytes
        """
        if req.size * req.n > self.statistics.size - self.statistics.used:
            self._error('no space available')
    
        debug('get_free_dev', req.size, req.n)
            
        # Alloc algorithm, we'd better has a list on which devs are sorted by
        # free space
        devs = {}
        all_dev_ids = self.cache.keys()
        random.shuffle(all_dev_ids)
        for dev_id in all_dev_ids:
            dev = self.cache[dev_id]
            if dev.status == 'online' and dev.size - dev.used > req.size:
                devs[dev_id] = dev
                if len(devs) >= req.n:
                    break
                
        if not devs:
            self._error('no free devs') # Find nothing, this should not happen
            
        debug(devs)
        
        # Maybe it's better not to caculate the size here. Because some writes
        # to the returned devices may fail, it's not accurate anyway
        # Update pool stat
        #self.statistics.used += req.chunk_size * len(devs)
        # Update dev stat
        #for id, dev in devs.items():
        #    self.cache[id].used += req.chunk_size
        #self._flush()
        # This alloc is just a suggestion devs, user will get no space error from
        # chunk server anyway
        return devs
        
    def free_chunk(self, req):
        pass
    
    def locate(self, req):
        """find which hosts contain the given devices"""
        devs = {}
        for id in req.dev_ids:
            if id in self.cache:
                devs[id] = self.cache[id]
        return devs
    
