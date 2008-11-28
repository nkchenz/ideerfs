#!/usr/bin/python
# coding: utf8

from util import *
from dev import *
import time
import hashlib

import random

class StorageService(Service):
    """
    Receive chunk infos from chunk server at startup
    """
    
    def __init__(self, addr):
        self.meta_service_addr = addr
        
        self.cache = OODict()

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
    
    def locate(self, req):
        """find which hosts contain the given devices"""
        devs = {}
        for id in req.dev_ids:
            if id in self.cache:
                devs[id] = self.cache[id]
        return devs
    
