from util import *
from dev import *
import time
import hashlib

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
        response.statistics.total_disks = len(self.cache)
        response.statistics.invalid_disks = 0
        return response
