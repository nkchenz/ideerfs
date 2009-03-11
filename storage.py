"""
Storage service: manage and allocate storage
"""
from util import *
from dev import *
import time
import hashlib

import random

class StorageService(Service):
    """
    Receive chunk infos from chunk server at startup
On startup, the Namenode enters a special state called Safemode. Replication of data blocks
does not occur when the Namenode is in the Safemode state. The Namenode receives
Heartbeat and Blockreport messages from the Datanodes. A Blockreport contains the list of
data blocks that a Datanode is hosting. Each block has a specified minimum number of
replicas. A block is considered safely replicated when the minimum number of replicas of
that data block has checked in with the Namenode. After a configurable percentage of safely
replicated data blocks checks in with the Namenode (plus an additional 30 seconds), the
Namenode exits the Safemode state. It then determines the list of data blocks (if any) that
still have fewer than the specified number of replicas. The Namenode then replicates these
blocks to other Datanodes.
    """
    
    def __init__(self, addr):
        self._addr = addr
        
        self.cache = OODict()
        self.nodes = {}
        self.deleted = defaultdict(list)
        
        # Get all chunks in the whole file system, how to delete chunk?
        self.cm = ConfigManager(os.path.expanduser('~/.ideerfs/'))
        # all_chunks stored all alive chunks in the entire system, it's critical important
        
        debug('loading chunks...')
        self.chunks_file = 'all_chunks'
        self.chunks = self.cm.load(self.chunks_file, OODict())
        self.chunks_map = defaultdict(set)
        debug('done')
        
    def _flush_chunks(self):
        self.cm.save(self.chunks, self.chunks_file)
        
    def _writeable(self, id):
        return self.cache[id].mode != 'frozen' 

    def _avaiable(self, id):
        if id not in self.cache:
            return False
        dev = self.cache[id]
        return self._is_alive(dev.host) and dev.status == 'online'

    def _is_alive(self, host):
        return host in self.nodes and time.time() - self.nodes[host].update_time < 120 
    
    def _free_enough(self, id, size):
        dev = self.cache[id]
        return dev.size - dev.used > size
    
    def _get_dev_addr(self, id):
        dev = self.cache[id]
        return self.nodes[dev.host].addr
    
    def malloc(self, req):
        """
        return n devices each having free space larger than size bytes
        """
        debug('malloc %s bytes on %d devices' % (req.size, req.n))
        
        # Alloc algorithm, we'd better has a list on which devs are sorted by free space
        rv = {}
        devs = self.cache.keys()
        random.shuffle(devs)
        for dev in devs:
            if self._avaiable(dev) and self._writeable(dev) and self._free_enough(dev, req.size):
                rv[dev] = self._get_dev_addr(dev)
                if len(rv) >= req.n:
                    break
        debug(rv)           
        
        if not rv:
            self._error('no dev avaiable') # Find nothing, this should not happen
        return rv

    
    def search(self, req):
        """Search chunks locations"""
        tmp = object_hash(req.file)
        rv = {} # Return value
        c = OODict()
    
        for chunk in req.chunks.keys():
            id = chunk_id(tmp, chunk)
            if id not in self.chunks:
                continue
        
            c.version = self.chunks[id]['v']
            c.locations = {}
            
            if id in self.chunks_map:
                for dev in self.chunks_map[id]: # Devices of replications
                    if self._avaiable(dev):
                        c.locations[dev] = self._get_dev_addr(dev)

            rv[chunk] = c
            
        return rv
        
    
    def free(self, req):
        """Delete chunks in dict deleted"""
        for fid, chunks in req.deleted.items():
            fhash = object_hash(fid)
            for chunk in chunks.keys():
                name = chunk_id(fhash, chunk)
                name2 = '.'.join([name, str(self.chunks[name]['v'])]) # Real chunk file name with version
                del self.chunks[name] # Delete index
                
                for dev in self.chunks_map[name]: # Delete all replications
                    self.deleted[dev].append(name2)
                del self.chunks_map[name]

        self._flush_chunks()
        return 'ok'
        
    def update(self, req):
        """Found object in devs, after writting, update chunk info"""
        chunk = chunk_id(object_hash(req.object_id), req.chunk_id)
        if chunk not in self.chunks:
            if req.new:
                self.chunks[chunk] = {'v': req.version, 'rf': req.rf}
                self.chunks_map[chunk] = set(req.devs)
                self._flush_chunks()
                return 'ok'
            else:
                self._error('unknown chunk')
        
        if req.new:
            self._error('chunk exists, cant be new')
                
        v = self.chunks[chunk]['v']
        if req.version < v:
            self._error('stale chunk')
        if req.version > v:
            self.chunks[chunk]['v'] = req.version
            self.chunks_map[chunk] = set(req.devs)
        else:
            self.chunks_map[chunk].union(set(req.devs))
            
        return 'ok'


    def hb(self, req):
        # Update nodes healthy
        host, _ = req.addr
        if host not in self.nodes:
            self.nodes[host] = OODict({'devs': set([])})
        self.nodes[host].addr = req.addr
        self.nodes[host].update_time = time.time()
            
        # Update changed devs
        for id, dev in req.changed_devs.items(): # dev here is a dict
            if not dev and id in self.cache: # Removed
                del self.cache[id]
                if id in self.nodes[host].devs:
                    self.nodes[host].devs.remove(id)
            else:
                dev['host'] = host # Remember the host it came from
                self.cache[id] = OODict(dev)
                self.nodes[host].devs.add(id)
        
        # Update chunk locations
        deleted = defaultdict(list)
        for dev, chunks in req.chunks_report.items(): # dev here is actually dev_id
            for chunk in chunks:
                id, version = chunk.rsplit('.', 1)
                version = int(version)
                if id not in self.chunks or version != self.chunks[id]['v']:
                    # Deleted? Stale?
                    deleted[dev].append(chunk)
                else:
                    # Even if version > self.chunks[id]['v'], we honor the latter
                    self.chunks_map[id].add(dev)

        # See if there are chunks deleted by meta node
        for dev in self.nodes[host].devs:
            if dev in self.deleted:
                deleted[dev] += self.deleted[dev]
                del self.deleted[dev]

        return {'deleted_chunks': dict(deleted)}


    def stat(self, req):
        return {'disks': self.cache, 'chunks': len(self.chunks) }
    
