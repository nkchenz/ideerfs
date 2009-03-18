"""
Storage service: manage and allocate storage
"""

import time
import random
from collections import defaultdict

from util import *
from dev import *
from service import *
from obj import *
from oodict import *
from io import *
import config

class StorageService(Service):
    """Storage management for chunk store
    disks manager:
    online
    offline
    frozen
    status

    storage allocator:
    alloc       add one entry in chunks db, if anything fails, remember to free please
    free        

    chunk locator:
    search

    heartbeat service:
    hb
    
    replicator:
    replicate chunk?

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
        self._addr = config.storage_manager_address
        self._db = FileDB(config.home)
        
        self._deleted_chunks = defaultdict(list) # Deleted chunks
        self._nodes = {} # Nodes Alive

        log('Loading devices')
        self._devices_file = 'all_devices' # Device config and address
        self._devices = self._db.load(self._devices_file, defaultdict(dict))
        
        # Get all chunks in the whole file system
        # all_chunks holds all alive chunks in the entire system, it's critical important
        log('Loading chunks')
        self._chunks_file = 'all_chunks'
        self._chunks = self._db.load(self._chunks_file, [])

        log('Loading chunks location cache')
        self._chunks_map_file = 'chunks_map'
        self._chunks_map = self._db.load(self._chunks_map_file, defaultdict(list))

    def _flush(self):
        # This is quite heavy
        self._db.store(self._chunks, self._chunks_file)
        self._db.store(self._devices, self._devices_file)
        self._db.store(self._chunks_map, self._chunks_map_file)
        
    def _writeable(self, did):
        """Check if a device is writeable"""
        return self._available(id) and self.cache[id].mode != 'frozen' 

    def _available(self, did):
        """Check if a device is available"""
        if did not in self._devices:
            return False
        return self._is_alive(self._devices[did].addr)

    def _is_alive(self, host):
        """Check if a host is alive. If we haven't heard of it more than 120
        seconds, then we consider it as dead.
        
        We may use hostid aka hid here.        
        """
        return host in self.nodes and time.time() - self.nodes[host].update_time < 120 
    
    def _free_enough(self, did, size):
        """Check if a device has larger space than size bytes"""
        dev = self.cache[id]
        return dev.size - dev.used > size
    
    def _get_dev_addr(self, did):
        """Locate device address"""
        dev = self.cache[id]
        return self.nodes[dev.host].addr
    
    def alloc(self, req):
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
        

    def _update_host(self, host, did = None):
        """Update host aliveness, add device if given"""
        if host not in self._nodes:
            self._nodes[host] = OODict({'devs': set()})
        self._nodes[host].update_time = time.time()
        if did:
            self._nodes[host].devs.add(did)

    def heartbeat(self, req):
        """Update nodes healthy
        update dev.used
        send deleted chunks back
        @addr
        @confs      configs of changed devices
        """
        self._update_host(req.addr)
            
        # Update changed devs
        for did, conf in req.confs.items():
            self._devices[did] = conf
           
        # See if there are chunks deleted by meta node
        deleted = defaultdict(list)
        for did in self.nodes[req.addr].devs:
            if did in self._deleted_chunks:
                deleted[did] += self._deleted_chunks[did]
                del self._deleted_chunks[did]

        return {'deleted_chunks': dict(deleted)}

    def online(self, req):
        """Online device

        @conf       device config
        @addr       chunk server address
        @report     chunk report
        """
        did = req.conf.id
        if did in self._devices:
            self._error('already online')
        self._device[did].conf = req.conf
        self._devices[did].addr = req.addr

        for chunk in req.report:
            # Add to chunks db. 
            # If chunk has been deleted,  too old or too new, we mark it
            # as stale and tell the chunk server to delete it later.
            if chunk not in self._chunks:
                self._deleted_chunks[did].append(chunk)
            else:
                # Save location info
                self._chunks_map[chunk].append(did)

        self._update_host(req.addr, did)
        log('Device %s online' % req.did)
        # Flush
        self._flush()
        return 'ok'

    def offline(self, req):
        """Offline device

        @did               device id
        @replicate         bool, whether to replicate
        """
        if req.did not in self._devices:
            self._error('not online')
    
        # Delete device
        addr = self._devices[req.did].addr
        del self._devices[req.did]
        
        # Remove this device from devices list of the host
        self._nodes[addr].devs.remove(req.did)
        
        # Update location infos for chunks on it
        # chunk -> dev ok
        # dev -> chunks how? should we iterate all the chunks to find chunks
        # belonging to one device?
        
        #if replicate

        log('Device %s offline' % req.did)
        self._flush()
        return 'ok'

    def frozen(self, req):
        """Frozen device
        @did
        """
        if req.did not in self._devices:
            self._error('not online')
    
        # Should we wait for writings to be finished?
        self._devices[req.did].conf.mode = 'frozen'

        self._flush()
        log('Device %s frozen' % req.did)
        return 'ok'

    def status(self, req):
        """Get status of the storage system"""
        return {'devices': self._devices, 'chunks': len(self._chunks)}
        # Iterate dev cache to get realtime stat here, disk stat is carried with
        # chunk server's heart-beat message
        #return  {'summary': self.statistics, 'disks': self.cache}
