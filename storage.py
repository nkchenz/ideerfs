"""
Storage service: manage and allocate storage
"""

import time
import random
from collections import defaultdict

from util import *
from service import *
from oodict import *
from io import *
import config

class StorageService(Service):
    """Storage management for chunk store
    disk manager:
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
        self._db = FileDB(config.home)
        
        # Deleted chunks, indexed by device id, so you can easily get all
        # deleted chunks on one device
        self._deleted_chunks = defaultdict(list)
        self._nodes = {} # Nodes Alive info

        log('Loading devices')
        self._devices_file = 'all_devices' # Device config and address cache
        self._devices = self._db.load(self._devices_file, defaultdict(dict))
        
        """Chunk location DB, dict.  key is (fid, cid), value is a dict of
        version and locations.
                (1, 2): {'v': 2, 'l': set([dev1, dev2])}
        """
        log('Loading chunks location cache')
        self._chunks_map_file = 'chunks_map'
        self._chunks_map = self._db.load(self._chunks_map_file, defaultdict(dict))

    def _flush(self):
        # This is quite heavy
        self._db.store(self._devices, self._devices_file)
        self._db.store(self._chunks_map, self._chunks_map_file)
        
    def _writeable(self, did):
        """Check if a device is writeable"""
        return self._available(id) and self._devices[did].mode != 'frozen' 

    def _available(self, did):
        """Check if a device is available"""
        if did not in self._devices:
            return False
        return self._is_alive(self._devices[did].addr)

    def _is_alive(self, host):
        """Check if a host is alive. If we haven't heard of it more than 120
        seconds, we consider it as dead.
        
        We may use hostid aka hid here.        
        """
        return host in self._nodes and time.time() - self._nodes[host].update_time < 120 
    
    def _free_enough(self, did, size):
        """Check if a device has larger space than size bytes"""
        conf = self._devices[did].conf
        return conf.size - conf.used > size
    
    def alloc(self, req):
        """Allocate spaces

        @size
        @n

        return locations, which is a list of tuple (did, addr)
        """
        debug('malloc %s bytes on %d devices' % (req.size, req.n))
        
        # Alloc algorithm, better have a list sorted by free space
        value = []
        dids = self._devices.keys()
        random.shuffle(dids)
        found = 0
        for did in dids:
            if self._writeable(did) and self._free_enough(did, req.size):
                value.append((did, self._devices[did].addr))
                found += 1
                if found >= req.n:
                    break
        if not found:
            self._error('no dev avaiable') # Find nothing, this should not happen
        debug(value)
        return value

    def _delete_chunks_map_entry(self, chunk):
        key = chunk.fid, chunk.cid
        if key not in self._chunks_map:
            return
        # Haven't check version
        for did in self._chunks_map[key]['l']:
            self._deleted_chunks[did].append(chunk)

        del self._chunks_map[key]

    def _insert_chunks_map_entry(self, chunk, did):
        """Add chunk replica location info to map
        
        return True if inserted, False if stale """
        key = chunk.fid, chunk.cid
        if key not in self._chunks_map:
            self._chunks_map[key] = {'v': chunk.version, 'l': set([did])}
        else:
            old_version = self._chunks_map[key]['v']
            if chunk.version < old_version:
                return False # Stale
            elif chunk.version == old_version:
                self._chunks_map[key]['l'].add(did)
            else:
                # Free old ones
                old_chunk = chunk
                old_chunk.version = old_version
                for did in self._chunks_map[key]['l']:
                    self._deleted_chunks[did].append(old_chunk)
                # Save new
                self._chunks_map[key]['l'] = set([did])

        return True

    def publish(self, req):
        """Publish a chunk replica, this is for single newly created chunk. If
        you want to publish all chunks on a device, use 'online' method please.
        @chunk
        @dids
        """
        stale = []
        for did in req.dids:
            if not self._insert_chunks_map_entry(chunk, did):
                stale.append(did)
        return {'stale': stale}

    def search(self, req):
        """Search chunks locations
        
        @chunks     dict of chunks
        
        return dict of cid: locations. 
        locations is list of tuple (did, addr)
        """
        value = defaultdict(list)
        for cid, chunk in req.chunks.items():
            key = chunk.fid, chunk.cid
            if key not in self._chunks_map:
                continue # No replicas 
            
            version = self._chunks_map[key]['v']
            if chunk.version != version:
                debug('version mismatch', chunk, version)
                continue # Version error
           
            # Found avaiable devices
            for did in self._chunks_map[key]['l']:
                if self._avaiable(did):
                    value[cid].append((did, self._devices[did].addr))

        return value
    
    def free(self, req):
        """Delete chunk entry and all replicas

        @deleted        chunk list 
        """
        for chunk in req.deleted:
            self._delete_chunks_map_entry(chunk)
        self._flush()
        return 'ok'
        

    def _update_host(self, host, did = None):
        """Update host aliveness, add device entry if given"""
        if host not in self._nodes:
            self._nodes[host] = OODict({'devs': set()})
        self._nodes[host].update_time = time.time()
        if did:
            self._nodes[host].devs.add(did)

    def heartbeat(self, req):
        """Update nodes healthy

        @addr
        @confs      configs of changed devices, optional

        return deleted chunks if have
        """
        self._update_host(req.addr)
            
        # Update changed devices
        for did, conf in req.confs.items():
            self._devices[did].conf = conf
           
        # See whether there are chunks deleted by meta node, belonging to this
        # chunk server
        deleted = defaultdict(list)
        for did in self._nodes[req.addr].devs:
            if did in self._deleted_chunks:
                deleted[did] = self._deleted_chunks[did]
                del self._deleted_chunks[did]
        return {'deleted_chunks': dict(deleted)}

    def online(self, req):
        """Online device

        @conf       device config
        @addr       chunk server address
        @report     chunk report dict
        """
        did = req.conf.id
        if did in self._devices:
            self._error('already online')
        self._devices[did].conf = req.conf
        self._devices[did].addr = req.addr

        for key, chunk in req.report.items():
            self._insert_chunks_map_entry(chunk, did)

        self._update_host(req.addr, did)

        # Flush
        self._flush()
        log('Device %s online' % req.did)
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
        
        # Remove entry from devices list of host
        self._nodes[addr].devs.remove(req.did)
        
        # Update location infos for chunks on it
        # chunk -> dev ok
        # dev -> chunks how? should we iterate all the chunks?
        # belonging to one device?
        
        #if replicate

        self._flush()
        log('Device %s offline' % req.did)
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
