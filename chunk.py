"""
Chunk service
"""

import os
from oodict import OODict
import hashlib
from util import *
from dev import *
from nio import *

CHUNK_HEADER_SIZE = 1024

import time
import thread

class ChunkHeader(OODict):
    """size, algo, checksum"""
    pass

class Chunk:
    def __init__(self):
        self.header = ChunkHeader()
        self.data = ''


    def read(self, file):
        # Read existing chunk
        if not os.path.isfile(file):
            raise IOError('chunk lost or stale')
        
        fp = open(file, 'r')
        try:
            header = eval(fp.read(CHUNK_HEADER_SIZE))
        except:
            raise IOError('chunk header corrupt')
        if not header:
            fp.close()
            raise IOError('chunk header corrupt')
        self.header = ChunkHeader(header)
        
        data = fp.read(self.header.size)
        if len(data) != self.header.size:
            fp.close()
            raise IOError('chunk data lost')
        
        if self.header.algo == 'sha1':
            if hashlib.sha1(data).hexdigest() != self.header.checksum:
                fp.close()
                raise IOError('chunk data corrupt')
            
        self.data = data
        fp.close()


    def touch(self, file, chunk_size):
        """
        Create a new sparse chunk file with no header, if you want to load this
        kind of chunk, error will be detected
        """
        self.header.size = chunk_size
        self.data = '\0' * self.header.size
        
        # Create base dir
        d = os.path.dirname(file)
        if not os.path.exists(d):
            os.makedirs(d)
            
        # Writing, truncate if exists. This is dangerous if you use a wrong meta dev,
        # object with the same id will be overwrited
        f = open(file, 'w')
        f.truncate(CHUNK_HEADER_SIZE + self.header.size)
        f.close()
    
        

    
    def write(self, file, offset, data):
        """Write to existing file"""
        self.old_real_size = get_file_real_size(file)
        
        # May raise IOError
        f = open(file, 'r+') # Update file
        # Write header, fix-length 1024 bytes from file start
        fmt = '%%-%ds' % CHUNK_HEADER_SIZE
        f.write(fmt % pformat(self.header)[:CHUNK_HEADER_SIZE])
        f.seek(offset + CHUNK_HEADER_SIZE)
        f.write(data)
        f.close()
        
        self.real_size = get_file_real_size(file)
        debug('chunk size:', file, self.real_size, self.old_real_size)
        
    def update_checksum(self, offset, data):
        if offset + len(data) > self.header.size:
            raise IOError('chunk write out of range')
        
        new_data = self.data[:offset] + data + self.data[offset + len(data):]
        # We do not write new_data to disk here because in that case chunk file
        # will has no holes on lower layer disk fs
        self.header.algo = 'sha1'
        self.header.checksum = hashlib.sha1(new_data).hexdigest()


class ChunkService(Service):
    """
    Chunk server needs to know all the using devices at startup. One way is through
    configuration file, which contains all the devices exported, but what happens
    when we want to add some devices, do we need to reboot the chunk server? If not,
    there should be some methods to inform the server.
    
    Instead of this, we implement local storage management as part of the server,
    command 'ideer.py storage' communicates with local chunk server through sockets
    interface chunk.admin_dev.
    """
    def __init__(self, addr, storage_addr):
        self._addr = addr
        self._storage_service_addr = storage_addr
        
        self.devices = DeviceManager(storage_addr)

        thread.start_new_thread(self._hb_for_storage_manager, ())
        
        
    def _get_chunk_filename(self, object_id, chunk_id, version):
        # Because different chunks of same object may exists on same device, chunk
        # file name must based on both object_id and chunk_id. If multicopies of a 
        # chunk are allowed to exist on same device too, there shall be something to
        # differentiate them.
        name = self._id2path(object_id)
        return '.'.join([name, str(chunk_id), str(version)])
    
    def _get_chunk_filepath(self, dev_path, object_id, chunk_id, version):
        return os.path.join(dev_path, 'OBJECTS', \
            self._get_chunk_filename(object_id, chunk_id, version))
        
    def write(self, req):        
        # Checksum is based on the whole chunk, that means, every time we have to
        # read or write the whole chunk to verify it, a little bit overkill.
        # hashlist can be used here, split the chunk to 10 small ones
        # read n small chunks contain offset+len
        # verify 
        # modify hashlist
        # write data back, save header
        if req.dev_id not in self.devices.cache:
            self._error('dev not exists')
        
        dev = self.devices.cache[req.dev_id]
        if dev.status != 'online':
            self._error('dev not online')
        if dev.mode == 'frozen':
            self._error('dev frozen')  

        chunk = Chunk()
        file = self._get_chunk_filepath(dev.path, req.object_id, req.chunk_id, req.version)
        old_file = self._get_chunk_filepath(dev.path, req.object_id, req.chunk_id, req.version - 1)
        
        try:
            # There should be a safe limit of free space. Even if it's a old existing
            # chunk, we may write to hole in it, so still need extra space check
            if dev.size - dev.used < len(req.payload) * 3:
                self._error('no enough space')
                
            if not os.path.exists(old_file):
                # Create chunk file on disk, there is possibility that chunk file 
                # is left invalid on disk if it has not been written to disk successfully
                chunk.touch(file, req.chunk_size) 
                chunk.update_checksum(req.offset, req.payload)
                chunk.write(file, req.offset, req.payload)
                # Update dev stat finally
                dev.used += chunk.real_size
            else:
                chunk.read(old_file)
                chunk.update_checksum(req.offset, req.payload)
                chunk.write(old_file, req.offset, req.payload)
                os.rename(old_file, file) # Rename it to new version
                dev.used += chunk.real_size - chunk.old_real_size 
            
            # Save status back to disk
            self.devices._flush(dev.id)
            
        except IOError, err:
            self._error(err.message)
        # pipe write
        return 'ok'


    def read(self, req):
        if req.dev_id not in self.devices.cache:
            self._error('dev not exists')
        
        dev = self.devices.cache[req.dev_id]
        if dev.status != 'online':
            self._error('dev not online')
            
        file = self._get_chunk_filepath(dev.path, req.object_id, req.chunk_id, req.version)
        chunk = Chunk()
        try:
            chunk.read(file)
        except IOError, err:
            self._error(err.message)

        if req.offset + req.len > chunk.header.size:
            self._error('chunk read out of range')
        return 'ok', chunk.data[req.offset: req.offset + req.len] # This is a payload


    def admin_dev(self, req):
        #req.action
        #req.dev
        
        try:
            handler = getattr(self.devices, req.action)
        except AttributeError:
            self._error('unknown action')
                
        if not callable(handler):
            self._error('unkown action')

        try:
            return handler(req)
        except IOError, err:
            self._error(err.message)


    def _delete_chunk(self, id, chunk):
        if id not in self.devices.cache:
            return
        dev = self.devices.cache[id]
        chf = os.path.join(dev.path, 'OBJECTS', self._hash2path(chunk))
        debug('delete chunk %s on %s' % (chunk, id))
        debug(dev.used)
        # Safe delete
        try:
            dev.used -= get_file_real_size(chf)
            self.devices._flush(dev.id)
            
            os.remove(chf)
        except OSError:
            pass
        debug(dev.used)

    def _hb_for_storage_manager(self):
        nio = NetWorkIO(self._storage_service_addr)
        
        while True:
        
            #debug('sending heart beat message to storage manager')
            # We need lock here
            changed = self.devices.changed
            need_send_report = self.devices.need_send_report
            self.devices.need_send_report = set()
            self.devices.changed = set()
            
            changed_devs = {}
            for id in changed:
                if id not in self.devices.cache:
                    changed_devs[id] = {} # Removed
                else:
                    changed_devs[id] = self.devices.cache[id]
            
            chunks_report = {}
            for id in need_send_report:
                chunks_report[id] = self.devices.chunks[id]
                del self.devices.chunks[id]
                
            rc = nio.call('storage.hb', addr = self._addr, changed_devs = changed_devs, \
                chunks_report = chunks_report)
            
            # Delete old chunks
            for dev, chunks in rc.deleted_chunks.items():
                for chunk in chunks:
                    self._delete_chunk(dev, chunk)
            
            time.sleep(5)


class DeviceManager(Service):
    """
    status:
        offline
        online
        replacing
        invalid    
        
    mode
        frozen
        
    how to send chunkreport? push or poll? use a independent thread or piggiback 
    with heart beat message?
    
    
    Maybe ideer.py shall contact with storage manager directly, chunk serivce only
    provides read, write, replicate, delete, report operations etc, it's a clear design.
    
    The diffcult is when allocating a new chunk, how to deal with device used size, 
    we have no idea about the real size of a sparse chunk file
    """
    
    def __init__(self, storage_addr):
        self._storage_service_addr = storage_addr
                
        self.cache_file = 'exported_devices'
        self.cm = ConfigManager(os.path.expanduser('~/.ideerfs/'))
        self.cache = self.cm.load(self.cache_file, OODict())
                
        self.chunks = {}
        self.changed = set()
        self.need_send_report = set()
        
        # If you use a[k] access a dict item, it's converted to OODict automatically
        # For later convenience
        for k, v in self.cache.items():
            v = OODict(v)
            self.cache[k] = v
            if v.status == 'online':
                thread.start_new_thread(self.scan_chunks, (v,))

    def scan_chunks(self, dev):
        debug('scanning chunks', dev.path)
        self.chunks[dev.id] = self.get_chunks_list(dev.path)
        self.changed.add(dev.id)
        self.need_send_report.add(dev.id)

    def get_chunks_list(self, path):
        """Get all the chunks of a device"""
        root = os.path.join(path, 'OBJECTS')
        chunks = []
        for n1 in os.listdir(root):
            l1 = os.path.join(root, n1)
            for n2 in os.listdir(l1):
                l2 = os.path.join(l1, n2)
                chunks.append(map(lambda f: n1 + n2 +f, os.listdir(l2)))
        return reduce(lambda x, y : x +y, chunks, [])                
        
    def _flush(self, id):
        """Flush the changes of dev indicated by id"""
        self.changed.add(id)
        self.cm.save(self.cache, self.cache_file)
        
    def _get_dev(self, path):
        """Return a dev object, but check status first"""
        dev = Dev(path)
        if not dev.config:
            self._error('not formatted')
        
        if dev.config.data_type != 'chunk':
            self._error('wrong data type')
        return dev

    def online(self, req):
        """Update dev status in devices cache to 'online', notice that we do not 
        update the config file on disk"""
        dev = self._get_dev(req.path) # Get dev from path
        id = dev.config.id
        if id not in self.cache:
            self.cache[id] = dev.config # Add entry
        else:
            if self.cache[id].status != 'offline':
                self._error('not offline')
                
        self.cache[id].status = 'online'
        self._flush(id)
        
        # Start another thread to scan all the chunks the new device holds
        thread.start_new_thread(self.scan_chunks, (dev.config,))
        return 'ok'
        
    def offline(self, req):
        """Update dev status in devices cache to 'offline'
        """
        dev = self._get_dev(req.path) # Get dev from path
        id = dev.config.id
        if id not in self.cache:
            self._error('not in cache')
        
        if self.cache[id].status != 'online':
            self._error('not online')
                
        self.cache[id].status = 'offline'
        self._flush(id)
        # Notify stroage manager dev offline
        return 'ok'
        
    def remove(self, req):
        dev = self._get_dev(req.path) # Get dev from path
        id = dev.config.id
        if id not in self.cache:
            self._error('not in cache')
        del self.cache[id]
        self._flush(id)
        return 'ok'
        
    def frozen(self, req):
        dev = self._get_dev(req.path)
        id = dev.config.id
        if id not in self.cache:
            self._error('not in cache')

        self.cache[id].mode = 'frozen'
        self._flush(id)
        return 'ok'
        
    def stat(self, req):
        if req.path == 'all':
            nio = NetWorkIO(self._storage_service_addr)
            return nio.call('storage.stat')
        
        if req.path == 'local':
            return {'disks': self.cache}
        
        for k, v in self.cache.items():
            if v['path'] == req.path:
                return {'disks': {k: v}}
        
        self._error('no such dev')
        
        # Iterate dev cache to get realtime stat here, disk stat is carried with
        # chunk server's heart-beat message
        #return  {'summary': self.statistics, 'disks': self.cache}


if __name__ == '__main__':
    pass
