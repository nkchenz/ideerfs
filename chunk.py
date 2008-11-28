#!/usr/bin/python
# coding: utf8
import os
from oodict import OODict
import hashlib
from util import *
from dev import *

CHUNK_HEADER_SIZE = 1024

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
    
        
    def _get_file_real_size(self, file):
        s = os.stat(file)
        if hasattr(s, 'st_blocks'):
            # st_blksize is wrong on my linux box, should be 512. Some smaller file
            # about 10k use 256 blksize, I don't know why.
            #Linux ideer 2.6.24-21-generic #1 SMP Mon Aug 25 17:32:09 UTC 2008 i686 GNU/Linux
            #Python 2.5.2 (r252:60911, Jul 31 2008, 17:28:52) 
            return s.st_blocks * 512 #s.st_blksize
        else:
            # If st_blocks is not supported
            return s.st_size
    
    def write(self, file, offset, data):
        """Write to existing file"""
        self.old_real_size = self._get_file_real_size(file)
        
        # May raise IOError
        f = open(file, 'r+') # Update file
        # Write header, fix-length 1024 bytes from file start
        fmt = '%%-%ds' % CHUNK_HEADER_SIZE
        f.write(fmt % pformat(self.header)[:CHUNK_HEADER_SIZE])
        f.seek(offset + CHUNK_HEADER_SIZE)
        f.write(data)
        f.close()
        
        self.real_size = self._get_file_real_size(file)
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
    def __init__(self, addr):
        self.CHUNK_HEADER_SIZE = 1024
        self.storage_service_addr = addr
        
        self.devices = DeviceManager()

        
    def _send_chunk_report(self):
        # Where chunk server starts up, it should send informations about the chunks
        # it has to storage server
        
        # To make it simple, now we use the 'ideer.py storage online' command to 
        # send this infos
        pass
        
        
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
        try:
            # There should be a safe limit of free space. Even if it's a old existing
            # chunk, we may write to hole in it, so still need extra space check
            if dev.size - dev.used < len(req.payload) * 3:
                self._error('no enough space')
            
            if req.is_new_chunk:
                # Create chunk file on disk, there is possibility that chunk file 
                # is left invalid on disk if it has not been written to disk successfully
                chunk.touch(file, req.chunk_size) 
                chunk.update_checksum(req.offset, req.payload)
                chunk.write(file, req.offset, req.payload)
                # Update dev stat finally
                dev.used += chunk.real_size
            else:
                old_file = self._get_chunk_filepath(dev.path, req.object_id, req.chunk_id, req.version - 1)
                chunk.read(old_file)
                chunk.update_checksum(req.offset, req.payload)
                chunk.write(old_file, req.offset, req.payload)
                os.rename(old_file, file) # Rename it to new version
                dev.used += chunk.real_size - chunk.old_real_size 
            
            # Save status back to disk
            self.devices._flush()
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



class DeviceManager(Service):
    """
    status:
        offline
        online
        replacing
        invalid    
        
    mode
        frozen
    """
    
    def __init__(self):
        self.cache_file = 'exported_devices'
        self.cm = ConfigManager(os.path.expanduser('~/.ideerfs/'))
        self.cache = self.cm.load(self.cache_file, OODict())
        
        # If you use a[k] access a dict item, it's converted to OODict automatically
        # For later convenience
        for k, v in self.cache.items():
            self.cache[k] = OODict(v)
    
    def _flush(self):
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
        self._flush()
        # Notify stroage manager dev online
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
        self._flush()
        # Notify stroage manager dev offline
        return 'ok'
        
    def remove(self, req):
        dev = self._get_dev(req.path) # Get dev from path
        id = dev.config.id
        if id not in self.cache:
            self._error('not in cache')
        del self.cache[id]
        self._flush()
        return 'ok'
        
    def frozen(self, req):
        dev = self._get_dev(req.path)
        id = dev.config.id
        if id not in self.cache:
            self._error('not in cache')

        self.cache[id].mode = 'frozen'
        self._flush()
        return 'ok'
        
    def stat(self, req):
        return self.cache
        # Iterate dev cache to get realtime stat here, disk stat is carried with
        # chunk server's heart-beat message
        #return  {'summary': self.statistics, 'disks': self.cache}
        


if __name__ == '__main__':
    pass
