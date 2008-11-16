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
        
        f = open(file, 'w') # Writing, truncate if exists
        f.seek(CHUNK_HEADER_SIZE + self.header.size - 1)
        f.write('\0')
        f.close()
        
    def write(self, file, offset, data):
        """Write to existing file"""
        # May raise IOError
        f = open(file, 'r+') # Update file
        # Write header, fix-length 1024 bytes from file start
        fmt = '%%-%ds' % CHUNK_HEADER_SIZE
        f.write(fmt % pformat(self.header)[:CHUNK_HEADER_SIZE])
        f.seek(offset + CHUNK_HEADER_SIZE)
        f.write(data)
        f.close()
        
    def update_checksum(self, offset, data):
        if offset + len(data) > self.header.size:
            raise IOError('chunk write out of range')
        
        new_data = self.data[:req.offset] + data + self.data[req.offset + len(data):]
        # We do not write new_data to disk here because in that case chunk file
        # will has no holes on lower layer disk fs
        self.header.algo = 'sha1'
        self.header.checksum = hashlib.sha1(new_data).hexdigest()


class ChunkService(Service):
    """
    """
    def __init__(self):
        self.CHUNK_HEADER_SIZE = 1024
        pass
        
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
      
    
    def _check_dev_status(self, path, status):
        dev = Dev(path)
        try:
            dev.check_status(status)
        except IOError, err:
            self._error(err.message)
        return dev
        
    def write(self, req):        
        # Checksum is based on the whole chunk, that means, every time we have to
        # read or write the whole chunk to verify it, a little bit overkill.
        # hashlist can be used here, split the chunk to 10 small ones
        # read n small chunks contain offset+len
        # verify 
        # modify hashlist
        # write data back, save header
        
        dev = self._check_dev_status(req.dev_path, ['online'])
        chunk = Chunk()
        file = self._get_chunk_filepath(req.dev_path, req.object_id, req.chunk_id, req.version)
        try:
            if not req.is_new:
                old_file = self._get_chunk_filepath(req.dev_path, req.object_id, req.chunk_id, req.version - 1)
                chunk.read(old_file)
                chunk.update_checksum(req.offset, req.payload)
                chunk.write(old_file, req.offset, req.payload)
                os.rename(old_file, file) # Rename it to new version
            else:
                # There should be a safe limit of free space
                if dev.config.size - dev.config.used < req.chunk_size * 3:
                    self._error('no space for chunk')
                # Create chunk file on disk, there is possibility that chunk file 
                # is left invalid on disk if it has not been written to disk successfully
                chunk.touch(file, req.chunk_size) 
                chunk.update_checksum(req.offset, req.payload)
                chunk.write(file, req.offset, req.payload)
                # Update dev stat finally
                dev.config.used += req.chunk_size
                dev.config_manager.save(dev.config, dev.config_file)
        except IOError, err:
            self._error(err.message)
        # pipe write
        return 'ok'


    def read(self, req):
        self._check_dev_status(req.dev_path, ['online', 'frozen'])
            
        file = self._get_chunk_filepath(req.dev_path, req.object_id, req.chunk_id, req.version)
        chunk = Chunk()
        try:
            chunk.read(file)
        except IOError, err:
            self._error(err.message)

        if req.offset + req.len > chunk.header.size:
            self._error('chunk read out of range')
        return 'ok', chunk.data[req.offset: req.offset + req.len] # This is a payload


if __name__ == '__main__':
    
    req = OODict()
    req.dev_path = '/data/sdb'
    req.object_id = 2
    req.chunk_id = 4
    req.version = 2
    req.offset = 0
    req.payload = '0123456789'
    req.chunk_size = 1024 * 1024 * 64
    req.is_new = True
    
    cs = ChunkService()
    print cs.write(req)
    
    req.len=10
    print cs.read(req)