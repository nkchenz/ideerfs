#!/usr/bin/python
# coding: utf8
import os
from oodict import OODict
import hashlib
from util import *

CHUNK_HEADER_SIZE = 1024

class ChunkHeader(OODict):
    pass

class Chunk:
    def __init__(self):
        self.header = ChunkHeader()
        self.size = 0
        self.data = ''


    def load(self, file):
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

    def write(file, offset, data, mode):
        # May raise IOError
        f = open(file, mode)
        # Write header, fix-length 1024 bytes from file start
        fmt = '%%-%ds' % CHUNK_HEADER_SIZE
        f.write(fmt % pformat(self.header)[:CHUNK_HEADER_SIZE])
        f.seek(offset + CHUNK_HEADER_SIZE)
        f.write(data)
        f.close()
        
    def update_checksum(offset, data):
        new_data = ''
        if chunk.header.size < req.offset: 
            # write range out of data, fill hole with zeros, 
            self.data += zeros(req.offset - chunk.header.size) 
        
        # part in, part out
        new_data = self.data[:req.offset] + data
        
        end = req.offset + len(data)
        if end < chunk.header.size:
            # write range in data
            new_data += self.data[end:]

        # We do not write new_data to disk here because in that case chunk file
        # will has no holes on lower layer disk fs
        self.header.checksum = hashlib.sha1(new_data).hexdigest()
        self.header.size = len(new_data)


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
        
        
    def _mk_path(self, object_id, chunk_id, version):
        # Because different chunks of same object may exists on same device, chunk
        # file name must based on both object_id and chunk_id. If multicopies of a 
        # chunk are allowed to exist on same device too, there shall be something to
        # differentiate them.
        path = self._id2path(object_id)
        return '.'.join([path, str(chunk_id), str(version)])
    
    
    def write(self, req):        
        # Checksum is based on the whole chunk, that means, every time we have to
        # read or write the whole chunk to verify it, a little bit overkill.
        # hashlist can be used here, split the chunk to 10 small ones
        # read n small chunks contain offset+len
        # verify 
        # modify hashlist
        # write data back, save header
        
        # There maybe holes in chunk, so when create new chunks, hashlist should be
        # regenerate
        chunk = Chunk()
        file = os.path.join(req.dev, self._mk_path(req.object_id, req.chunk_id, req.version))
        try:
            if req.overwrite:
                old_file = os.path.join(req.dev, self._mk_path(req.object_id, req.chunk_id, req.version - 1))
                chunk.load(old_file)
                chunk.update_checksum(req.offset, req.payload)
                chunk.write(old_file, req.offset, req.payload, 'w')
                os.rename(oldfile, file) # Rename it to new version
            else:
                chunk.update_checksum(req.offset, req.payload)
                chunk.write(file, req.offset, req.payload, 'w+')
        except IOError, err:
            self._error(err.message + ': ' + file)
        # pipe write


    def read(self, req):
        file = os.path.join(req.dev_path, self._mk_path(req.object_id, req.chunk_id, req.version))
        chunk = Chunk()
        try:
            chunk.load(file)
        except IOError, err:
            self._error(err.message + ': ' + file)
    
        end = req.offset + req.len
        if offset > len(chunk.data) or end > len(chunk.data):
            self._error('not enough data to read')
        return 'ok', chunk.data[req.offset: end] # This is a payload


if __name__ == '__main__':
    pass

