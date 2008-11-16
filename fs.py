#!/usr/bin/python
# coding: utf8

"""
FS interface

refs:
http://hadoop.apache.org/core/docs/current/api/org/apache/hadoop/fs/FileSystem.html
http://hadoop.apache.org/core/docs/current/api/org/apache/hadoop/dfs/DistributedFileSystem.html
http://hadoop.apache.org/core/docs/current/api/org/apache/hadoop/fs/kfs/KosmosFileSystem.html
"""
from nio import *
from util import *


class FileSystem:
    
    def __init__(self):
        self.nio_meta = NetWorkIO('localhost', 1984)
        #self.nio_storage = NetWorkIO('localhost', 1984)
        self.nio_storage = self.nio_meta

    '''
    def _create(self, file, type, attr):
        req = OODict()
        req.method = 'meta.create'
        req.file =  file
        req.type = type
        req.attr = attr
        result = self.nio_meta.request(req)
        if 'error' in result:
            print result.error
            return False
        else:
            return True
    '''

    def create(self, file, **attr):
        """Create new files with attrs: replication factor, bs, permission
        foo.create('/kernel/sched.c', replication_factor = 3, chunk_size = '64m')
        """
        return self.nio_meta.call('meta.create', file = file, type = 'file', attr = attr)

    def delete(self, file, recursive = False):
        # mv to /trash
        pass
    
    def exists(self, file):
        return self.nio_meta.call('meta.exists', file = file)

    def get_chunk_info(self, file, chunk_id):
        # Get version and dev_ids
        info = self.nio_meta.call('meta.get_chunk_info', file = file, chunk_id = chunk_id)
        if info is None:
            return None
        # Translate dev_ids to locations
        info.locations = self.nio_storage.call('storage.locate', dev_ids = info.dev_ids)
        return info
        
    def alloc_chunk(self, chunk_size, n):
        """Alloc n new chunks"""
        return self.nio_storage.call('storage.alloc_chunk', chunk_size = chunk_size, \
            n = n)
            
    def free_chunk(self, chunk):
        # Free the chunk, called by client
        return self.nio_storage.call('storage.free_chunk', chunk_id = chunk_id)

        
    def get_file_meta(self, file):
        return self.nio_meta.call('meta.get', file = file)
    
    def set(self, file, attrs):
        return self.nio_meta.call('meta.set', file = file, attrs = attrs)
    
    def set_file_meta(self, file, attr, value):
        pass
    
    def lsdir(self, dir):
        """list dir, return [] if not exists or not a dir"""
        if not dir:
            return []
        #print self.nio_meta.call('meta.test_payload', dir = dir)
        return self.nio_meta.call('meta.lsdir', dir = dir)

    def mkdir(self, dir):
        return self.nio_meta.call('meta.create', file = dir, type = 'dir')
            
    def mv():
        # Rename
        pass
        
    def open(self, file):
        """"
        Return a File object
        
        We do not use the old way of open mode:
        'w+' write, truncate first
        'rw' read write
        'r' readonly mode
        'a' append write mode
        """
        if not self.exists(file):
            raise IOError('file not exists')
        return File(file, self)

    def close():
        # No more fs operations, flush fs cache
        pass
    
    def stat():
        # Default stripe_size, rf, 
        pass

    '''
    def copy_from_local_file(src, dst, overwrite?):
        pass
    
    def copy_to_local_file(src, dst):
        pass
        
    def copy(src, dst):
        # Need to check whether they are in fs or local
        pass

    def append(self, file, data, progress):
        # It's better if there is a way to get 
        pass
    '''



class File:
    def __init__(self, name, fs):
        self.name = name
        self.fs = fs
        self.client_read_buffer = '' # Client buffer
        # This buffer better be localfile, if only in mem we'll lost it when crashing
        # No need, if client crashs, we'd better let user know
        self.client_write_buffer = ''
        
        # If we get file meta here, what shall we do if meta changes in other threads?
        #self.meta = self.fs.get_file_meta(self.name)
                
    def close():
        """Close file, flush buffers"""
        pass
    
    
    def flock(self, range):
        # Before read and write, better get a lock 
        pass
    
    
    def tell():
        pass
        
    def seek():
        pass
        

    def _read_chunk(self, meta, chunk_id, offset, len):
        info = self.fs.get_chunk_info(self.name, chunk_id)
        if not info: # This chunk is a hole 
            return zeros(len)
        
        data = ''
        for id, dev in info.locations.items():
            try:
                dev = OODict(dev)
                nio_chunk = NetWorkIO(dev.host, 1984)
                status, payload = nio_chunk.call('chunk.read', dev_path = dev.path, \
                    object_id = meta.id, chunk_id = chunk_id, version = info.version, \
                    offset = offset, len = len)
                nio_chunk.close()
                return payload
            except IOError, err:
                print '%s:%s read error: %s' % (dev.host, dev.path, err.message)
        
        # Fatal error
        raise IOError('no replications available: file %s chunk %d' % (self.name, chunk_id))
    

    def read(self, offset, bytes):
        """
        Read len from offset, this may return less than bytes data if eof 
        encountered or error happens
        """        
        meta = self.fs.get_file_meta(self.name)
        if offset >= meta.size:
            return None
        if offset + bytes > meta.size:
            bytes = meta.size - offset
            
        chunk_id = offset / meta.chunk_size
        offset_in_chunk = offset % meta.chunk_size
        window_len = meta.chunk_size - offset_in_chunk
        done_len = 0
        data = ''
        while True:
            if done_len + window_len >= bytes:
                # If reading window is larger than needed, decrease it
                window_len = bytes - done_len
            data += self._read_chunk(meta, chunk_id, offset_in_chunk, window_len)
            done_len += window_len
            if done_len >= bytes:
                break
            window_len = meta.chunk_size
            chunk_id += 1
            offset_in_chunk = 0
            
        return data
        
    
    def flush():
        pass
    
    
    def _write_chunk(self, meta, chunk_id, offset, payload):
        is_new = True    
        info = self.fs.get_chunk_info(self.name, chunk_id)
        if info: # Chunks already exist
            info.version += 1
            is_new = False
        else:# Alloc new chunk
            info = OODict()
            info.version = 1
            info.locations = self.fs.alloc_chunk(meta.chunk_size, meta.replication_factor)
        
        # Save chunk to data node
        dev_ids = []
        for id, dev in info.locations.items():
            try:
                dev = OODict(dev)
                nio_chunk = NetWorkIO(dev.host, 1984)
                nio_chunk.call('chunk.write', dev_path = dev.path, object_id = meta.id, \
                    chunk_id = chunk_id, version = info.version, offset = offset, \
                    payload = payload, is_new = is_new, chunk_size = meta.chunk_size)
                nio_chunk.close()
                dev_ids.append(id)
            except IOError, err:
                print '%s:%s write error: %s' % (dev.host, dev.path, err.message)
        if not dev_ids:
            raise IOError('all replications failed') # Fatal error
    
        # Tell meta node we're done
        attrs = OODict()    
        
        # Calculate new file size
        if chunk_id == meta.size / meta.chunk_size:
            # we are the final chunk
            offset2 = offset + len(payload)
            if offset2 > meta.size % meta.chunk_size:
                attrs.size = chunk_id * meta.chunk_size + offset2
                meta.size = attrs.size # Save the new size for next chunk
    
        attrs.chunks = {chunk_id: {'version': info.version, 'dev_ids': dev_ids}}
        self.fs.set(self.name, attrs)
    
    def write(self, offset, data):
        """
        Return bytes written, if there are errors, part of data may not get written
        how to deal with exceptions?
        """
        if not data:
            return 0 # Zero bytes written
        meta = self.fs.get_file_meta(self.name)
        total = len(data)    
        # Offset may in the middle of chunk, data may accross servral chunks
        chunk_id = offset / meta.chunk_size
        offset_in_chunk = offset % meta.chunk_size
        # Free space left in first chunk
        window_len = meta.chunk_size - offset_in_chunk
        done_len = 0
        while True:
            if done_len + window_len >= total:
                window_len = total - done_len
            chunk_data = data[done_len: done_len + window_len]
            # Becareful, meta.size is changed is this function
            self._write_chunk(meta, chunk_id, offset_in_chunk, chunk_data)
            done_len += window_len
            if done_len >= total:
                break
            window_len = meta.chunk_size
            chunk_id += 1
            offset_in_chunk = 0
            
        return done_len

        """
        #self.client_cache
        # only write when a chunk is full, first write to a local file
        
        meta.create(file)
        meta.get(file)
        
        locations = storage.alloc(1, rf=file.rf)
        chunk.write(locations)
        meta.set(chunk: locations)
        
        storage.free
        
        storage.alloc_chunk
        storage.free_chunk
        
        chunk.free?
        
        #storage is like the locations databases
        
        #alloc block from meta or storage?
        #-free_chunk, storagemanager use the heartbeat message of chunknode to tell
        # it to delete a chunk
        #  chunk.free?
        #  chunk.alloc?
        """
        pass
    
    

if __name__ == '__main__':

    foo = FileSystem()
    foo.exists('/')
    foo.exists('/c')
    foo.mkdir('/')
    foo.exists('/')

    foo.mkdir('/a')
    foo.mkdir('/b')
    foo.lsdir('/')