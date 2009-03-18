"""
FS client interface

refs:
http://hadoop.apache.org/core/docs/current/api/org/apache/hadoop/fs/FileSystem.html
http://hadoop.apache.org/core/docs/current/api/org/apache/hadoop/dfs/DistributedFileSystem.html
http://hadoop.apache.org/core/docs/current/api/org/apache/hadoop/fs/kfs/KosmosFileSystem.html
"""

from nio import *
from util import *
import config

class FileSystem:
    
    def __init__(self):
        self.nio_meta = NetWorkIO(config.meta_server_address)
        self.nio_storage = NetWorkIO(config.storage_server_address)

    def create(self, file, **attr):
        """Create new files with attrs: replication factor, bs, permission
        foo.create('/kernel/sched.c', replica_factor = 3, chunk_size = '64m')
        """
        return self.nio_meta.call('meta.create', file = file, type = 'file', attr = attr)

    def delete(self, file, recursive):
        return self.nio_meta.call('meta.delete', file = file, recursive = recursive)
    
    def exists(self, file):
        return self.nio_meta.call('meta.exists', file = file)

    def get_chunk_info(self, file, chunks):
        """Get the localtions of chunks"""
        rv = self.nio_meta.call('meta.get_chunks', file = file, chunks = chunks)
        if not rv.chunks:
            return None
        return self.nio_storage.call('storage.search', file = rv.id, chunks = rv.chunks)
        
    def read_chunk(self, object_id, chunk_id, info, offset, len):
        """
        Read chunk of a object, from offset, length is len, locations and version
        are in info
        """
        info = OODict(info)
        for dev, addr in info.locations.items():
            try:
                nio_chunk = NetWorkIO(addr)
                status, payload = nio_chunk.call('chunk.read', dev_id = dev, \
                    object_id = object_id, chunk_id = chunk_id, version = info.version, \
                    offset = offset, len = len)
                nio_chunk.close()
                return payload
            except IOError, err:
                print 'dev %s at %s for %d:%d read error: %s' % (dev, str(addr), object_id, chunk_id, err.message)
        # Fatal error
        raise IOError('no replications available: object %d chunk %d' % (object_id, chunk_id))
    
     
    def write_chunk(self, object_id, chunk_id, info, offset, payload):
        # Write chunk to data node
        info = OODict(info)
        devs = []
        for dev, addr in info.locations.items():
            try:
                nio_chunk = NetWorkIO(addr)
                nio_chunk.call('chunk.write', dev_id = dev, object_id = object_id, \
                    chunk_id = chunk_id, version = info.version, offset = offset, \
                    payload = payload, chunk_size = info.size)
                nio_chunk.close()
                devs.append(dev)
            except IOError, err:
                print 'dev %s at %s for %d:%d write error: %s' % (dev, str(addr), object_id, chunk_id, err.message)
        
        return devs
    
    
    def alloc_chunk(self, chunk_size, n):
        """Alloc n new chunks"""
        return self.nio_storage.call('storage.malloc', size = chunk_size, \
            n = n)
    
    def update_chunk_info(self, object_id, chunk_id, version, devs, new, rf):
        return self.nio_storage.call('storage.update', object_id = object_id, chunk_id = chunk_id, \
            version = version, devs = devs, new = new, rf = rf)    
        
    def free_chunk(self, object_id, chunk):
        return self.nio_storage.call('storage.free', object_id = object_id, chunk_id = chunk)

        
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
            
    def mv(self, old_file, new_file):
        return self.nio_meta.call('meta.rename', old_file = old_file, new_file = new_file)
        
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
        # No need, if client crashes, we'd better let user know
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
        
    def read(self, offset, bytes):
        """
        Read len from offset, this may return less than bytes data if eof 
        encountered or error happens
        """        
        meta = self.fs.get_file_meta(self.name)
        if offset >= meta.size: # The final byte's offset is meta.size-1
            return None
        if offset + bytes > meta.size:
            bytes = meta.size - offset
        if bytes == 0:
            return None
        
        start = offset / meta.chunk_size
        end = (offset + bytes) / meta.chunk_size
        chunks = range(start, end + 1) # Get the chunk numbers we read, consequent
        cl = self.fs.get_chunk_info(self.name, chunks) # And their locations, may discrete when reading sparse file
        if not cl:
            return zero(bytes) # Not found 
        
        data = []
        for chunk in chunks:
            w_start = 0
            w_end = meta.chunk_size
            if chunk == start: # First chunk
                w_start = offset % meta.chunk_size
            if chunk == end: # Final chunk
                w_end = (offset + bytes) % meta.chunk_size

            if chunk not in cl:
                data.append(zeros(w_end - w_start))
            else:
                data.append(self.fs.read_chunk(meta.id, chunk, cl[chunk], w_start, w_end - w_start))
            
        return ''.join(data)
    
    
    def flush():
        pass

    def write(self, offset, data):
        """
        Return bytes written, if there are errors, part of data may not get written
        how to deal with exceptions?
        """
        if not data:
            return 0 # Zero bytes written
        meta = self.fs.get_file_meta(self.name)
        bytes = len(data)    
        
        start = offset / meta.chunk_size
        offset2 = offset + bytes
        end = offset2 / meta.chunk_size
        chunks = range(start, end + 1)
        cl = self.fs.get_chunk_info(self.name, chunks)
        if not cl:
            cl = {}
            
        for chunk in chunks:
            
            attrs = OODict()
            
            i = chunk - start # ith chunk
        
            # Two offsets for data
            w_start = i * meta.chunk_size
            w_end = (i + 1) * meta.chunk_size
            if chunk == start: # First chunk
                w_start = 0
            if chunk == end: # Final chunk
                w_end = bytes
                if offset2 > meta.size:
                    attrs.size = offset2
                    
            if chunk not in cl:
                info = OODict()
                info.version = 0
                info.locations = self.fs.alloc_chunk(meta.chunk_size, meta.replica_factor)
                # Add this new chunk to meta node
                attrs.chunks = {chunk: {}}
                new = True
                #if not info.locations:
                #    raise IOError('storage.alloc no free space')
            else:
                new = False
                info = self.cl[chunk]
            
            info.version += 1
            info.size = meta.chunk_size
            offset_in_chunk = (offset + w_start) % meta.chunk_size # Real offset in chunk where to start writing
            devs = self.fs.write_chunk(meta.id, chunk, info, offset_in_chunk, data[w_start: w_end])
            
            if not devs:
                if new:
                    self.fs.free_chunk(meta.id, chunk) # cleanup
                # Fatal error
                # Fixme, should free the chunk you allocated when something
                # bad happens
                raise IOError('no replications available: object %d chunk %d' % (meta.id, chunk))
    
            # Update and file size to meta node
            if attrs:
                self.fs.set(self.name, attrs)
            
            # Update chunk info and version with storage node, maybe this should 
            # be done by chunk node
            self.fs.update_chunk_info(meta.id, chunk, info.version, devs, new, meta.replica_factor)
            
        return bytes
