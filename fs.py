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
    """Filesystem interface at client node
    
    We could use a easy way to declare this functions
    def __init__func(self, method):
        proto, func = method.split('.')
        nio = getattr(self, '_nio_' + proto)
        setattr(self, func, lambda **args: nio.call(method, **args))
    """
    
    def __init__(self):
        self._nio_meta = NetWorkIO(config.meta_server_address)
        self._nio_storage = NetWorkIO(config.storage_server_address)

    def create(self, file, **attrs):
        """Create new files with attrs: replication factor, bs, permission
        foo.create('/kernel/sched.c', replica_factor = 3, chunk_size = '64m')
        """
        return self._nio_meta.call('meta.create', file = file, type = 'file', attrs = attrs)

    def delete(self, file, recursive):
        return self._nio_meta.call('meta.delete', file = file, recursive = recursive)
    
    def exists(self, file):
        return self._nio_meta.call('meta.exists', file = file)

    def lsdir(self, dir):
        """list dir, return [] if not exists or not a dir"""
        return self._nio_meta.call('meta.lsdir', dir = dir)

    def mkdir(self, dir):
        return self._nio_meta.call('meta.create', file = dir, type = 'dir')
            
    def mv(self, old_file, new_file):
        return self._nio_meta.call('meta.rename', old_file = old_file, new_file = new_file)
        
    def open(self, file):
        """Return a File object"""
        if not self.exists(file):
            raise IOError('file not exists')
        return File(file, self)

    def close(self):
        # No more fs operations, flush
        pass
    
    def status(self):
        # Default stripe_size, rf, 
        pass

    def copy_from_local_file(self, src, dst, overwrite):
        pass
    
    def copy_to_local_file(self, src, dst):
        pass
        
    def copy(self, src, dst):
        pass

    def append(self, file, data, progress):
        pass

    def stat(self, file):
        """Get attributes of file, stat file"""
        return self._nio_meta.call('meta.get', file = file)
    
    def setattr(self, fid, attrs):
        return self._nio_meta.call('meta.set', fid = fid, attrs = attrs)

    def get_chunks(self, fid, offset, length):
        """Query meta server whether a chunk exists in file
        """
        return self._nio_meta.call('meta.get_chunks', fid = fid, chunks = chunks)

    def get_chunk_locations(self, chunks):
        """Get the localtions of chunks"""
        return self._nio_storage.call('storage.search', chunks = chunks)

    def alloc_chunk(self, size, n):
        """Alloc n new chunks"""
        return self._nio_storage.call('storage.alloc', size = size, n = n)
        
    def free_chunk(self, chunk):
        return self._nio_storage.call('storage.free', chunk = chunk)
      

class File:
    def __init__(self, name, fs):
        self.name = name
        self._fs = fs
        self._read_buffer = '' # Client buffer
        self._write_buffer = ''
        
        # If we get file meta here, what shall we do if meta changes in other threads?
        #self.meta = self.fs.get_file_meta(self.name)
                
    def close():
        """Close file, flush buffers"""
        pass
    
    def lock(self, range):
        # Before read and write, better get a lock 
        pass
    
    def tell():
        pass
        
    def seek():
        pass
        
    def flush():
        pass
         
    def read_chunk(self, chunk, offset, len):
        """
        Read chunk of a object, from offset, length is len, locations and version
        are in info
        """
        info = OODict(info)
        for dev, addr in locations.items():
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
     
    def write_chunk(self, chunk, info, offset, data):
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
    
 
    def read(self, offset, length):
        """Read len bytes from offset.

        @offset
        @len

        May return less than len if EOF is encountered or errors happened
        """        
        meta = self._fs.stat(self.name)
        if offset >= meta.size:
            return None # offset outof range
        if length == 0:
            return None
        if offset + length > meta.size:
            length = meta.size - offset # read outof range
        
        chunks = self._fs.get_chunks(meta.id, offset, length)
        # Window algo
        data = []
        # Init, special case for beginning
        w_start = offset % meta.chunk_size
        w_len = meta.chunk_size - w_start
        cid = chunks.first
        while cid <= chunks.last:
            # Special case for ending, final chunk. We can handle the case
            # that beginning is als ending.
            if cid == chunks.last:
                w_len = (offset + length) % meta.chunk_size
 
            # Body
            if cid not in chunks.exist_chunks:
                data.append(zeros(w_len)) # Hole in sparse file
            else:
                #chunk = (meta.id, cid, version)
                #locations = ()
                data.append(self._fs.read_chunk(meta.id, chunk, locations, w_start, w_len))

            # Iterate next
            cid += 1
            # Set normal case value
            w_start = 0
            w_len = meta.chunk_sze
           
        return ''.join(data)

    def write(self, offset, data):
        """Write data to offset of file
        
        @offset
        @data

        Return bytes written. If there are errors, write as many as possisble.
        """
        if not data:
            return 0 # Zero bytes written
        meta = self._fs.stat(self.name)
        length = len(data)
        offset2 = offset + length # The last byte write

        chunks = self._fs.get_chunks(meta.id, offset, length)
        
        w_start = 0
        offset_in_chunk = offset % meta.chunk_size
        w_len = meta.chunk_size - offset_in_chunk
        cid = chunks.first
        attrs = OODict()
        while cid <= chunks.last:

            new = False
            if cid not in chunks.exist_chunks:
                # Alloc new chunk
                # new = True
                new = True
                #if not info.locations:
                #    raise IOError('storage.alloc no free space')
            else:
            
            # Write chunk
            devs = self._fs.write_chunk(meta.id, chunk, info, offset_in_chunk, data[w_start: w_start + w_len])
            if not devs:
                if new:
                    self._fs.free_chunk(meta.id, chunk) # cleanup
                # Fatal error
                # Fixme, should free the chunk you allocated when something
                # bad happens
                raise IOError('no replications available: object %d chunk %d' % (meta.id, chunk))
    
    
            # See whether write outof file, update file size as needed
            new_size = w_start + w_len
            if new_size > meta.size:
                pass

            # Update chunk info and version with storage node, maybe this should 
            # be done by chunk node
            self._fs.update_chunk_info(meta.id, chunk, info.version, devs, new, meta.replica_factor)
            
            cid += 1

            offset_in_chunk = 0
            w_start += w_len
            if cid == chunks.last: # Final chunk
                w_len = offset2 % meta.chunk_size
            
        return length
