"""FS client interface

refs:
http://hadoop.apache.org/core/docs/current/api/org/apache/hadoop/fs/FileSystem.html
http://hadoop.apache.org/core/docs/current/api/org/apache/hadoop/dfs/DistributedFileSystem.html
http://hadoop.apache.org/core/docs/current/api/org/apache/hadoop/fs/kfs/KosmosFileSystem.html
"""

from oodict import *
from obj import *
from nio import *
from util import *
import config

class FileSystem:
    """Filesystem interface at client node
    
    We could use a easier way to declare these functions:
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
        return self._nio_meta.call('meta.stat', file = file)
    
    def setattr(self, fid, attrs):
        return self._nio_meta.call('meta.set', fid = fid, attrs = attrs)

    def get_chunks(self, fid, offset, length):
        """Query meta server whether a chunk exists in file """
        return self._nio_meta.call('meta.get_chunks', fid = fid, offset = offset, length = length)

    def get_chunk_locations(self, chunks):
        """Get the localtions of chunks"""
        return self._nio_storage.call('storage.search', chunks = chunks)

    def alloc_chunk(self, size, n):
        """Alloc n new chunks"""
        return self._nio_storage.call('storage.alloc', size = size, n = n)
        
    def publish_chunk(self, chunk, dids):
        return self._nio_storage.call('storage.publish', chunk = chunk, dids = dids)
      

class File:
    def __init__(self, name, fs):
        self.name = name
        self._fs = fs
        self._read_buffer = '' # Client buffer
        self._write_buffer = ''
        
        # Current position
        # self._pos = 0

        # If we get file meta here, what shall we do if meta changes in other threads?
        #self.meta = self.fs.get_file_meta(self.name)
                
    def close(self):
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
         
    def _read_chunk(self, chunk, loca, offset, len):
        """Read chunk from replicas"""
        for did, addr in loca:
            try:
                nio_chunk = NetWorkIO(addr)
                status, payload = nio_chunk.call('chunk.read', did = did, chunk = chunk, offset = offset, len = len)
                nio_chunk.close()
                return payload
            except IOError, err:
                debug('read failed', loca,  chunk, err)
        # Fatal error
        raise IOError('no replica available', loca, chunk)
     
    def _write_chunk(self, chunk, loca, offset, data, new):
        # Write chunk to data nodes
        dids = []
        for did, addr in loca:
            try:
                nio_chunk = NetWorkIO(addr)
                nio_chunk.call('chunk.write', did = did, chunk = chunk, offset = offset, payload = data, new = new)
                nio_chunk.close()
                dids.append(did)
            except IOError, err:
                debug('write failed', loca,  chunk, err)
        
        if not dids:
            raise IOError('data not written')
        return dids
    
 
    def read(self, offset, length):
        """Read len bytes from offset.

        @offset
        @length

        May return less than length if EOF is encountered or errors happened
        """        
        meta = self._fs.stat(self.name)
        if offset >= meta.size:
            return None # offset outof range
        if length == 0:
            return None
        if offset + length > meta.size:
            length = meta.size - offset # read outof range
        
        chunks = self._fs.get_chunks(meta.id, offset, length)
        locations = self._fs.get_chunk_locations(chunks.exist_chunks)

        # Window algo
        data = []
        # Init, special case for beginning
        w_start = offset % meta.chunk_size
        w_len = meta.chunk_size - w_start
        cid = chunks.first
        while cid <= chunks.last:
            # Special case,  for final chunk. Take care when we only have one
            # chunk in the file
            if cid == chunks.last:
                w_len = (offset + length) % meta.chunk_size
 
            # Body
            if cid not in chunks.exist_chunks:
                data.append(zeros(w_len)) # Hole in sparse file
            else:
                chunk = chunks.exist_chunks[cid]
                if cid not in locations:
                    raise IOError('no replica found for chunk', chunk)
                data.append(self._read_chunk(chunk, locations[cid], w_start, w_len))

            # Iterate next
            cid += 1

            # Set normal case value
            w_start = 0
            w_len = meta.chunk_size
           
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

        chunks = self._fs.get_chunks(meta.id, offset, length)
        locations = self._fs.get_chunk_locations(chunks.exist_chunks)
        
        bytes_written = 0
        w_start = offset % meta.chunk_size
        w_len = meta.chunk_size - w_start
        cid = chunks.first
        attrs = OODict()
        while cid <= chunks.last:

            if cid == chunks.last: # Final chunk
                w_len = (offset + length) % meta.chunk_size

            new = False
            if cid not in chunks.exist_chunks:
                # Alloc new chunk
                new = True
                loca = self._fs.alloc_chunk(meta.chunk_size, meta.replica_factor)
                if not loca:
                    raise IOError('storage.alloc no free space')
                chunk = Chunk(meta.id, cid, 1)
                chunk.size = meta.chunk_size
            else:
                # Write old chunk 
                chunk = chunks.exist_chunks[cid]
                if cid not in locations:
                    raise IOError('no replica found for chunk', chunk)
                loca = locations[cid]
            
            # Write chunk. Perhaps we should seperate creating with writing,
            # and let chunk servers publish chunks to storage manager 
            dids = self._write_chunk(chunk, loca,  w_start, data[bytes_written: bytes_written + w_len], new)
    
            # Update size and chunk location
            # See whether we are writing out of file, update file size if yes
            bytes_written += w_len
            new_size = offset + bytes_written
            if new_size > meta.size:
                meta.size = new_size
                attrs.size = new_size
            if not new:
                chunk.version += 1 # Update version
            del chunk['size'] # We don't want chunk.size be saved in chunk, this is ugly. In fact, chunk is more than a object id than a object
            attrs.chunks = {cid: chunk}
            
            # Update with meta server
            self._fs.setattr(meta.id, attrs)
            # Update with storage server
            self._fs.publish_chunk(chunk, dids)
            
            cid += 1
            w_start = 0
            w_len = meta.chunk_size
            
        return bytes_written
