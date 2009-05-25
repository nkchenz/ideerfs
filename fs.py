"""FS client interface

refs:
http://hadoop.apache.org/core/docs/current/api/org/apache/hadoop/fs/FileSystem.html
http://hadoop.apache.org/core/docs/current/api/org/apache/hadoop/dfs/DistributedFileSystem.html
http://hadoop.apache.org/core/docs/current/api/org/apache/hadoop/fs/kfs/KosmosFileSystem.html
"""

from oodict import *
from util import *
import config
from logging import info, debug
from msg import messager
import thread
import threading

class FileSystem:
    """Filesystem interface at client node
    
    We could use a easier way to declare these functions:
    def __init__func(self, method):
        proto, func = method.split('.')
        nio = getattr(self, '_nio_' + proto)
        setattr(self, func, lambda **args: nio.call(method, **args))
    """

    def create(self, file, **attrs):
        """Create new files with attrs: replication factor, bs, permission
        foo.create('/kernel/sched.c', replica_factor = 3, chunk_size = '64m')
        """
        return messager.call(config.meta_server_address, 'meta.create', file = file, type = 'file', attrs = attrs)

    def delete(self, file, recursive):
        return messager.call(config.meta_server_address, 'meta.delete', file = file, recursive = recursive)
    
    def exists(self, file):
        return messager.call(config.meta_server_address, 'meta.exists', file = file)

    def lsdir(self, dir):
        """list dir, return [] if not exists or not a dir"""
        return messager.call(config.meta_server_address, 'meta.lsdir', dir = dir)

    def mkdir(self, dir):
        return messager.call(config.meta_server_address, 'meta.create', file = dir, type = 'dir')
            
    def mv(self, old_file, new_file):
        return messager.call(config.meta_server_address, 'meta.rename', old_file = old_file, new_file = new_file)
        
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
        return messager.call(config.meta_server_address, 'meta.stat', file = file)
    
    def setattr(self, fid, attrs):
        return messager.call(config.meta_server_address, 'meta.set', fid = fid, attrs = attrs)

    def get_chunks(self, fid, offset, length):
        """Query meta server whether a chunk exists in file """
        return messager.call(config.meta_server_address, 'meta.get_chunks', fid = fid, offset = offset, length = length)

    def get_chunk_locations(self, chunks):
        """Get the localtions of chunks"""
        return messager.call(config.storage_server_address, 'storage.search', chunks = chunks)

    def alloc_chunk(self, size, n):
        """Alloc n new chunks"""
        return messager.call(config.storage_server_address, 'storage.alloc', size = size, n = n)
        
    def publish_chunk(self, chunk, dids):
        return messager.call(config.storage_server_address, 'storage.publish', chunk = chunk, dids = dids)
      

class File:
    """
    We must provide such an interface that servral GBs of data can be written
    to remote network in seconds.
    """
    def __init__(self, name, fs):
        self.name = name
        self._fs = fs
        self._read_buffer = '' # Client buffer
        self._write_buffer = '' # Sequent write buffer

        self._write_workers = []
        self._idle_queue = []
        self._idle_queue_cv = threading.Condition()

        self._closing = False

        # Next byte to read or write
        self._pos = 0

        # If we get file meta here, what shall we do if meta changes in other threads?
        # Get write lock
        self.meta = self.stat()
                
    def close(self):
        """Close file, flush buffers"""
        self.flush()
        # Tell all workers to exit, some workers maybe sleep right now, so need to
        # feed somthing to wake them up
        self._closing = True
        for i in self._write_workers:
            self._feed_worker(i, 'bye') # This arbitrary string will not be written
    
    def lock(self, range):
        # Before read and write, better get a lock 
        pass
    
    def flush(self):
        self.write_chunk(self._write_buffer)
        self._write_buffer = ''

    def stat(self):
        return self._fs.stat(self.name)

    def _read_chunk(self, chunk, loca, offset, length):
        """Read chunk from replicas"""
        for did, addr in loca:
            try:
                status, payload = messager.call(addr, 'chunk.read', did = did, chunk = chunk, offset = offset, len = length)
                debug('file.read_chunk: cid %d offset %d length %d get %d', chunk.cid, offset, length, len(payload))
                return payload
            except IOError, err:
                debug('Read failed for chunk %s at %s@%s: %s', chunk, did, addr, err)
        # Fatal error
        raise IOError('no replica available', loca, chunk)
     
    def _write_chunk(self, chunk, loca, offset, data, new):
        # Write chunk to data nodes
        dids = []
        for did, addr in loca:
            try:
                messager.call(addr, 'chunk.write', did = did, chunk = chunk, offset = offset, payload = data, new = new)
                dids.append(did)
            except IOError, err:
                debug('Write failed for chunk %s at %s@%s: %s', chunk, did, addr, err)
        
        if not dids:
            raise IOError('data not written')
        return dids
    
    def read(self, length = None):
        """Read len bytes from current pos, maybe multi chunks

        @length

        May return less than length if EOF is encountered or errors happened
        """        
        meta = self.stat()
        if not length:
            length = meta.size
        offset = self._pos
        if offset >= meta.size:
            return None # offset outof range
        if length == 0:
            return None
        if offset + length > meta.size:
            length = meta.size - offset # read outof range
        
        chunks = self._fs.get_chunks(meta.id, offset, length)
        locations = self._fs.get_chunk_locations(chunks.exist_chunks)

        debug('file.read: pos %d length %d', offset, length)

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
                tmp = (offset + length) % meta.chunk_size
                if tmp == 0:
                    w_len = meta.chunk_size
                else:
                    w_len = tmp
 
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
            self._pos += w_len

            # Set normal case value
            w_start = 0
            w_len = meta.chunk_size
           
        return ''.join(data)

    def write(self, data):
        """Buffer small writes, only write full chunks to network"""
        self._write_buffer += data
        dl = len(data)
        # first send out buffer and part of data
        # then send out slices of data 
        # until we get a non-full chunk, and put it to buffer
        csize = self.meta.chunk_size
        while True:
            offset = self._pos % csize # Usually 0, aligned by chunk
            bsize = len(self._write_buffer)
            if offset + bsize < csize:
                if bsize:
                    debug('file.write: buffered offset %d size %d', offset, bsize)
                break
            length = csize - offset
            debug('file.write: chunk full offset %d length %d', offset, length)
            self.write_chunk(self._write_buffer[:length]) # Should contains offset in file
            self._pos += length
            self._write_buffer = self._write_buffer[length:]

    def _sched_write(self, chunk):
        """Sched chunk writing to a worker and return, no wait for IO """
        if len(self._write_workers) < self.meta.pwrites:
            i = OODict()
            i.data = None
            i.data_cv = threading.Condition()
            i.id = len(self._write_workers)
            self._write_workers.append(i)
            #start new worker
            thread.start_new_thread(self._write_worker, (i))
        else:
            # Find a idle worker whose data is None, sleep until found   
            self._idle_queue_cv.acquire()
            while not self._idle_queue:
                debug('no idle worker found, wait')
                self._idle_queue_cv.wait()
            i = self._idle_queue.pop(0)
            debug('idle worker %d found', i.id)
            self._idle_queue_cv.release()

        self._feed_worker(i, chunk)

    def _feed_worker(self, i, data):
        i.data_cv.acquire()
        debug('feed worker %d', i.id)
        i.data = data
        i.data_cv.notify()
        i.data_cv.release()

    def _write_worker(self, i)
        """ # Every worker should has its own queue, better scale
        # If using global queue, thunder-herd problem?
        """
        debug('write worker %d started', i.id)
        while True:
            i.data_cv.acquire()
            while not i.data:
                i.data_cv.wait() # No data available, sleep
            # Even we release the lock, it's impossible for main thread to 
            # feed us again, because we are not on the idle queue
            i.data_cv.release()

            # Check for closing file
            if self._closing:
                break

            debug('worker %d working', i.id)
            self._write_data(i.data)
            i.data = None

            # Notiy we are idle, OK to feed us again
            self._idle_queue_cv.acquire()
            debug('worker %d idle', i.id)
            self._idle_queue.append(i)
            self._idle_queue_cv.notify()
            self._idle_queue_cv.release()

        debug('write worker %d closed', i.id)

    def _write_data(self, data):
        """Write data to chunk, offset in chunk is self._pos % meta.chunk_size"""
        if not data:
            return 0 # Zero bytes written
        meta = self.stat()
        length = len(data)
        chunks = self._fs.get_chunks(meta.id, self._pos, length)
        locations = self._fs.get_chunk_locations(chunks.exist_chunks)
        cid = chunks.first
        attrs = OODict()
        new = False
        if cid not in chunks.exist_chunks:
            # Alloc new chunk
            new = True
            loca = self._fs.alloc_chunk(meta.chunk_size, meta.replica_factor)
            if not loca:
                raise IOError('storage.alloc no free space')
            chunk = OODict() # No need be a chunk instance, just a dict will be ok
            chunk.fid, chunk.cid, chunk.version = meta.id, cid, 1
            chunk.size = meta.chunk_size
        else:
            # Write old chunk 
            chunk = chunks.exist_chunks[cid]
            if cid not in locations:
                raise IOError('no replica found for chunk', chunk)
            loca = locations[cid]
        
        # Write chunk. 
        # TODO: Seperate creating with writing, and let chunk servers 
        # publish chunks to storage manager 
        #       Check if read, write succeed 
        dids = self._write_chunk(chunk, loca, self._pos % meta.chunk_size, data, new)

        # Update size and chunk location
        # See whether we are writing out of file, update file size if yes
        # Fixme! We may have problem here if not all w_len bytes are written
        #self._pos += length
        if self._pos > meta.size:
            attrs.size = self._pos # Update file size
        if not new:
            chunk.version += 1 # Update version
        # We don't want chunk.size be saved in chunk, so ugly. 
        # In fact, chunk is just like object id 
        del chunk['size']
        attrs.chunks = {cid: chunk}
        
        # Update with meta server
        self._fs.setattr(meta.id, attrs)
        # Update with storage server
        self._fs.publish_chunk(chunk, dids)
        
        return length
