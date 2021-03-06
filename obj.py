"""Object interface """

import time
import os
from logging import info, debug
import thread
import zlib

from dev import *
from oodict import OODict
import config

class Object(OODict):
    """Object model
    
    flat mode, all attributes are in self directly"""

    def __init__(self, name, id, parent_id, type, attrs = {}):
        self.id = id
        self.type = type
        self.ctime = time.time()
        self.name = name

        for k,v in attrs.items():
            self[k] = v
        
        if type == 'dir':
            self.children = {'.': id, '..': parent_id} # Dir
        else:
            self.size = 0 # File
            self.chunks = {}

class ObjectShard():
    """Read write objects from meta_dev, middle layer between meta service and
    meta device
    
    Files:
    seq        current object id
    objects    all the objects
    root       root object, every object set should have a root
    """
    def __init__(self):
        self._seq_file = 'seq'
        self._root_file = 'root'
        self._objects_file = 'objects'
        self.lock_seq = thread.allocate_lock()

    def load(self, path):
        """Load object shard from device"""
        self._shard = Dev(path)
        if not self._shard.config:
            raise IOError('%s not formatted' % path)

        self._root = self._shard.load(self._root_file)
        self._seq = self._shard.load(self._seq_file)
        if not self._root:
            raise IOError('root file corrupted') # Fatal error
        if not self._seq:
            raise IOError('seq file corrupted') # Fatal error
        #self.store_object(Object('/', self._root, self._root, 'dir'))
            
        self._objects = self._shard.load(self._objects_file, {})

    def format(self, dev):
        pass

    def create_tree(self):
        """Create a empty new meta tree"""
        self._seq = 0
        self._objects = {}
        self._root = self.create_object()
        self.store_object(Object('/', self._root, self._root, 'dir'))

    def get_object_path(self, id):
        """Method to transfer object id to path on disk"""
        #return os.path.join(self._shard.meta_dir, id2path(id))
        pass

    def get_root_object(self):
        return self._root

    def flush(self):
        # Check point here
        #self._shard.store(self._seq, self._seq_file)
        #self._shard.store(self._objects, 'objects')
        pass

    def create_object(self):
        """In face it only returns the next object id"""
        if self._seq is None:
            raise IOError('seq file corrupted') # Fatal error
        # Lock
        self.lock_seq.acquire()
        self._seq += 1
        self.flush()
        self.lock_seq.release()
        # Release
        return self._seq
    
    def delete_object(self, id):
        del self._objects[id]
        self.flush()
        return True

    def load_object(self, id):
        if id in self._objects:
            return self._objects[id]
        return None

    def store_object(self, obj):
        self._objects[obj.id] = obj
        self.flush()
        return True

class Chunk(OODict):
    pass

CHUNK_HEADER_SIZE = 1024

class ChunkShard():
    """Read write chunks from a disk, middle layer between chunk service and
    real disk
    
    Every disk should has a db which holds infos of all the chunks it has.
    So it may be faster when sending chunk report
    
    Files:
    chunks          chunks db
    CHUNK/fid.id.version        a data chunk 
    
    But you can't use self.dev.load('chunks'), self.dev.store(self.chunks,
    'chunks'), that's too heavy for a db operation like insert or delete.

    Fixme:
        create_chunk
        overwrite_chunk
    """

    def __init__(self):
        pass

    def format(self, dev):
        os.mkdir(os.path.join(dev.config.path, 'CHUNKS'))

    def load_chunk(self, chunk, dev):
        """Read chunk from device, return a chunk object
        @fid
        @cid
        @version
        @dev
        """
        file = self._get_chunk_path(chunk, dev)
        if not os.path.isfile(file):
            raise IOError('chunk lost or stale')

        fp = open(file, 'r')
        try:
            header = OODict(eval(fp.read(CHUNK_HEADER_SIZE).strip('\x00')))
        except:
            raise IOError('chunk header corrupt')

        if not header:
            raise IOError('chunk header corrupt')

        data = fp.read(header.size)
        if len(data) != header.size:
            raise IOError('chunk data lost')

        if header.algo == 'adler32' and zlib.adler32(data) != header.checksum:
            raise IOError('chunk data corrupt')

        if header.algo == 'sha1' and hashlib.sha1(data).hexdigest() != header.checksum:
            raise IOError('chunk data corrupt')

        fp.close()
        header.data = data
        return header

    def _update_checksum(self, chunk, offset, data):
        """Update checksum of a chunk"""
        if offset + len(data) > chunk.size:
            raise IOError('chunk write out of range')
        tmp = chunk.data[:offset] + data + chunk.data[offset + len(data):]
        # We do not write tmp back to disk here because in that case chunk
        # file will not be sparse
        if chunk.algo == 'sha1':
            chunk.checksum = hashlib.sha1(tmp).hexdigest()

        if chunk.algo == 'adler32':
            chunk.checksum = zlib.adler32(tmp)

    def store_chunk(self, chunk, offset, data, dev, new = False):
        """Store data to offset of chunk

        @chunk              chunk info: fid, cid, version, size
        @offset
        @data
        @dev
        @new                Is this a new chunk?
        
        It's dangerous to write on the old file. The file will be corrupted if
        something bad happens in the middle. It's safer to write on a new
        file, then rename as needed, but need extra io.
        """
        file = self._get_chunk_path(chunk, dev)
        # Get chunk data
        if new:
            chunk.psize = 0 # Original physical size
            chunk.data = zeros(chunk.size)
            f = open(file, 'w') # Create new empty file 
            f.truncate(CHUNK_HEADER_SIZE + chunk.size)
            f.close()
        else:
            # New chunk filled with other infos
            chunk = self.load_chunk(chunk, dev)

        # Update check sum
        chunk.algo = config.checksum_algo
        if chunk.algo:
            self._update_checksum(chunk, offset, data)
        # Write data
        self._write_chunk_data(file, offset, data)

        # Get new physical size
        old_psize = chunk.psize
        chunk.psize = get_psize(file)

        if not new: # If new is True, means that this is just a replica
            chunk.version += 1

        # Write header back
        self._write_chunk_header(file, chunk)

        # Rename if needed
        if not new:
            new_file = self._get_chunk_path(chunk, dev)
            os.rename(file, new_file)

        # Update dev.used
        dev.config.used += chunk.psize - old_psize
        dev.flush()
        
    def _write_chunk_data(self, file, offset, data):
        f = open(file, 'rb+') # Update file
        f.seek(offset + CHUNK_HEADER_SIZE)
        f.write(data)
        f.close()

    def _write_chunk_header(self, file, chunk):
        f = open(file, 'rb+') # Update file
        del chunk['data'] # Please use key to delete item in dict, not attribute 
        f.write(pformat(chunk)[:CHUNK_HEADER_SIZE]) # Only 1024 for header
        f.close()

    def _get_chunk_path(self, chunk, dev):
        return '%s/CHUNKS/%d.%d.%d' % (dev.config.path, chunk.fid, chunk.cid, chunk.version)

    def delete_chunks(self, chunks, dev):
        """Delete chunk"""
        for chunk in chunks:
            chunk = Chunk(chunk) # Translate dict to Chunk object
            file = self._get_chunk_path(chunk, dev)
            debug('Delete %s', file)
            if not os.path.exists(file):
                continue
            # Safe delete
            try:
                dev.config.used -= get_psize(file)
                os.remove(file)
            except OSError:
                pass

        # Save change to disk
        dev.flush()
