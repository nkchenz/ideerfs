"""
Object interface
"""

import time
import os
import hashlib

from dev import *
from oodict import OODict
import config


class Object(OODict):

    def __init__(self, name, id, parent_id, type, attr = {}):
        self.id = id
        self.type = type
        self.meta = {
            'ctime': '%d' % time.time(),
            'name': name
        }
        
        for k,v in attr.items():
            self.meta[k] = v
        
        if type == 'dir':
            self.children = {
                '.': id,
                '..': parent_id
            }
        else:
            self.meta['size'] = 0
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

    def create_root_object(self, path):
        """Create root object"""
        self._shard = Dev(path)
        if not self._shard.config:
            raise IOError('%s not formatted' % path)
        self._seq = 0
        self._objects = {}
        self._root = self.create_object()
        self.store_object(Object('/', self._root, self._root, 'dir'))
        self._shard.store(self._root, self._root_file)
        self.flush()

    def get_object_path(self, id):
        """Method to transfer object id to path on disk"""
        #return os.path.join(self._shard.meta_dir, id2path(id))
        pass

    def get_root_object():
        return self._root

    def flush(self):
        # Check point here
        self._shard.store(self._seq, self._seq_file)
        self._shard.store(self._objects, 'objects')

    def create_object(self):
        if self._seq is None:
            raise IOError('seq file corrupted') # Fatal error
        # Lock
        self._seq += 1
        self.flush()
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
    
    """
    def __init__(self):
        self._chunks_file = 'chunks'
        self._chunks = {}

    def _load_chunk_db(self, dev):
        did = dev.config.id
        if did not in self.chunks:
            self.chunks[did] = dev.load(self._chunks_file)

    def _insert_chunk_entry(self, chunk, dev):
        """Add one entry for new chunk"""
        # Lockme
        self._load_chunk_db(dev)
        tmp = chunk.fid, chunk.cid, chunk.verion
        if tmp not in self.chunks[did]:
            self.chunks[did].append(tmp)

    def _delete_chunk_entry(self, chunk, dev):
        """Add one entry for new chunk"""
        # Lockme
        self._load_chunk_db(dev)
        tmp = chunk.fid, chunk.cid, chunk.verion
        if tmp in self.chunks[did]:
            self.chunks[did].remove(tmp)

    def _flush_chunk_db(self, dev):
        dev.store(self.chunks[dev.config.id], self._chunks_file)

    def load_chunk(self, chunk, dev):
        """Read chunk data from device
        @fid
        @cid
        @version
        @dev
        """
        file = self.get_chunk_path(chunk, dev)
        if not os.path.isfile(file):
            raise IOError('chunk lost or stale')

        fp = open(file, 'r')
        try:
            header = eval(fp.read(CHUNK_HEADER_SIZE))
            if not header:
               raise
        except:
            fp.close()
            raise IOError('chunk header corrupt')

        data = fp.read(header.size)
        if len(data) != header.size:
            fp.close()
            raise IOError('chunk data lost')

        if header.algo == 'sha1':
            if hashlib.sha1(data).hexdigest() != header.checksum:
                fp.close()
                raise IOError('chunk data corrupt')

        fp.close()
        return data

    def _update_checksum(chunk, offset, data):
        """Update checksum of a chunk"""
        if offset + len(data) > chunk.size:
            raise IOError('chunk write out of range')
        tmp = chunk.data[:offset] + data + chunk.data[offset + len(data):]
        # We do not write tmp back to disk here because in that case chunk
        # file will not be sparse
        chunk.algo = 'sha1'
        chunk.checksum = hashlib.sha1(tmp).hexdigest()

    def store_chunk(self, chunk, offset, data, dev, new = False):
        """Store data to offset of chunk

        @chunk              chunk info: fid, cid, version, size
        @offset
        @data
        @dev
        @new                Is this a new chunk?
        """
        file = self._get_chunk_filepath(chunk, dev)
        # Get chunk data
        if new:
            chunk.psize = 0 # Original physical size
            chunk.data = zeros(chunk.size)
            open(file, 'w').close() # Create new file 
        else:
            chunk.data = self.load_chunk(chunk, dev)

        # Write data
        self._write_chunk_data(file, offset, data)

        # Update check sum
        self._update_checksum(chunk, offset, data)
        
        # Get new physical size
        old_psize = chunk.psize
        chunk.psize = get_psize(file)

        if not new: # If new is True, means that this is just a replica
            chunk.version += 1

        # Write header back
        self._write_chunk_header(file, chunk)

        # Add chunk entry
        self._insert_chunk_entry(chunk, dev)
        # Rename if needed
        if not new:
            new_file = self._get_chunk_filepath(chunk, dev)
            os.rename(file, new_file)
            # Delete old version entry, chunk is no use anymore, just minus 1
            # to get old chunk
            chunk.version -= 1
            self._delete_chunk_entry(chunk, dev)
        self._flush_chunk_db(dev)

        # Update dev.used
        dev.config.used += chunk.psize - old_psize
        dev.flush()
        return 'ok'
        
    def _write_chunk_data(file, offset, data):
        f = open(file, 'rb+') # Update file
        f.seek(offset + CHUNK_HEADER_SIZE)
        f.write(data)
        f.close()

    def _write_chunk_header(file, chunk):
        f = open(file, 'rb+') # Update file
        del chunk.data
        f.write(pformat(chunk)[:CHUNK_HEADER_SIZE]) # Only 1024 for header
        f.close()

    def get_chunk_path(self, chunk, dev):
        # Because different chunks of same object may exists on same device, chunk
        # file name must based on both object_id and chunk_id. If multi copies of a 
        # chunk are allowed to exist on same device too, there shall be something to
        # differentiate them.
        return '%s/CHUNKS/%d.%d.%d' % (dev.config.path, chunk.fid, chunk.cid, chunk.version) 

    def delete_chunks(self, chunks, dev):
        """Delete chunk"""
        for chunk in chunks:
            file = self.get_chunk_path(chunk, dev)
            if not os.path.exists(file):
                continue
            # Safe delete
            try:
                dev.config.used -= get_psize(f)
                os.remove(file)
            except OSError:
                pass

            # Delete chunk entry
            # Lockme
            self._delete_chunk_entry(chunk, dev)

        # Save change to disk
        self._flush_chunk_db(dev)
        dev.flush()

