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
    meta device"""

    def __init__(self):
        self._shard = Dev(config.meta_dev)
        self._objects = self._shard.load('objects', {})
        self._seq_file = 'seq'
        self._seq = self._shard.load(self._seq_file)

    def get_object_path(self, id):
        """Method to tranfer object id to path on disk"""
        return os.path.join(self._shard.meta_dir, id2path(id))

    def flush(self):
        self._shard.store(self._seq, self._seq_file)
        self._shard.store(self._objects, 'objects')

    def create_object(self):
        if self._seq is None:
            raise 'seq file crrupt' # Fatal error
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
    
    """
    def __init__(self):
        pass

    def load_chunk(self, chunk, dev):
        """Read chunk data"""
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
        """Store data from offset in chunk
        save data
        save checksum
        update version
        update dev.used
        """
        file = self._get_chunk_filepath(chunk, dev)
        # Get chunk data
        if new:
            chunk.psize = 0 # Orignal physical size
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

        # Rename if needed
        if not new:
            new_file = self._get_chunk_filepath(chunk, dev)
            os.rename(file, new_file)

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
        # file name must based on both object_id and chunk_id. If multicopies of a 
        # chunk are allowed to exist on same device too, there shall be something to
        # differentiate them.
        return '%s/CHUNKS/%d.%d.%d' % (dev.config.path, chunk.fid, chunk.id, chunk.version) 
 

    def delete_chunk(self, id, chunk):
        if id not in self.devices.cache:
            return
        dev = self.devices.cache[id]
        chf = os.path.join(dev.path, 'OBJECTS', self._hash2path(chunk))
        debug('delete chunk %s on %s' % (chunk, id))
        debug(dev.used)
        # Safe delete
        try:
            dev.used -= get_file_real_size(chf)
            self.devices._flush(dev.id)

            os.remove(chf)
        except OSError:
            pass
        debug(dev.used)

