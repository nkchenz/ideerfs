"""
Chunk service
"""

import os
import hashlib
import time
import thread

from oodict import OODict
from util import *
from dev import *
from nio import *
from io import *
from service import *
import config

class ChunkService(Service):
    """
    Chunk server needs to know all the using devices at startup. One way is through
    configuration file, which contains all the devices exported, but what happens
    when we want to add some devices, do we need to reboot the chunk server? If not,
    there should be some methods to inform the server.

    No such device, chunk server doesn't care about dev status: online,
    offline... the status is only kept on storage manager, the lattter pull
    chunk server for chunk report.

    When offlining a disk, how can we know no one is writting on it? write lock?

    Instead of this, we implement local storage management as part of the server,
    command 'ideer.py storage' communicates with local chunk server through sockets
    interface chunk.admin_dev.
    """
    def __init__(self, addr):
        self._addr = config.chunk_server_address
        self._db = FileDB(config.home)
        self._devices_file = 'devices'
        self._chunk_shard = ChunkShard()
        thread.start_new_thread(self._heartbeat, ())

    def _update_devices(self):
        """It's better to be signaled when changes happened than updating everytime before
        each operation"""
        self._devices = self._db.load(self._devices_file)

    def _lookup_dev(self, id):
        # Lookup device path from devices_exported by id
        self._update_devices()
        if id not in self._devices:
            self._error('dev not exists')
        # Check disk status
        dev = Dev(self._devices[id])
        if not dev.config or dev.config.status != 'online':
            self._error('dev not online')
        return dev

    def write(self, req):
        """
        Write data to a chunk of version 'req.version', after that increase
        version by 1, need to rename file.

        If req.new = True, means create a new chunk


        # Checksum is based on whole chunk, which means, every time we have to
        # read or write the whole chunk to verify it, a little bit overkill.
        # Hashlist can be used here, split a chunk to 10 small ones, read n
        # small chunks which contain the data you want, offset+len

        """
        dev = self._lookup_dev(req.dev_id)
        if dev.config.mode == 'frozen':
            self._error('dev frozen')

        # There should be a safe limit of free space. Even if it's a old
        # chunk, we may write to the hole in it, still need extra space
        if dev.config.size - dev.config.used < len(req.payload) * 3:
            self._error('no enough space')

            c = Chunk()
            c.fid = req.object_id
            c.id = req.chunk_id
            c.version = req.version
            c.size = req.chunk_size

        return 'ok'


    def read(self, req):
        dev = self._lookup_dev(req.dev_id)
        try:
            c = Chunk()
            c.fid = req.object_id
            c.id = req.chunk_id
            c.version = req.version
            data = self._chunk_shard.load_chunk(dev, c)
            # Load chunk from chunk_shard
        except IOError, err:
            self._error(err.message)

        if req.offset + req.len > len(data):
            self._error('chunk read out of range')
        return 'ok', data[req.offset: req.offset + req.len] # This is a payload


    def delete(self, req):
        """Delete chunks directly"""
        pass

    def replicate(self, req):
        pass


    def _heartbeat(self):
        """Heartbeat message to storage server
        upstream: devices infos such as size, used change
        downstream: deleted chunks
        """
        nio = NetWorkIO(config.storage_server_address)
        while True:
            # Lock
            changed = self.devices_changed
            self.devices_changed = {} 
            # Unlock

            rc = nio.call('storage.heartbeat', addr = self._addr, changed_devs = changed)

            # Delete old chunks
            for dev, chunks in rc.deleted_chunks.items():
                for chunk in chunks:
                    self._delete_chunk(dev, chunk)

            time.sleep(5)


