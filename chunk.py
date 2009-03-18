"""
Chunk service
"""

import os
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
    """Service chunks on devices

    Device status from config file is checked first.
    When offlining a disk, how can we know no one is writing on it? write lock?
    """

    def __init__(self, addr):
        self._addr = config.chunk_server_address
        self._db = FileDB(config.home)
        self._devices_file = 'exported_devices'
        self._chunk_shard = ChunkShard()
        self._changed_devices = []
        thread.start_new_thread(self._heartbeat, ())

    def _update_devices(self):
        """It's better to be signaled when changes happened than updating every time before
        each operation"""
        self._devices = self._db.load(self._devices_file)

    def _lookup_dev(self, did):
        """Lookup device path from exported_devices by id"""
        self._update_devices()
        if did not in self._devices:
            self._error('dev not exists')
        # Check disk status
        dev = Dev(self._devices[did])
        if not dev.config or dev.config.status != 'online':
            self._error('dev not online')
        return dev

    def _mark_changed(self, did):
        """Mark the dev changed, we will update its real used size to storage
        server later in the heartbeat message"""
        # Lock me
        if did not in self._changed_devices:
            self._changed_devices.append(did)

    def write(self, req):
        """Write data to a chunk of version 'req.version', after that increase
        version by 1, need to rename file.
        If req.new = True, means create a new chunk
        # Checksum is based on whole chunk, which means, every time we have to
        # read or write the whole chunk to verify it, a little bit overkill.
        # Hash list can be used here, split a chunk to 10 small ones, read n
        # small chunks which contain the data you want, offset+len

        @fid, cid, version, size
        @offset, data
        @did            device id
        @new            whether to create a new chunk

        """
        dev = self._lookup_dev(req.did)
        if dev.config.mode == 'frozen':
            self._error('dev frozen')

        # There should be a safe limit of free space. Even if it's a old
        # chunk, we may write to the hole in it, still need extra space
        if dev.config.size - dev.config.used < len(req.payload) * 3:
            self._error('no enough space')

        try:
            c = Chunk()
            c.fid, c.cid, c.version, c.size = req.fid, req.cid, req.version, req.size
            self._chunk_shard.store_chunk(c, req.offset, req.payload, dev, req.new)

            self._mark_changed(req.did)
        except IOError, err:
            self._error(err.message)
        return 'ok'

    def read(self, req):
        """Read from chunk
        @did        device id
        @fid        file object id
        @cid        chunk id
        @offset     offset
        @len        bytes want to read
        """
        dev = self._lookup_dev(req.did)
        try:
            c = Chunk()
            c.fid, c.cid, c.version = req.fid, req.cid, req.verion
            data = self._chunk_shard.load_chunk(c, dev)
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
            tmp = self.devices_changed
            self.devices_changed = [] 
            confs = {}
            # Unlock

            # Read newest status of devs
            # changed = {}
            for did in tmp:
                try:
                    confs[did] = self._lookup_dev(did).config
                except:
                    continue

            rc = nio.call('storage.heartbeat', addr = self._addr, confs = confs)

            # Delete old chunks
            for did, chunks in rc.deleted_chunks.items():
                try:
                    dev = self._lookup_dev(did)
                except:
                    continue
                self._mark_changed(did) 
                self._chunk_shard.delete_chunks(chunks, dev)

            time.sleep(5)
