"""Chunk service"""

import os
import time
import thread

from oodict import OODict
from util import *
from dev import *
from obj import *
from io import *
from service import *
import config
from msg import messager

class ChunkService(Service):
    """Service chunks on devices

    Device status from config file is checked first.
    When offlining a disk, how can we know no one is writing on it? write lock?
    """

    def __init__(self):
        self._addr = config.chunk_server_address
        self._db = FileDB(config.home)
        self._devices_file = 'exported_devices'
        self._chunk_shard = ChunkShard()
        self._devices_changed = []
        
        info('Starting heartbeat thread')
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
        return dev

    def _mark_changed(self, did):
        """Mark the dev changed, we will update its real used size to storage
        server later in the heartbeat message"""
        # Lock me
        if did not in self._devices_changed:
            self._devices_changed.append(did)

    def write(self, req):
        """Write data to a chunk of version 'req.version', after that increase
        version by 1, need to rename file.
        If req.new = True, means create a new chunk
        # Checksum is based on whole chunk, which means, every time we have to
        # read or write the whole chunk to verify it, a little bit overkill.
        # Hash list can be used here, split a chunk to 10 small ones, read n
        # small chunks which contain the data you want, offset+len

        @chunk          fid, cid, version, size
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
            chunk = Chunk(req.chunk)
            self._chunk_shard.store_chunk(req.chunk, req.offset, req.payload, dev, req.new)
            self._mark_changed(req.did)
        except IOError, err:
            self._error(err)
        return 'ok'

    def read(self, req):
        """Read from chunk
        @did        device id
        @chunk
        @offset     offset
        @len        bytes want to read
        """
        dev = self._lookup_dev(req.did)
        try:
            chunk = Chunk(req.chunk)
            chunk = self._chunk_shard.load_chunk(chunk, dev)
            data = chunk.data
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
        while True:
            # Lock
            tmp = self._devices_changed
            self._devices_changed = []
            confs = {}
            # Unlock

            # Read newest status of devs
            # changed = {}
            for did in tmp:
                try:
                    confs[did] = self._lookup_dev(did).config
                except:
                    continue

            rc = messager.call(config.storage_server_address, 'storage.heartbeat', addr = self._addr, confs = confs)
            if rc.needreport:
                # Start another thread to send chunk reports
                thread.start_new_thread(self._send_chunk_reports, ())

            # Delete old chunks
            for did, chunks in rc.deleted_chunks.items():
                try:
                    dev = self._lookup_dev(did)
                except:
                    continue
                self._mark_changed(did)
                self._chunk_shard.delete_chunks(chunks, dev)

            time.sleep(5)

    def _send_chunk_reports(self):
        self._update_devices()
        for did, path in self._devices.items():
            info('Onlining %s %s' % (did, path))
            os.system('./ideer.py storage online %s' % path)
