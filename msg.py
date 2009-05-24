"""Messager layer"""

import os
import socket
import time
import thread
import protocol
import exception

from oodict import OODict

class Messager:

    def __init__(self):
        self._sockets = {}
        self._connect_error_retry = 3
        self._send_error_retry = 3
        self._id = 0
        self._id_lock = thread.allocate_lock()

    def _connect(self, addr):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        retry = 0
        while True:
            try:
                s.connect(addr)
                self._sockets[addr] = s
                break
            except socket.error, err:
                print 'socket.error', err
                if retry >= self._connect_error_retry: # No more retrys
                    raise exception.NoMoreRetryError('socket connect error')
                time.sleep(2 ** retry)
                retry += 1

    def call(self, dest, method, **args):
        # How to detect socket errors? how to resend message and reconnect
        # socket?
        # Connect socket
        if dest not in self._sockets:
            self._connect(dest)

        # Pack message
        req = OODict()
        req.method = method
        for k, v in args.items():
            req[k] = v

        # Lock id here
        self._id_lock.acquire()
        req._id = self._id
        self._id += 1
        self._id_lock.release()

        retry = 0
        while True:
            try:
                protocol.send_message(self._sockets[dest], req)
                break
            except socket.error, err:
                # There must be something wrong with this socket
                # Create a new one
                self._connect(dest)
                # Only retry when connected, 
                if retry >= self._send_error_retry:
                    raise exception.NoMoreRetryError('socket send error')
                # Do not need sleep here
                retry += 1

        # Retrans here FIXME
        # What if error happens while read answer? Should we retry?
        # Set a timer here to get ack
        resp = protocol.read_message(self._sockets[dest])
        if 'error' in resp:
            raise exception.ResponseError(resp.error)

        # Response must have 'value' if not have 'error'
        # What about payload?
        if 'payload' in resp:
            return resp.value, resp.payload
        else:
            return resp.value

    def bye(self, addr):
        # Fixme: we should close connections which are inactive
        if addr in self._sockets:
            self._sockets[addr].close()

messager = Messager()

