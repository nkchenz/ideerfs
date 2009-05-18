"""Network IO interface, like the RPC"""

import os
import socket
import time
from protocol import *
from exception import *

class NetWorkIO:
    """
    TODO:
        non block io
        retrans on no answer


    msg member begins with '_' is for maintain use only

    Application layer should have its own open/close handshaking other than using
    TCP's.
    """

    def __init__(self, addr):
        #self.timeout_on_no_ack = 30 
        #self.retrys_on_no_ack = 3

        # Retry numbers on socket connect error
        self.retrys_on_socket_connect_error = 5
        self.retrys_on_socket_send_error = 0

        self.socket = None
        self.remote_addr = addr

        self.req_id = 0

    def _connect(self):
        """Connect socket to node, with self.retrys_on_socket_connect_error retrys"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        retry = self.retrys_on_socket_connect_error 
        t = 1
        while True:
            try:
                start = time.time()
                self.socket.connect(self.remote_addr)
                return 
            except socket.error, err:
                print 'socket.error', err, '%ds used' % (time.time() - start)
                if retry <= 0: # No more retrys
                    raise NoMoreRetryError('socket connect error')
                print 'sleep %d seconds, %d more retrys' % (t, retry)
                time.sleep(t)
                t *= 2
                retry -= 1

    def request(self, req):
        if not self.socket:
            self._connect()

        req._id = self.req_id
        self.req_id += 1

        retry = self.retrys_on_socket_send_error
        while True:
            try:
                send_message(self.socket, req)
                break
            except socket.error, err:
                # There must be something wrong with this socket
                # Create a new one
                self._connect()
                # Only retry when connected, 
                if retry <= 0:
                    raise NoMoreRetryError('socket send error')
                # Do not need sleep here
                retry -= 1

        # Retrans here FIXME
        # What if error happens while read answer? Should we retry?
        # Set a timer here to get ack
        msg = read_message(self.socket)
        return msg
    
    
    def call(self, method, **args):
        req = OODict()
        req.method = method
        for k, v in args.items():
            req[k] = v
            
        resp = self.request(req)
        if 'error' in resp:
            raise ResponseError(resp.error)

        # Response must have 'value' if not have 'error'
        # What about payload?
        if 'payload' in resp:
            return resp.value, resp.payload
        else:
            return resp.value

    def close(self):
        self.socket.close()

class Messager:

    def __init__(self):
        self._processer = aio.AIO()
        self._sockets = {}
        self._connect_error_retry = 3
        self._send_error_retry = 3
        self._id = 0

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
                    raise NoMoreRetryError('socket connect error')
                time.sleep(2 ** retry)
                retry += 1

    def request(self, req):
        if not self.socket:
            self._connect()

        req._id = self.req_id
        self.req_id += 1

        retry = self.retrys_on_socket_send_error
        while True:
            try:
                send_message(self.socket, req)
                break
            except socket.error, err:
                # There must be something wrong with this socket
                # Create a new one
                self._connect()
                # Only retry when connected, 
                if retry <= 0:
                    raise NoMoreRetryError('socket send error')
                # Do not need sleep here
                retry -= 1

        # Retrans here FIXME
        # What if error happens while read answer? Should we retry?
        # Set a timer here to get ack
        msg = read_message(self.socket)
        return msg
    
    def call(self, method, **args):
        req = OODict()
        req.method = method
        for k, v in args.items():
            req[k] = v
            
        resp = self.request(req)
        if 'error' in resp:
            raise ResponseError(resp.error)

        # Response must have 'value' if not have 'error'
        # What about payload?
        if 'payload' in resp:
            return resp.value, resp.payload
        else:
            return resp.value

    def call(self, addr, method, **args, block = True, timeout = 0):
        # How to detect socket errors? how to resend message and reconnect
        # socket?

        # Connect socket
        if addr not self._sockets:
            self._connect(addr)

        # Pack message
        req = OODict()
        req.method = method
        for k, v in args.items():
            req[k] = v
        req._id = self._id
        self._id += 1

        retry = self.retrys_on_socket_send_error
        while True:
            try:
                send_message(self.socket, req)
                break
            except socket.error, err:
                # There must be something wrong with this socket
                # Create a new one
                self._connect()
                # Only retry when connected, 
                if retry <= 0:
                    raise NoMoreRetryError('socket send error')
                # Do not need sleep here
                retry -= 1

        # Retrans here FIXME
        # What if error happens while read answer? Should we retry?
        # Set a timer here to get ack
        msg = read_message(self.socket)
        return msg
    
        if 'error' in resp:
            raise ResponseError(resp.error)

        # Response must have 'value' if not have 'error'
        # What about payload?
        if 'payload' in resp:
            return resp.value, resp.payload
        else:
            return resp.value
        pass

'''
socket.check from kfs
241 bool TcpSocket::IsGood()
242 {
243     if (mSockFd < 0)
244         return false;
245 
246 #if 0
247     char c;
248     
249     // the socket could've been closed by the system because the peer
250     // died.  so, tell if the socket is good, peek to see if any data
251     // can be read; read returns 0 if the socket has been
252     // closed. otherwise, will get -1 with errno=EAGAIN.
253     
254     if (Peek(&c, 1) == 0)
255         return false;
256 #endif
257     return true;
258 }
'''
