#!/usr/bin/python
#coding: utf8

import os
import socket
import time
from protocal import *
from exception import *

class NetWorkIO:
    """
    TODO:
        non block io
        retrans on no answer


    msg member begins with '_' is for maintain use only

    """

    def __init__(self, ip, port):
        # 发送出去之后，启动重试超时
        # 默认重试，超时时间。如果Call给定的值为0，则使用此值
        #self.timeout_on_no_ack = 30 # 如果过此时间仍没有收到ack，则重新发送请求  
        #self.retrys_on_no_ack = 3

        # Retry numbers on socket connect error
        self.retrys_on_socket_connect_error = 5
        self.retrys_on_socket_send_error = 3

        self.socket = None
        self.remote_ip = ip
        self.remote_port = port

        self.req_id = 0

    def _connect(self):
        """Connect socket to node, with self.retrys_on_socket_connect_error retrys"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        retry = self.retrys_on_socket_connect_error 
        t = 1
        while True:
            try:
                start = time.time()
                self.socket.connect((self.remote_ip, self.remote_port))
                return 
            except socket.error, err:
                print 'socket.error', err, '%ds used' % (time.time() - start)
                if retry <= 0: # No more retrys
                    raise SocketConnectError
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
                if send_message(self.socket, req):
                    break
            except socket.error, err:
                # There must be something wrong with this socket
                # Create a new one
                self._connect()
                # Only retry when connected, 
                if retry <= 0:
                    raise SocketSendError
                # Do not need sleep here
                retry -= 1

        # What if error happens while read answer? Should we retry?
        # Set a timer here to get ack
        return read_message(self.socket)
