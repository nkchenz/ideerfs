# coding: utf8

import pprint
import socket
from conf import *
from msg import *
from rc import *

class MessageChannel:
    """
    A message channel for peers
    """

    def __init__(self, sk, host):
        self.sk = sk
        self.host = host
        self.msg_sn = 0 # Serial number for messages on this channel

    def close(self):
        self.sk.close()

    def decode(self, data):
        """Decode msg"""
        return eval(data) # Not safe, be sure to check it's only a plain dict

    def encode(self, msg):
        return pprint.pformat(msg) + '\n\n'

    def send(self, msg):
        """
        Send msg atomatic, payload is either sent or fail

        If we want AIO, need to add send recv queue, timeout, callback 
        """
        msg.data.sn = self.msg_sn
        self.msg_sn += 1
        if msg.payload:
            msg.data.payload_length = len(msg.payload)
        data = self.encode(msg.data)
        self.sk.send('%d\n%s' % (len(data), data))
        if not msg.payload: # Don't have payload
            return MSG_SEND_OK
        if self.sk.send(msg.payload) != msg.data.payload_length:
            return MSG_SEND_ERR # Something wrong with connection
        return MSG_SEND_OK

    def readline(self, sk):
        data = ''
        while True:
            c = sk.recv(1)
            if not c:
                return None
            if c == '\n':
                return data
            data += c

    def recv(self):
        length = self.readline(self.sk)
        if not length:
            return None
        data = self.sk.recv(int(length))
        if not data:
            return None
        msg = Message(self.decode(data))
        if 'payload_length' not in msg.data: # Simple message
            return msg
        msg.payload = None
        data = self.sk.recv(msg.data.payload_length)
        if len(data) != msg.data.payload_length:
            return None # Partial receive error, check connection
        msg.payload = data
        return msg

def NewChannel(ip, port):
    sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sk.connect((ip, port))
    except socket.error, err:
        print 'error:', err
        return None
    return MessageChannel(sk, ip)
