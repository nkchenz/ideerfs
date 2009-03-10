"""
OnWire protocal
"""

import pprint
import socket
from struct import pack, unpack
from oodict import OODict
from util import *

BUFFER_SIZE = 1024 * 1024 * 8 
# I'm not sure whether buffer is needed here, for sending hundreds of megabytes using one socket.send

def send_message(f, req):
    # Put bulk data aka payload at the end
    payload = None
    if 'payload' in req:
        payload = req.payload
        req.payload_length = len(req.payload)
        del req['payload']
    msg_encoded = pprint.pformat(req) 
    version = 1
    data = pack('!ii', version, len(msg_encoded)) + msg_encoded
    if payload:
        data += payload
    #print data
    # socket.send does not guarantee sending all data
    sent = 0
    total = len(data)
    while True:
            size = f.send(data[sent: sent + BUFFER_SIZE])
            sent += size
            if sent >= total:
                return True
    # Let socket.error go to upper layer
    #except socket.error, err:
    #    print err
    #    return False # socket error: broken pipe? closed? reset?

def read_message(f):
        data = f.recv(8)
        if not data:
            return None
        ver, length = unpack('!ii', data)
        data = f.recv(length)
        if not data:
            return None
        msg = OODict(eval(data))
        if 'payload_length' not in msg: # Simple message
            return msg
        
        done_len = 0
        payload = ''
        while True:
            data = f.recv(BUFFER_SIZE)
            done_len += len(data)
            payload += data
            if done_len >= msg.payload_length:
                break
        
        msg.payload = payload
        del msg['payload_length']
        return msg


