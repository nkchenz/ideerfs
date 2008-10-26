# coding: utf8

import pprint
from struct import pack, unpack
from oodict import OODict

def send_message(f, req):
    # Put bulk data aka payload at the end
    payload = None
    if 'payload' in req:
        payload = req.payload
        req.payload_length = len(req.payload)
        del req['payload']
    msg_encoded = pprint.pformat(req) 
    ver = 1
    data = pack('!ii', ver, len(msg_encoded)) + msg_encoded
    if payload:
        data += payload
    # socket.send does not guarantee sending all data
    sent = 0
    total = len(data)
    while True:
        try:
            size = f.send(data[sent:])
            sent += size
            if sent >= total:
                return True
        except err:
            return False # socket error: broken pipe? closed? reset?

def read_message(f):
    try:
        ver, length = unpack('!ii', f.recv(8))
        data = f.recv(length)
        if not data:
            return None
        msg = OODict(eval(data))
        if 'payload_length' not in msg: # Simple message
            return msg
        payload = f.recv(msg.payload_length)
        if len(payload) != msg.payload_length:
            return None # Partial receive error, check connection
        msg.payload = payload
        del msg['payload_length']
        return msg
    except:
        # Socket error
        return None

