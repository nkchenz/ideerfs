"""OnWire protocol """

import pprint
import socket
from struct import pack, unpack
from oodict import OODict
from util import *
import cPickle

BUFFER_SIZE = 1024 * 1024 * 8 
# I'm not sure whether buffer is needed here, for sending hundreds of megabytes using one socket.send

def pack_message(req):
    """Pack req dict to binary data"""
    # Get payload length
    payload = None
    if 'payload' in req:
        payload = req.payload
        req.payload_length = len(req.payload)
        del req['payload']

    # Generate message body
    msg_encoded = cPickle.dumps(req, cPickle.HIGHEST_PROTOCOL)

    # version, length of message body
    version = 1
    data = pack('!ii', version, len(msg_encoded))
    # message body
    data += msg_encoded
    # Payload
    if payload:
        data += payload

    return data

def unpack_message_body(data):
    """Unpack message body from data, set msg._end if we carry payload"""
    l = len(data)
    if l < 8:
        return None # version and length not complete

    ver, length = unpack('!ii', data[:8])
    msg_end = 8 + length
    # Read message body
    if l < msg_end:
        return None # Message body not complete

    msg = OODict(cPickle.loads(data[8: msg_end]))
    if 'payload_length' in msg: # For payload
        msg._end = msg_end
    return msg

def unpack_payload(data, msg):
    """Unpack payload from data for msg"""
    payload_end = msg._end + msg.payload_length
    if len(data) < payload_end:
        return None # Payload not complete
    payload = data[msg._end: payload_end]
    del msg['_end']
    del msg['payload_length']
    return payload

def unpack_message(data):
    msg = unpack_message_body(data)
    if msg is None:
        return None
    if 'payload_length' not in msg: # Simple message
        return msg
    payload = unpack_payload(data, msg)
    if payload is None:
        return None
    msg.payload = payload
    return msg

def send_message(f, req):
    """Send a packet, put bulk data aka payload at the end
    
    return  bytes sent, if error happens, raise socket.error
    """
    data = pack_message(req)
    # Send, socket.send does not guarantee sending all data.
    # There might be socket.error exceptions, upper layer should handle them
    # socket error: broken pipe? closed? reset?
    sent = 0
    total = len(data)
    while True:
        size = f.send(data[sent: sent + BUFFER_SIZE])
        sent += size
        if sent >= total:
            return total

def read_message(f):
    """Read a packet
    
    return msg received, if error happens, raise socket.error
    """
    # Read two bytes: message version and length
    data = f.recv(8)
    if not data:
        raise socket.error('socket recv error')
    ver, length = unpack('!ii', data)

    # Read message body
    data = f.recv(length)
    if not data:
        raise socket.error('socket recv error')
    msg = OODict(cPickle.loads(data))
    if 'payload_length' not in msg: # Simple message
        return msg
    
    # Read payload
    done_len = 0
    payload = ''
    while True:
        data = f.recv(BUFFER_SIZE)
        if not data:
            raise socket.error('socket recv error')
        done_len += len(data)
        payload += data
        if done_len >= msg.payload_length:
            break
    
    msg.payload = payload
    del msg['payload_length']
    return msg
