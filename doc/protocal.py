# coding: utf8

import pprint
from oodict import OODict

class Protocal:
    """All messages based on dict format:
        version 4 bytes
        length  4 bytes
        msg
        payload if 'payload_length' in msg
    """

    def send_message(self, f, req):
        msg = OODict()
        msg.id = req.id
        msg.function = req.method
        msg.args = req.args
        #data.ack = self.dack # 需要ack服务器端吗？
        if 'payload' in req:
            msg.payload_length = len(req.payload)
        msg_encoded = pprint.pformat(msg) 
        ver = 1
        data = pack('!ii', ver, len(msg_encoded)) + msg_encoded
        if 'payload' in req:
            data += req.payload
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
    
    def read_message(self, f):
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
            return msg
        except:
            # Socket error
            return None


    
class TestClientProtocal(Protocal):
    
    def echo(self, f, req):
        f.send_message(req)
        return f.read_message()

    def echo_result(self):
        pass
        

class TestServerProtocal(Protocal):
    pass

