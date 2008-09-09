# coding: utf8

from conf import *
from msg import *

class Conn:
    def __init__(self, socket):
        self.sk = socket

    def close(self):
        self.sk.close()

    def send(self, msg, payload = None):
        #if not payload:
        # Maybe it's better just add a four bytes field MSG_LEN before msg
        # So we can get rid of the restrictions imposed on message size
        if payload:
            msg.payload_length = len(payload)
        fmt = '%%-%ds' % REQUEST_HEADER_SIZE
        self.sk.send(fmt % str(msg))

        '''
        if not payload:
            return 
        remain_length = msg['payload_length'] 
        # We need set a timer here
        while True:
            data = self.sk.send(BvUFFER_SIZE)
            if not data:
                break
            msg['payload'] += data
            remain_length -= len(data)
            if remain_length <= 0:
                break
        # If len(msg['payload']) != msg['payload_length'], there must be errors
        # Let upper layer care about this
        return msg
        '''
        # 应该有一个io调度缓冲层, 包括超时机制在内，事务?
        # 不应该在这里读取数据，大文件的情况下上层代码可能需要对部分收到的数据进行处理

    def recv(self):
        # Should read REQUEST_HEADER_SIZE exactly
        header = self.sk.recv(REQUEST_HEADER_SIZE)
        if not header:
            return None

        msg = Message(eval(header)) # not safe, be sure to check it's only a plain dict
        if 'payload_length' not in msg:
            return msg

        msg['payload'] = ''
        remain_length = msg['payload_length'] 
        # We need set a timer here
        while True:
            data = self.sk.recv(BUFFER_SIZE)
            if not data:
                break
            msg['payload'] += data
            remain_length -= len(data)
            if remain_length <= 0:
                break
        # If len(msg['payload']) != msg['payload_length'], there must be errors
        # Let upper layer care about this
        return msg

