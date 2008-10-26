#!/usr/bin/python
# coding: utf8

"""
High Availabile NetWork IO
CopyRight (C) 2008 Chen Zheng <nkchenz@gmail.com>

Distributed under terms of GPL v2
"""

import socket
import time
import pprint
from oodict import *

class Request(OODict):

    def __init__(self):
        pass

class NetWorkIO:
    """
    本模块的作用是在TCP之上层建立可靠的会话连接，屏蔽网络故障对应用造成的影响，提高可用性
    
    异步写入socket，req转送到等待响应队列。socket读取是同步的，该线程不断的从该socket读取数据
    
    数据结构：
    req = {
    'function': 'facebook.user.getid'
    'args':{'name': 'chenz'}
    'callback': #异步状态变化通知，如果没有设置则使用默认值。同步IO忽略此项。
    
    #以下数据系统内部自动生成
    'id': # 自动生成的标识
    'status': 'committed'
    'birth_time':   # 开始时间，用于计算超时
    'retry': # 剩余重试次数。
    # 如何处理非等幂操作？ 服务器端应该保存一个队列，如果是重复操作，简单返回结果即可
     
    # 调用成功时的返回值
    'returns': {'id': 17389289} 

    'error': error messages
    }


    发送数据包：
        version
        id
        function
        args
        payload
        ack, done, commit
        payload_length if has payload
        returns

    non block call:
        Call
        send_request_non_block

    Sender thread:
        do_send_request
           create_socket
        do_send_request_raw

    """

    def create_socket(self):
        """
        Create a new socket to remote host. Retry self.socket_error_retry times, 
        so total operations is self.socket_error_retry+1.
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        t = 1
        retry = self.retrys_on_socket_connect_error 
        while True:
            try:
                start = time.time()
                s.connect((self.remote_host, self.remote_port))
                return s
            except socket.error, err:
                print 'socket.error', err, '%ds used' % (time.time() - start)
                if retry <= 0: # No more retrys
                    return None
                print 'sleep %d seconds, %d more retrys' % (t, retry)
                time.sleep(t)
                t *= 2
                retry -= 1

    def send_request_non_block(self, req):
        # Deal with queue, send request, non blocking mode, with errors handling 
        # Add to sending queue
        # return req
        # Should we start a timer?
        pass

    def send_request(self, req):
        """Send request in blocking mode
        What should we do if request has been committed by server, but client does not receive
        any result because of network error? Should we retry?

        timeout? retry?
        """
        if self.do_send_request(req):
            # Check errors

            # Receive ack, done, committed, message
            return req.returns
 
    def sender(self):
        """Sending thread """
        while True:
            if not self.wait4_send_queue:
                # Should use a condition var here
                time.sleep(1)
                continue

            req = self.wait4_send_queue.remove[0]
            # Check timeout?
            if self.do_send_request(req):
                self.wait4_ack_queue.append(req)
            else:
                self.done_queue.append(req) # Error


    def do_send_request(self, req):
        """
        If retry 3 times, both error, shall we continue or return error?
        """
        retry = self.retrys_on_socket_send_error 
        while True:
            try:
                if not self.socket:
                    self.socket = self.create_socket()
                    if not self.socket:
                        req.error = 'socket can\'t be created'
                        return False
                if self.do_send_request_raw(req):
                    return True
            except socket.error, err:
                self.socket = None
                # There must be something wrong with the socket, so just create a new one
                if retry <= 0: # No more retrys
                    req.error = 'no more send retrys '
                    return False
                # Do not need sleep here
                retry -= 1


    def do_send_request_raw(self, req):
        """If send ok, return True, else return False. Let upper layer care about errors"""
        msg = req.msg 
        msg_encoded = pprint.pformat(msg) + '\n\n'
        data = '%d\n%s%s' % (len(msg_encoded), msg_encoded, req.payload)
        # socket.send does not guarantee sending all data
        remain_len = len(data)
        while True:
            try:
                size = self.socket.send(data)
                remain_len -= size
                if remain_len <=0:
                    return True
                data = data[-remain_len:]
            except socket.error, err:
                print 'do_send_request_simple', error
                return False # socket error: broken pipe? closed? reset?

    def request_to_msg(self, req):
        """Pack request"""
        msg = OODict()
        msg.version = req.version
        msg.id = req.id
        msg.function = req.function
        msg.args = req.args
        #data.ack = self.ack # 需要ack服务器端吗？
        if req.payload:
            msg.payload_length = len(req.payload)
        return msg 


    def Call(self, function, args, payload = '', block = True, retry = None, timeout = None):
        """
        整个操作在timeout之后会取消，放弃执行，不管是同步还是异步IO
        """
        req = OODict()
        # Construct request
        req.function = function
        req.args = args 
        #if not retry:
        #    retry = self.default_request_retry
        if not timeout:
            timeout = self.default_request_timeout
        req.retry = retry
        req.birth_time = time.time()
        req.expire_time = req.birth_time + timeout
        
        req.id = self.req_id
        self.req_id += 1
        req.version = 1
        req.payload = payload

        req.error = ''
        # Save msg for later use
        req.msg = self.request_to_msg(req)
 
        # Return True if sent successfully. Please check req.returns for results
        if block:
            return self.send_request(req)
        else:
            return self.send_request_non_block(req)

    def Cancel(self, id):
        #取消调用的执行
        pass

    def __init__(self, host, port):
        self.remote_host = host
        self.remote_port = port

        self.wait4_send_queue = []
        self.wait4_ack_queue = []
        self.wait4_done_queue = []
        self.wait4_commit_queue = []
        self.committed_queue = []

        # 发送出去之后，启动重试超时
        # 默认重试，超时时间。如果Call给定的值为0，则使用此值
        self.timeout_on_no_ack = 30 # 如果过此时间仍没有收到ack，则重新发送请求  
        self.retrys_on_no_ack = 3
        self.default_request_timeout = 300

        # Retry numbers on socket connect error
        self.retrys_on_socket_connect_error = 7 
        self.retrys_on_socket_send_error = 3 

        # Signal for sender thread to quit
        self.sender_shutdown = False
        
        self.req_id = 0
        self.ack_id = 0
        self.done_id = 0
        self.commit_id = 0

        self.socket = None


    def Close(self):
        self.socket.close()
        # And other cleanup stuffs


class NetworkServiceServer:
    """
    高并发，可以同时处理多个客户连接
    c10k经典问题
    """

if __name__ == '__main__':

    foo = NetWorkIO('localhost', 1984)
    #foo = NetWorkIO('baidu.com', 80)

    foo.Call('fs.cp', {'src':'/foo', 'dest': '/bar'})
    #foo.Call('fs.ls', {'file':'/'})

    foo.Close()


'''
# Examples

# 异步IO状态变化时调用此函数, 这个callback是在另一个线程中被调用，不知道是否会有影响
def aio_status_handler(req):
    print 'req %d %s' % (req.id, req.status)

foo = NetworkIO('localhost', 1984)
foo.default_status_change_hanlder = aio_status_handler

# 获得新会话, args={}，block=True同步返回，使用默认重试和超时参数
foo.Call('session.open', {})
if not foo.session:
    print 'Can not open new session '
    sys.exit(1)

req.function = 'fs.mkdir'
req.args = {'file':'a b c'}
foo.Call(req)

foo.Call('fs.ls', {'file':'/'})
foo.Call('fs.cp', {'src':'/foo', 'dest': '/bar'})

for req in write_reqs:
    foo.Call(req, block = False)

foo.Call('fs.write', {'file':'/foo', 'offset': 1024, 'payload': data}, block = False, retry = 8, timeout = 60s)

req = 'fs.read'
args ={'file':'/foo', 'offset': 1024, 'size': 1M}
if not foo.Call(req,  timeout = 60s)
    print 'Error:', req.status
else:
    print len(req.returns)

foo.Close()
'''
