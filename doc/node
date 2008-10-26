#!/usr/bin/python
#coding: utf8

import os
import socket


class NetWorkIO:
    """    

    NIO is complicated, you create a io channel between two nodes

    socket create and socket send are two different things, dont put them
    together.

    Both nodes should understand the data format of a protocal: request and 
    returns, but only server node need to know how to server, how to deal 
    with request.

    client node: be able to send a request and understand the return values

    MetaProtocalSpeaking
    BlockProtocalUnderstand

    MetaDataProtocal:
        MetaDataQueryProtocal: understand how to query metadata
            open, ls, mkdir, touch, rm

            meta.mkdir /ab
            meta.touch /ab/c
            meta.ls    /
            meta.rm
            meta.set
            meta.get

            block.read
            block.write
            block.replicate

            hb.alive

            open, close, flush, read, seek, tell, truncate, write, rm

        MetaDataAnswerProtocal: understand how to answer metadata

    ChunkDataProtocal:
        ChunkDataQueryProtocal
            read, write
        ChunkDataAnswerProtocal


    A reliable channel only one conversation is happening at any time

    Learn(DataNodeProtocal)
    Learn(MetaNodeProtocal)
    Learn(BlockNodeProtocal)
    Learn(ClientNodeProtocal)
    
    Listen:
        req
        lookup req
        do req

    Talk:
    Skills
    load(role)

    Speak

    #Can't understand
    """


    def __init__(self):
        # 发送出去之后，启动重试超时
        # 默认重试，超时时间。如果Call给定的值为0，则使用此值
        self.timeout_on_no_ack = 30 # 如果过此时间仍没有收到ack，则重新发送请求  
        self.retrys_on_no_ack = 3
        self.default_request_timeout = 300

        # Retry numbers on socket connect error
        self.retrys_on_socket_connect_error = 7 
        self.retrys_on_socket_send_error = 3 
        
        self.protos_spoken = []
        self.protos_understood = []
        self.sockets = {}
        self.port = 1984

    def __connect(self, node, port):
        """Connect socket to node, with self.retrys_on_socket_connect_error retrys"""
        self.sockets[node] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        retry = self.retrys_on_socket_connect_error 
        while True:
            try:
                start = time.time()
                self.sockets[node].connect((node, port))
                return True
            except socket.error, err:
                print 'socket.error', err, '%ds used' % (time.time() - start)
                if retry <= 0: # No more retrys
                    return False
                print 'sleep %d seconds, %d more retrys' % (t, retry)
                time.sleep(t)
                t *= 2
                retry -= 1

    def learn(self, proto):
        pass

    def request(node, req):
        """
        如果想对request的请求进行更精细的控制应该如何处理？

        #ideerfs.meta.ls
        #ideerfs.block.read
        #ed2k.get_meta
        """
        # Check proto
        proto = req.method.split('.')[0]
        if proto not in self.protos_spoken:
            req.error = 'unknown proto'
            return False

        if node not in self.sockets:
            if not self.__connect(node, self.port):
                req.error = 'connect error'
                return False

        retry = self.retrys_on_socket_send_error 
        while True:
            try:
                if proto.send(self.sockets[node], req):
                    return True
            except socket.error, err:
                # There must be something wrong with this socket
                # Create a new one
                if not self.__connect(node, self.port):
                    req.error = 'connect error'
                    return False
                # Only retry when connected, 
                if retry <= 0:
                    req.error = 'no more retrys '
                    return False
                # Do not need sleep here
                retry -= 1
        
        # Check return value, where to check? in this function or in proto.method()
        # If in this function, why bother proto?

        req.error = ''
        return True

if __name == '__main__':

    # 插装不同的模块

    i = Node()
    i.learn(MetaDataSendProtocal)
    i.learn(TestClientProtocal)
    i.learn(TestServerProtocal)
    i.learn(ChunkDataClientProtocal)

    
    i.test.echo('nihao')
    i.meta.mkdir('/foo')
    i.block.read(node, block_id)
    i.ed2k.get_meta('hash')


    i.request('localhost', 'test.echo', msg = 'nihao')
    
    mnode = 'localhost'
    dnode = 'localhost'

    i.request(mnode, 'meta.mkdir', dir = '/foo')
    i.request(mnode, 'meta.touch', dir = '/foo/bar')
    i.request(mnode, 'meta.ls', file = '/foo')

    mnode = NIO(metaserver)
    mnode.mkdir('/foo')
    mnode.touch('/foo/bar')
    mnode.ls(file)

    i.request('NodeB', 'MetaData.ls /')

    Node.Ask('metadata.ls', '/', 'nodeb')
        proto = function.split()[0]
        if not self.understand(proto):
            # I cant understand you
            print 'Sorry, but I cant understand proto:', proto
            return False
        self.Tell('nodeb', words)
        results = self.Listen()
        return results


    I.Request('NodeB', 'MetaData.ls /')

