#!/usr/bin/python
# coding: utf8

"""
agent is like a server
"""

import os
import socket
import sys
import thread
import signal
from conf import *
from msg import *
from channel import *

class Agent:
    """
    A agent is responsible for sending and recving messages
    """

    def __init__(self):
        self.shutdown = False

    def init(self, ip, port):
        """Bind port"""
        self.sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # For socket.error: (98, 'Address already in use')
        self.sk.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  
        try:
            self.sk.bind((ip, port))
        except:
            raise
            return False
        return True

    def start(self):
        self.sk.listen(MAX_WAITING_CLIENTS)
        while True:
            if self.shutdown: # Gracefully shutdown
                break
            try:
                conn = self.sk.accept()
                thread.start_new_thread(self.worker, (conn,))
            except:
                pass
        # Release port
        self.sk.close()
        print 'Shutdown OK'

    def cleanup(self, signal, dummy):
        print 'Cleanup'
        self.shutdown = True
    
    def daemon(self):
        """Swtich to background"""
        if os.fork() > 0:
            sys.exit(0)
        #os.chdir("/") 
        os.umask(0) 
        os.setsid() 
        if os.fork() > 0:
            sys.exit(0)

    def set_signal(self):
        signal.signal(signal.SIGTERM, self.cleanup)
        signal.signal(signal.SIGINT, self.cleanup)

    def worker(self, conn):
        sk, addr = conn
        print 'Connected from', addr
        mc = MessageChannel(sk)

        # 发送欢迎信息
        w = Message()
        w.data.cmd = 'welcome'
        w.data.info = WELCOME_MESSAGE
        mc.send(w)

        # 应该有一种方法，只要有活动的channel，worker就一直忙碌，而不会阻塞
        # 一个线程可以同时服务多个channel
        while True:
            msg = mc.recv()
            if not msg or msg.data.cmd == 'bye':
                break
            mc.send(msg)
        mc.close()
        print 'Bye', addr

agent = Agent()
#if os.path.isfile(PID_FILE):
#    print 'Already started? Found pid file', PID_FILE
#    sys.exit(-1)
agent.init(LISTEN_ADDRESS, LISTEN_PORT)
agent.set_signal()
#ideerfs.daemon()

pid = '%s' % os.getpid()
print pid
open(PID_FILE, 'w+').write(pid)

agent.start()
