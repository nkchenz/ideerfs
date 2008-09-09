#!/usr/bin/python
# coding: utf8

import os
import socket
import sys
import thread
import signal
from conf import *
from msg import *
from conn import *

shutdown = False

def usage():
    print 'ideerfs [start | stop]'
    sys.exit(-1)

def server_init():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # For socket.error: (98, 'Address already in use')
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  
    s.bind((LISTEN_ADDRESS, IDEERFS_PORT))
    return s

def worker(conn):
    s, addr = conn
    client = Conn(s)
    print 'Connected from', addr

    # 发送欢迎信息
    w = Message()
    w.cmd = 'welcome'
    w.info = WELCOME
    client.send(w)

    while True:
        msg = client.recv()
        if not msg or msg.cmd == 'bye':
            break
        client.send(msg)
    client.close()
    print addr, 'bye'

def server_start(s):
    s.listen(MAX_WAITING_CLIENTS)
    while True:
        if shutdown: # Gracefully shutdown server
            break
        conn = s.accept()
        thread.start_new_thread(worker, (conn,))
    # Release port
    s.close()

def cleanup(s, f):
    shutdown = True


if len(sys.argv) != 2 or sys.argv[1] not in ['start', 'stop']:
    usage()

if sys.argv[1] == 'stop':
    if not os.path.isfile(PID_FILE):
        print 'Not found pid file', PID_FILE
        sys.exit(-1)
    os.system('kill -9 %s' % open(PID_FILE, 'r').read())
    os.remove(PID_FILE)
    print 'stop ok'
    sys.exit(0)
    
if sys.argv[1] == 'start':
    #if os.path.isfile(PID_FILE):
    #    print 'Already started? Found pid file', PID_FILE
    #    sys.exit(-1)

    s = server_init()

    # 转换为后台进程
    """
    if os.fork() > 0:
        sys.exit(0)
    #os.chdir("/") 
    os.umask(0) 
    os.setsid() 
    if os.fork() > 0:
        sys.exit(0)
    """

    pid = '%s' % os.getpid()
    print pid
    open(PID_FILE, 'w+').write(pid)

    signal.signal(signal.SIGTERM, cleanup)
    signal.signal(signal.SIGINT, cleanup)

    server_start(s)

'''
pool = []
for i in range(int(opt.threads)):
    pool.append(threading.Thread(target = foo, args = (opt.cmd, opt.log, i)))

    print 'Start time ', time.ctime()
    s = time.time()
    map(lambda p: p.start(), pool)
    map(lambda p: p.join(), pool)

'''
