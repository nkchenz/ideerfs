#!/usr/bin/python

import os
import socket
import sys
import time
import thread
import daemon
import syslog


host = 'localhost'                 # Symbolic name meaning the local host
port = 51984              # Arbitrary non-privileged port
backlog = 128
welcome = 'Welcome to ideerfs, Friends!\n'

# Make sure you have write permission of these files, abosolute path please
pid_file = '/home/master/ideerfs.pid'
log_file = '/home/master/ideerfs.log'


def usage():
    print 'ideerfs [start | stop]'
    sys.exit(-1)

def server_init():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    return s

def worker(conn):
    s, addr = conn
    s.send(welcome)
    while True:
        data = s.recv(1024)
        if not data:
            break
        s.send(time.ctime() + ' ' + data)
    s.close()

def server_start(s):
    s.listen(backlog)
    try:
        while True:
            conn = s.accept()
            thread.start_new_thread(worker, (conn,))
    except:
        # Release port
        s.close()
        raise


if len(sys.argv) != 2 or sys.argv[1] not in ['start', 'stop']:
    usage()

if sys.argv[1] == 'stop':
    if not os.path.isfile(pid_file):
        print 'Not found pid file', pid_file
        sys.exit(-1)
    os.system('kill -9 %s' % open(pid_file, 'r').read())
    os.remove(pid_file)
    sys.exit(0)
    
if sys.argv[1] == 'start':
    if os.path.isfile(pid_file):
        print 'Already started? Found pid file', pid_file
        sys.exit(-1)


    s = server_init()

    if os.fork() > 0:
        sys.exit(0)
    os.chdir("/") 
    os.umask(0) 
    os.setsid() 
    if os.fork() > 0:
        sys.exit(0)
    
    pid = '%s' % os.getpid()
    print pid
    open(pid_file, 'w+').write(pid)

    server_start(s)

'''
pool = []
for i in range(int(opt.threads)):
    pool.append(threading.Thread(target = foo, args = (opt.cmd, opt.log, i)))

    print 'Start time ', time.ctime()
    s = time.time()
    map(lambda p: p.start(), pool)
    map(lambda p: p.join(), pool)


    try:
	s = socket.socket(af, socktype, proto)
    except socket.error, msg:
	s = None
	continue
    try:
	s.bind(sa)
	s.listen(1)
    except socket.error, msg:
	s.close()
	s = None
	continue
    break

if s is None:
    print 'could not open socket'
    sys.exit(1)
conn, addr = s.accept()
print 'Connected by', addr
while 1:
    data = conn.recv(1024)
    if not data: break
    conn.send(data)
conn.close()
'''
