#!/usr/bin/python
#coding: utf8

"""
Client for ideerfs
"""
import os
import sys
import cmd
import socket
from conn import *
from msg import *

class CMDClient(cmd.Cmd):

    def __init__(self, host):
        cmd.Cmd.__init__(self)
        self.prompt = "$ "

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((host, IDEERFS_PORT))
        except socket.error, err:
            print 'error:', err
            sys.exit(-1)

        self.conn = Conn(s)
        print host, 'connected'
        print self.conn.recv()
        # Read welcome message

    def do_exit(self, args):
        self.conn.close()
        return -1

    def do_EOF(self, args):
        return self.do_exit(args)

    def do_help(self, args):
        cmd.Cmd.do_help(self, args)

    def do_cp(self, args):
        files = args.split()
        if len(files) != 2:
            print 'error: cp src dest'
            return
        msg = Message()
        msg.version = 1
        msg.cmd = 'cp'
        msg.src = files[0]
        msg.dest = files[1]
        self.conn.send(msg)
        print self.conn.recv()

    def do_ls(self, args):
        msg = Message()
        msg.version = 1
        msg.cmd = 'ls'
        msg.file = args
        self.conn.send(msg)
        print self.conn.recv()

    def emptyline(self):    
        pass

    def default(self, line):       
        print line

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'usage: client.py host'
        sys.exit(-1)
    a = CMDClient(sys.argv[1])
    a.cmdloop() 
