#!/usr/bin/python
#coding: utf8

"""
Client for ideerfs


"""
import os
import sys
import cmd
import socket

IDEERFS_PORT = 51984
REQUEST_HEADER_SIZE = 1024
BUFFER_SIZE = 1024 * 32 # 32k read and write buffer for network traffic 

class cli_client(cmd.Cmd):

    def __init__(self, host):
        cmd.Cmd.__init__(self)
        self.prompt = "$ "
        self.conn = conn(host)
        print host, 'connected'
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
        self.conn.send_msg({'cmd': 'cp', 'src': files[0], 'dest': files[1]})
        print self.conn.recv_msg()

    def do_ls(self, args):
        self.conn.send_msg({'cmd': 'ls', 'file': args})
        print self.conn.recv_msg()

    def emptyline(self):    
        pass

    def default(self, line):       
        print line

class conn:
    def __init__(self, host):
        self.sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.sk.connect((host, IDEERFS_PORT))
        except socket.error, err:
            print 'error', err
            sys.exit(-1)

    def close(self):
        self.sk.close()


    def send_msg(self, msg, payload = None):
        msg['version'] =  1
        #if not payload:
        fmt = '%%-%ds' % REQUEST_HEADER_SIZE
        self.sk.send(fmt % str(msg))

    def recv_msg(self):
        # Should read REQUEST_HEADER_SIZE exactly
        header = self.sk.recv(REQUEST_HEADER_SIZE)
        msg = eval(header) # not safe, be sure to check it's only a plain dict
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


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'usage: client.py host'
        sys.exit(-1)
    a = cli_client(sys.argv[1])
    a.cmdloop() 
