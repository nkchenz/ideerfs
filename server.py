"""Simple TCP server framework """

import os
import socket
import sys
import threading
from logging import info, debug

import signal

MAX_WAITING_CLIENTS = 128

class Server:
    """
    Simple TCP server framework
        socket reuse
        threading request handle
        Ctrl+C signals dealing
        daemonize
    """

    def __init__(self, addr, pidfile = ''):
        self.shutdown = False
        if os.path.exists(pidfile):
            print 'Pid file exists, already started?'
            sys.exit(-1)
        self.pidfile = pidfile
        self.addr = addr
        self._set_signals()

    def bind(self, addr):
        """Bind socket"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # For socket.error: (98, 'Address already in use')
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(addr)
        return True

    def mainloop(self):
        """Reimplement this if you want use asynchronous poll or epoll"""
        while True:
            if self.shutdown: # Gracefully shutdown
                break
            try:
                conn = self.socket.accept()
                #self.request_handler(conn) # For debug only
                p = threading.Thread(target = self.request_handler, args = (conn,))
                p.start()
            except socket.error, err:
                if err[0] == 4:
                    pass # print 'CTRL+C'
                else:
                    raise

    def start(self):
        if self.pidfile:
            os.system('echo %d > %s' %(os.getpid(), self.pidfile))
        # Dispatcher, check conn pools for readable incoming messages
        try:
            self.bind(self.addr)
            self.socket.listen(MAX_WAITING_CLIENTS)
            self.mainloop()
            # Release port
            self.socket.close()
            print 'Shutdown OK'
            sys.exit(0) # Exit even if there are active connection threads
        except Exception, err:
            debug(err)
            raise

    def daemonize(self, **files):
        """Switch to background
        keyword args: stdin, stdout, stderr
        """
        if os.fork() > 0:
            sys.exit(0)
        #os.chdir("/") 
        os.umask(0)
        os.setsid()
        if os.fork() > 0:
            sys.exit(0)

        # Redirect stdin, stdout, stderr here to null
        stds = ['stdout', 'stdin', 'stderr']
        for f in stds:
            if f not in files:
                files[f] = '/dev/null'

        inf = file(files['stdin'], 'r')
        out = file(files['stdout'], 'a+', 0)
        err = file(files['stderr'], 'a+', 0)
        os.dup2(inf.fileno(), sys.stdin.fileno())
        os.dup2(out.fileno(), sys.stdout.fileno())
        os.dup2(err.fileno(), sys.stderr.fileno())

    def __cleanup(self, signal, dummy):
        self.shutdown = True
        if self.pidfile and os.path.exists(self.pidfile):
            os.remove(self.pidfile)
 
    def _set_signals(self):
        signal.signal(signal.SIGTERM, self.__cleanup)
        signal.signal(signal.SIGINT, self.__cleanup)

    def request_handler(self, conn):
        """Simple echo server, please implement this function as your need"""
        f, addr = conn
        print 'Connected from', addr
        while True:
                data = f.recv(1024)
                if not data:
                    break
                f.send(data)
        f.close()
        print 'Bye', addr

if __name__ == '__main__':
    server = Server()
    server.bind(('localhost', 1984))
    # Use the default request_handler
    server.mainloop()

