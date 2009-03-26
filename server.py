"""Simple TCP server framework """

import os
import socket
import sys
import thread
import threading

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

    def __init__(self):
        self.shutdown = False
        self.__set_signals()
        self._pid_file = ''

    def config(self, pid_file):
        if os.path.exists(pid_file):
            print 'Pid file exists, already started?'
            sys.exit(-1)
        self._pid_file = pid_file

    def bind(self, addr):
        """Bind socket"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # For socket.error: (98, 'Address already in use')
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(addr)
        return True

    def mainloop(self):
        # Dispatcher, check conn pools for readable incoming messages
        self.socket.listen(MAX_WAITING_CLIENTS)
        while True:
            if self.shutdown: # Gracefully shutdown
                break
            try:
                conn = self.socket.accept()
                #self.request_handler(conn) # For debug only
                #thread.start_new_thread(self.request_handler, (conn,))
                p = threading.Thread(target = self.request_handler, args = (conn,))
                p.start()
            except socket.error, err:
                if err[0] == 4:
                    print 'CTRL+C'
                else:
                    raise
        # Release port
        self.socket.close()
        print 'Shutdown OK'
        sys.exit(0) # Exit even if there are active connection threads

    def daemonize(self):
        """Switch to background"""
        if os.fork() > 0:
            sys.exit(0)
        #os.chdir("/") 
        os.umask(0)
        os.setsid()
        if os.fork() > 0:
            sys.exit(0)

        if self._pid_file:
            os.system('echo %d > %s' %(os.getpid(), self._pid_file))

    def __cleanup(self, signal, dummy):
        print 'Cleanup'
        self.shutdown = True
        if self._pid_file and os.path.exists(self._pid_file):
            os.remove(self._pid_file)
 
    def __set_signals(self):
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

