#!/usr/bin/python
# coding: utf8

import os
import socket
import sys
import thread
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

    def bind(self, ip, port):
        """Bind socket"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # For socket.error: (98, 'Address already in use')
        #self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  
        self.socket.bind((ip, port))
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
                thread.start_new_thread(self.request_handler, (conn,))
            except socket.error, err:
                if err[0] == 4:
                    print 'CTRL+C'
                else:
                    raise
        # Release port
        self.socket.close()
        print 'Shutdown OK'

   
    def daemonize(self):
        """Switch to background"""
        if os.fork() > 0:
            sys.exit(0)
        #os.chdir("/") 
        os.umask(0) 
        os.setsid() 
        if os.fork() > 0:
            sys.exit(0)

    def __cleanup(self, signal, dummy):
        print 'Cleanup'
        self.shutdown = True
 
    def __set_signals(self):
        signal.signal(signal.SIGTERM, self.__cleanup)
        signal.signal(signal.SIGINT, self.__cleanup)

    def request_handler(self, conn):
        """Simple echo server, please implement this funtion as your need"""
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
    server.bind('localhost', 1984)
    # Use the default request_handler
    server.mainloop()

