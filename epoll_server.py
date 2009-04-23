"""Server using epoll method"""

import os
from server import Server
import select

class EPollServer(Server):


    def request

    def loop(self):

        # Start request processing thread

        self.socket.setblocking(0) # Non blocking socket server

        epoll = select.epoll()
        epoll.register(self.socket.fileno(), select.EPOLLIN) # Level triggerred

        self.clients = {}
        self.request_queue = {}

        while True:
            if self.shutdown: # Gracefully shutdown
                break

            events = epoll.poll(1) # Timeout 1 second
            for fileno, event in events:
                if fileno == self.socket.fileno(): # New connection on server socket
                    while True:
                        conn, addr = self.socket.accept()
                        conn.setblocking(0) # Non blocking client socket
                        epoll.register(conn.fileno(), select.EPOLLIN | select.EPOLLOUT) # Poll in or out

                        # Init conn here
                elif event & select.EPOLLIN:
                    # Read from fileno
                    
                    # If we have a complete message, submit it to request_queue

                elif event & select.EPOLLOUT:
                    # Write to fileno
                    
                    # If a message has been set, remove it from request_queue
                    #

                    # Bye message?

                elif event & select.EPOLLHUP:

                    # Remote shutdown

                    # Clean up client

        epoll.unregister(self.socket.fileno())
        epoll.close()
        self.socket.close()

