"""Server using epoll method"""

import os
import select
from server import Server
from oodict import OODict
from protocol import *

BUFFER_SIZE = 1024 * 8

class EPollServer(Server):

    def mainloop(self):
        self.socket.setblocking(0) # Non blocking socket server

        epoll = select.epoll()
        epoll.register(self.socket.fileno(), select.EPOLLIN) # Level triggerred

        self.clients = {}

        while True:
            if self.shutdown: # Gracefully shutdown
                break

            events = epoll.poll(1) # Timeout 1 second
            for fileno, event in events:
                if fileno == self.socket.fileno(): # New connection on server socket
                    try:
                        while True:
                            sk, addr = self.socket.accept()
                            sk.setblocking(0) # Non blocking client socket
                            fn = sk.fileno()
                            epoll.register(fn, select.EPOLLIN) # Poll in 

                            # Init conn here
                            client = OODict()
                            client.addr = addr
                            client.sk = sk
                            client.request = None
                            client.request_data = ''
                            client.response = None
                            self.clients[fn] = client # Using fn as key
                    except socket.error:
                        pass


                elif event & select.EPOLLIN: # Although it's level triggerred, read or write as more as possible
                    # Read from fileno
                    client = self.clients[fileno]
                    try:
                        while True:
                            client.request_data += client.sk.recv(BUFFER_SIZE)
                    except socket.error:
                        pass
                   
                    # It's a little bit complicated here because we don't want
                    # to parse message body each time we look for payload
                    if client.request is None: # Parse message body if we dont have one
                        msg = unpack_message_body(client.request_data)
                        if msg is None:
                            continue # Message body not complete
                        client.request = msg

                    if 'payload_length' in client.request: # Check for payload
                        payload = unpack_payload(client.request_data, client.request)
                        if payload is None:
                            continue # Payload not complete
                        client.request.payload = payload
                        
                    # Everything is OK now, message body and payload both are complete
                    client.request_data = ''
                    client.request._fn = fileno # Who send this? 

                    # Submit to request queue
                    self.request_processer.submit(client.request)

                    # Init for next message
                    client.request = None
                    client.request_data = ''

                    # Wait for response
                    epoll.modify(fileno, select.EPOLLOUT)

                elif event & select.EPOLLOUT:
                    # Write to fileno
                    client = self.clients[fileno]
                    if client.response is None: # No response yet
                        continue
                    try:
                        sent = client.response_sent
                        while True:
                            size = client.sk.send(client.response_data[sent: sent + BUFFER_SIZE])
                            client.response_sent += size
                            if client.response_sent >= client.response_len:
                                # Message sent 
                                client.response = None
                                # Wait for response
                                epoll.modify(fileno, select.EPOLLIN)
                                break
                    except socket.error:
                        pass

                elif event & select.EPOLLHUP:
                    # Remote shutdown
                    client = self.clients[fileno]
                    client.sk.close()
                    del self.clients[fileno]

        epoll.unregister(self.socket.fileno())
        epoll.close()
        self.socket.close()

    def response_processer_callback(self, response):
        """Send response to network"""
        fn = response._req._fn
        client = self.clients[fn]
        if client.response: # Last response has not been sent out
            return None # Return None to tell response processer to queue back
        else:
            del response['_req']
            client.response = response
            client.response_data = pack_message(response)
            client.response_sent = 0 
            client.response_len = len(client.response_data)
            return 0 # OK
