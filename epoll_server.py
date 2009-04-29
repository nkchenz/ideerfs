"""Server using epoll method"""

import os
import select
from logging import info, debug

from server import Server
from oodict import OODict
from protocol import *
from util import *

BUFFER_SIZE = 1024 * 64 

class EPollServer(Server):

    def accept_clients(self):
        """Exception will be raised if there are no more connections to
        accept:
            [Errno 11] Resource temporarily unavailable
        """
        while True:
            sk, addr = self.socket.accept()

            if not sk or not addr:
                break # No more connections

            sk.setblocking(0) # Non blocking client socket
            self.epoll.register(sk.fileno(), select.EPOLLIN) # Poll in 

            # Init conn here
            client = OODict()
            client.addr = addr
            client.sk = sk
            client.request = None
            client.request_data = ''
            client.wrong_epollin_events = 0
            client.response = None
            self.clients[sk.fileno()] = client # Using fn as key
            debug('%s connected', addr)

    def mainloop(self):
        self.socket.setblocking(0) # Non blocking socket server
        self.clients = {}

        self.epoll = select.epoll()
        self.epoll.register(self.socket.fileno(), select.EPOLLIN) # Level triggerred

        while True:
            if self.shutdown: # Gracefully shutdown
                break
            events = self.epoll.poll(1) # Timeout 1 second
            for fileno, event in events:
                try:
                    if fileno == self.socket.fileno(): # New connection on server socket
                        self.accept_clients()
                    elif event & select.EPOLLIN: # Although it's level triggerred, read or write as more as possible
                        self.epoll_in(fileno)
                    elif event & select.EPOLLOUT:
                        self.epoll_out(fileno)
                    elif event & select.EPOLLHUP:
                        self.epoll_hup(fileno)
                except socket.error, err:
                    debug('epoll event exception: %s', err)
                    if err.errno == 11: # Catch the Errno
                        pass
                    else:
                        raise

        self.epoll.unregister(self.socket.fileno())
        self.epoll.close()
        self.socket.close()

    def epoll_in(self, fileno):
        # Read from fileno
        client = self.clients[fileno]
        data = ''
        # You will get 'Resource temporarily unavailable' exception as
        # expected if there is no more data while the connection is active.

        # But be very careful that there will be NO exceptions raised and
        # NO ERR or HUP events generated if you still recv or send after 
        # client side closed the connection.

        # You should figure out a way to unregister fileno of this
        # connection, or you will be flooded by dead empty events.
        
        # Read until no more data is available
        try:
            while True:
                tmp = client.sk.recv(BUFFER_SIZE)
                if not tmp:
                    break
                data += tmp
        except socket.error, err:
            if err.errno == 11:
                pass # Exception 11 is catched here because we need to save and parse received data
            else:
                raise

        if not data:
            client.wrong_epollin_events += 1
            if client.wrong_epollin_events >= 3:
                debug('Seen 3 consecutive empty EPOLLIN events, maybe connection %s is dead? Clean up', client.addr)
                self.epoll_hup(fileno)
            return
        client.wrong_epollin_events = 0
        client.request_data += data

        # It's a little bit complicated here because we don't want
        # to parse message body each time we look for payload
        if client.request is None: # Parse message body if we dont have one
            msg = unpack_message_body(client.request_data)
            if msg is None:
                return # Message body not complete
            client.request = msg

        if 'payload_length' in client.request: # Check for payload
            payload = unpack_payload(client.request_data, client.request)
            if payload is None:
                return # Payload not complete
            client.request.payload = payload
            
        # Everything is OK now, message body and payload both are complete
        client.request_data = ''
        client.request._fn = fileno # Who send this? 

        # Submit to request queue
        debug('Submit request %s', filter_req(client.request))
        self.request_processer.submit(client.request)

        # Init for next message
        client.request = None
        client.request_data = ''

        # Wait for response
        self.epoll.modify(fileno, select.EPOLLOUT)

    def epoll_out(self, fileno):
        client = self.clients[fileno]
        if client.response is None: # No response yet
            return
        while True:
            size = client.sk.send(client.response_data[client.response_sent: client.response_sent + BUFFER_SIZE])
            if not size:
                return # Can't send anything out
            client.response_sent += size
            if client.response_sent >= client.response_len:
                # Message sent 
                client.response = None
                # Wait for request
                self.epoll.modify(fileno, select.EPOLLIN)
                return

    def epoll_hup(self, fileno):
        # Remote shutdown
        # Hup or ERR events can't be generated when remote client closed the
        # connection, don't know why
        self.epoll.unregister(fileno)
        client = self.clients[fileno]
        debug('%s closed', client.addr)
        client.sk.close()
        del self.clients[fileno]

    def response_processer_callback(self, response):
        """Send response to network"""
        fn = response._req._fn
        client = self.clients[fn]
        if client.response: # Last response has not been sent out
            return None # Return None to tell response processer to queue back
        else:
            del response['_req']
            debug('Send back response %s', filter_req(response))
            client.response_data = pack_message(response)
            client.response_sent = 0 
            client.response_len = len(client.response_data)
            client.response = response
            return 0 # OK
