"""Event processing state machine
Two basic layers: request, response
    worker.request.next = response

Plugin layers: journal
    worker.request.next = journal
    worker.journal.next = response

Fixme: How to detech exception of processing thread and restart it?

"""

import thread
import threading
from logging import info, debug

from oodict import OODict
from exception import *
from util import *

class Processer:

    def __init__(self):
        self.queue = []
        self.next = None # Next processer in the state machine
        self.queue_empty = threading.Condition()

    def start(self):
        thread.start_new_thread(self.mainloop, ())

    def mainloop(self):
        while True:
            self.queue_empty.acquire()
            while len(self.queue) == 0:
                self.queue_empty.wait()
            self.queue_empty.release()

            item = self.queue.pop(0) # Proccessing from head
    
            result = self.processing(item)
            # Error happens during processing item, need queue it back
            if result is None:
                self.submit(item)
                continue
            
            # If we are not the last step, pass item to the next processer
            if self.next:
                self.next.submit(result) # Pass result to the next processer

    def processing(self, item):
        """Process each item, please reimplement this function
        
        If you want item be queued back, return None.

        Anything not None will be passed to next processer, and causes item be
        deleted in self.queue
        """
        return item

    def submit(self, item):
        self.queue_empty.acquire()
        self.queue.append(item)
        self.queue_empty.notify()
        self.queue_empty.release()


class RequestProcesser(Processer):

    def __init__(self):
        self.services = {}
        Processer.__init__(self)

    def register_service(self, name, ops):
        self.services[name] = ops

    def handle_req(self, req):
        service, method = req.method.split('.')
        if service not in self.services:
            raise RequestHandleError('unknown service %s' % service)

        try:
            handler = getattr(self.services[service], method)
        except AttributeError:
            raise RequestHandleError('unknown method %s' % method)

        if not callable(handler):
            raise RequestHandleError('unknown method %s' % method)

        return handler(req)

    def processing(self, req):
        response = OODict()
        response._id = req._id
        
        try:
            result = self.handle_req(req) # Must set req._status
            if result is None:
                return None # Request not done, need to queue back
            # For complicated calls, read for example, should return a tuple: value, payload
            if isinstance(result, tuple):
                response.value, response.payload = result
            else:
                response.value = result
        except RequestHandleError, err:
                response.error = str(err)

        #debug('Processed, request: %s response: %s', filter_req(req), filter_req(response))
        # Journal processer needs req, but response processer needs resp!
        response._req = req
        return response

class ResponseProcesser(Processer):
    """Please set self.handler to the real function sending response to
    network"""

    def processing(self, response):
        return self.handler(response)

