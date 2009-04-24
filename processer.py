"""Event processing state machine
Two basic layers: request, response
    worker.request.next = response

Plugin layers: journal
    worker.request.next = journal
    worker.journal.next = response
"""

from journal import  Journal
from 

class Processer:

    def __init__(self):
        self.queue = []
        self.next = None # Next processer in the state machine

    def start(self):
        thread.start_new_thread(self.mainloop, ())

    def mainloop(self):
        while True:
            try:
                item = self.queue.pop(0) # Proccessing from head
            except IndexError:
                continue # Empty queue
    
            result = self.processing(item)

            # Error happens during processing item, need queue it back
            if result is None:
                self.submit(item)
                continue
            
            # If we are not the last step, pass item to the next processer
            if self.next:
                self.next.submit(req) # Pass it to the next processer

    def processing(self, item):
        """Process each item, please reimplement this function
        
        If you want item be queued back, return None.

        Anything not None will be passed to next processer, and causes item be
        deleted in self.queue
        """
        return item

    def submit(self, item):
        self.queue.append(item)
    

class RequestProcesser(Processer):

    def processing(self, req):

        # Call metaops to process req

        # metaops may set req.status 

        # journal processer needs req, but response processer needs resp!

        return response

class ResponseProcesser(Processer):

    def processing(self, response):
        # Processing

        clients.addr.reponse = rep

