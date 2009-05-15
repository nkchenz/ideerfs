"""AIO model for network socket"""

import select
import thread
import threading
from logging import info, debug

from oodict import OODict

class AIO:

    def __init__(self):
        self.io_queue = {}
        self.epoll = select.epoll()
        thread.start_new_thread(self._mainloop, ())

    def read(self, fp, length, callback = None):
        return self._enqueue(fp, '', length, callback, 'read_queue', select.EPOLLIN)

    def write(self, fp, data, callback = None):
        return self._enqueue(fp, data, 0, callback, 'write_queue', select.EPOLLOUT)

    def wait(self, io):
        if io.fp.fileno() not in self.io_queue:
            raise # Closed socket
        io.done_cv.acquire()
        while not io.done:
           io.done_cv.wait()
        io.done_cv.release()

    def _enqueue(self, fp, data, length, callback, queue, mask):
        fn = fp.fileno()
        io = OODict()
        io.fn = fn
        io.data = data
        io.length = length
        io.callback = callback
        io.done_cv = threading.Condition()
        io.done = False
        
        if fn not in self.io_queue:
            self.io_queue[fn] = OODict()
            self.io_queue[fn][queue] = []
            self.io_queue[fn].mask = mask
            self.io_queue[fn].fp = fp
            self.io_queue[fn].errs = 0
            # fp must be a socket
            fp.setblocking(0)
            self.epoll.register(fn, self.io_queue[fn].mask)

        if self.io_queue[fn][queue]:
            self.io_queue[fn][queue].append(io)
        else:
            self.io_queue[fn][queue] = [io]
            self.io_queue[fn].mask |= mask
            self.epoll.modify(fn, self.io_queue[fn].mask)
        return io

    def _clear_mask(self, fn, mask):
        self.io_queue[fn].mask &= ~mask

    def _mainloop(self):
        while True:
            events = self.epoll.poll(1) # Timeout 1 second
            for fn, event in events:
                try:
                    if event & select.EPOLLIN:
                        self._epoll_in(fn)
                    elif event & select.EPOLLOUT:
                        self._epoll_out(fn)
                    elif event & select.EPOLLHUP:
                        self._epoll_hup(fn)
                except socket.error, err:
                    if err.errno == 11: # Catch the Errno
                        pass
                    else:
                        raise
        self.epoll.close()

    def _recv(self, client, size):
        """Read as most size bytes data, if there are too many error
        epollin events, close connection.
        """
        data = ''
        try:
            while True:
                tmp = client.fp.recv(size)
                if not tmp:
                    break
                data += tmp
                size -= len(tmp)
                if size <= 0:
                    break
        except socket.error, err:
            if err.errno == 11:
                pass 
            else:
                raise
        if not data:
            client.errs += 1
            if client.errs >= 3:
                self._epoll_hup(client.fp.fileno())
            return None
        client.errs = 0
        return data

    def _epoll_in(self, fn):
        client = self.io_queue[fn]
        while client.read_queue:
            io = client.read_queue[0]
            wanted = io.length - len(io.data)
            tmp = self._recv(client, wanted)
            io.data += tmp
            if len(tmp) < wanted:
                return  # No more data available
            io = client.read_queue.pop(0)
            io.done_cv.acquire()
            io.done = True
            io.done_cv.notify()
            io.done_cv.release()
            if io.callback:
                io.callback(io)

        self._clear_mask(fn, select.EPOLLIN)

    def _epoll_out(self, fn):
        client = self.io_queue[fn]
        while client.write_queue:
            io = client.write_queue[0]
            size = client.fp.send(io.data[io.length:])
            if not size:
                return
            io.length += size
            if io.length < len(io.data):
                return
            else:
                io = client.write_queue.pop(0)
                io.done_cv.acquire()
                io.done = True
                io.done_cv.notify()
                io.done_cv.release()
                if io.callback:
                    io.callback(io)
        
        self._clear_mask(fn, select.EPOLLOUT)

    def _epoll_hup(self, fn):
        self.epoll.unregister(fn)
        self.io_queue[fn].fp.close()
        del self.io_queue[fn]

