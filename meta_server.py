#!/usr/bin/python

"""Meta server

Start a daemon server for meta service, pid and debug log files are under the
config.home directory: 
    meta_server.pid
    meta_server.log

storage, chunk servers are similar.
"""
import thread

import config
from oodict import OODict
from nio_server import *
from meta import *
from journal import *

# Set loggers
init_logging(os.path.join(config.home, 'meta_server.log'))

server = NIOServer()
server.set_pid_file(os.path.join(config.home, 'meta_server.pid'))
if config.daemon:
    server.daemonize()

# Meta as ops + cp replay :  for cp only, generate objects
# Replay journals
meta = MetaService()
meta.mode = 'replay'
cper = CheckPoint(config.meta_dev)
cper.set_handler(meta)
cp = OODict()
if not cper.latest:
    debug('Create new tree')
    meta._object_shard.create_tree()
    cp = meta.get_tree()
    cp.committed_journal_id = -1
    cper.save_cp(cp)
else:
    cper.do_CP(True)


# Create processer for the event processing state machine
request_processer = RequestProcesser()
journal_processer = JournalProcesser()
response_processer = ResponseProcesser()

# Do some init stuff 
request_processer.ops = meta

# Chain them up
request_processer.next = journal_processer
journal_processer.next = response_processer

# OK, start them all
request_processer.start()
journal_processer.start()
response_processer.start()


       
        

# Start checkpoint thread
cper.start()

request_queue = []
journal_queue = []
reponse_queue = []
clients = {}


# Start epoll server
server.request_processer = request_processer
server.start()

# Meta + journal + mem + objects: for request processing
meta = MetaService()
meta.init_tree(cper.cp)
try:
    server.register('meta', meta)
    server.bind(config.meta_server_address)
    server.mainloop()
except Exception, err:
    debug(err)
    raise
