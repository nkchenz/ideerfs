#!/usr/bin/python

"""Meta server

Start a daemon server for meta service, pid and debug log files are under the
config.home directory: 
    meta_server.pid
    meta_server.log

storage, chunk servers are similar.
"""
import config
from oodict import OODict
from meta import *
from processer import *
from journal_processer import *
from cp_processer import *
from epoll_server import *

# Set loggers
init_logging(os.path.join(config.home, 'meta_server.log'))

server = EPollServer(addr = config.meta_server_addr, pidfile = os.path.join(config.home, 'meta_server.pid'))
# Daemonize server
if config.daemon:
    server.daemonize()

# Replay journals
replayer = MetaService()
cpd = CheckPointD(config.meta_dev)
cpd.handler = replayer
cpd.start()

# Create processer for the event processing state machine
request_processer = RequestProcesser()
journal_processer = JournalProcesser()
response_processer = ResponseProcesser()

# Do some init stuff 
meta = MetaService()
# Load the consistent meta image
image = cpd.load_cp()
meta._load_image(image)
request_processer.register_service('meta', meta)
server.request_processer = request_processer
response_processer.handler = server.response_processer_callback

# Chain them up
request_processer.next = journal_processer
journal_processer.next = response_processer

# OK, start them all
request_processer.start()
journal_processer.start()
response_processer.start()

# Start epoll server
server.start()

