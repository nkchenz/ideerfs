#!/usr/bin/python

"""Meta server

Start a daemon server for meta service, pid and debug log files are under the
config.home directory: 
    meta_server.pid
    meta_server.log

storage, chunk servers are similar.
"""
import config
from meta import *
from processer import *
from journal_processer import *
from checkpointd import *
from epoll_server import *

# Set loggers
init_logging(os.path.join(config.home, 'meta_server.log'))

server = EPollServer(addr = config.meta_server_address, pidfile = os.path.join(config.home, 'meta_server.pid'))
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
journal_processer = JournalProcesser(config.meta_dev)
response_processer = ResponseProcesser()

# Do some init stuff 
meta = MetaService()
# Load the consistent meta image
image = cpd.load_cp()
meta._load_image(image)
# Tell request processer the meta ops handler
request_processer.register_service('meta', meta)
# Tell the server where to submit completed request
server.request_processer = request_processer
# Tell responser processer how to send response
response_processer.handler = server.response_processer_callback

# Chain them up
request_processer.next = journal_processer # After a request is processed, journal it
journal_processer.next = response_processer # After journaled, send response back

# OK, start them all
info('Starting request processer')
request_processer.start()
journal_processer.start()
info('Starting response processer')
response_processer.start()
server.start()
