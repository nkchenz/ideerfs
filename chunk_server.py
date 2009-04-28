#!/usr/bin/python

"""Chunk server"""

import config
from nio_server import *
from chunk import *

init_logging(os.path.join(config.home, 'chunk_server.log'))
server = NIOServer(addr = config.chunk_server_address, pidfile = os.path.join(config.home, 'chunk_server.pid'))
if config.daemon:
    server.daemonize()
server.register('chunk', ChunkService())
server.start()
