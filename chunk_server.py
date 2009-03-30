#!/usr/bin/python

"""Chunk server"""

import config
from nio_server import *
from chunk import *

init_logging(os.path.join(config.home, 'chunk_server.log'))
server = NIOServer()
server.set_pid_file(os.path.join(config.home, 'chunk_server.pid'))
if config.daemon:
    server.daemonize()
try:
    server.register('chunk', ChunkService())
    server.bind(config.chunk_server_address)
    server.mainloop()
except Exception, err:
    debug(err)
    raise

