#!/usr/bin/python

"""Chunk server"""

import config
from nio_server import *
from chunk import *

init_logging(os.path.join(config.home, 'chunk_server.log'))
server = NIOServer()
server.daemonize()
server.set_pid_file(os.path.join(config.home, 'chunk_server.pid'))
server.register('chunk', ChunkService())
server.bind(config.chunk_server_address)
server.mainloop()

