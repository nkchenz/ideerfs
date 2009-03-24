#!/usr/bin/python

"""Chunk server"""

import config
from nio_server import *
from chunk import *

server = NIOServer()
server.register('chunk', ChunkService())
server.bind(config.chunk_server_address)
server.mainloop()

