#!/usr/bin/python

"""Meta server """

import config
from nio_server import *
from meta import *

server = NIOServer()
server.register('meta', MetaService())
server.bind(config.meta_server_address)
server.mainloop()

