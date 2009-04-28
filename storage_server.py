#!/usr/bin/python

"""Storage manager """

import config
from nio_server import *
from storage import *

init_logging(os.path.join(config.home, 'storage_server.log'))
server = NIOServer(addr = config.storage_server_address, pidfile = os.path.join(config.home, 'storage_server.pid'))
if config.daemon:
    server.daemonize()
server.register('storage', StorageService())
server.start()
