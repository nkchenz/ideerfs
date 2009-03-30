#!/usr/bin/python

"""Storage manager """

import config
from nio_server import *
from storage import *

init_logging(os.path.join(config.home, 'storage_server.log'))
server = NIOServer()
server.set_pid_file(os.path.join(config.home, 'storage_server.pid'))
if config.daemon:
    server.daemonize()
try:
    server.register('storage', StorageService())
    server.bind(config.storage_server_address)
    server.mainloop()
except Exception, err:
    debug(err)
    raise
