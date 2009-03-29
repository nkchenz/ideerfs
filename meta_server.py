#!/usr/bin/python

"""Meta server

Start a daemon server for meta service, pid and debug log files are under the
config.home directory: 
    meta_server.pid
    meta_server.log

storage, chunk servers are similar.
"""

import config
from nio_server import *
from meta import *

# Set loggers
init_logging(os.path.join(config.home, 'meta_server.log'))

server = NIOServer()
server.set_pid_file(os.path.join(config.home, 'meta_server.pid'))
if config.daemon:
    server.daemonize()
server.register('meta', MetaService())
server.bind(config.meta_server_address)
server.mainloop()
