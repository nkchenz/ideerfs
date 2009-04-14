#!/usr/bin/python

"""Meta server

Start a daemon server for meta service, pid and debug log files are under the
config.home directory: 
    meta_server.pid
    meta_server.log

storage, chunk servers are similar.
"""

import config
from oodict import OODict()
from nio_server import *
from meta import *

# Set loggers
init_logging(os.path.join(config.home, 'meta_server.log'))

server = NIOServer()

# Meta as ops + cp replay :  for cp only, generate objects
# Replay journals
meta = MetaService()
meta.mode = 'replay'
cper = CheckPointer(config.meta_dev)
cper.set_handler(meta)
cp = OODict()
if not cper.latest:
    meta._object_shard.init_tree() # Create a new tree
    cp = meta.checkpoint()
    cper.save_cp(cp, True)
else:
    cper.do_CP()

# Start checkpoint thread
cper.start()

# Meta + journal + mem + objects: for request processing
meta = MetaService()
meta.init(cper.cp)
server.set_pid_file(os.path.join(config.home, 'meta_server.pid'))
if config.daemon:
    server.daemonize()
try:
    server.register('meta', meta)
    server.bind(config.meta_server_address)
    server.mainloop()
except Exception, err:
    debug(err)
    raise
