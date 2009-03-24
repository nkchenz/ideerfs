#!/usr/bin/python

"""Storage manager """

import config
from nio_server import *
from storage import *

server = NIOServer()
server.register('storage', StorageService())
server.bind(config.storage_server_address)
server.mainloop()

