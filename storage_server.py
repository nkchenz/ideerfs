#!/usr/bin/python

"""Storage manager """

import config
from storage import *
from processer import *
from epoll_server import *

init_logging(os.path.join(config.home, 'storage_server.log'))

server = EPollServer(addr = config.storage_server_address, pidfile = os.path.join(config.home, 'storage_server.pid'))
# Daemonize server
if config.daemon:
    server.daemonize()

request_processer = RequestProcesser()
response_processer = ResponseProcesser()
request_processer.next = response_processer

server.request_processer = request_processer
response_processer.handler = server.response_processer_callback

info('Starting request processer')
request_processer.start()
info('Starting response processer')
response_processer.start()

storage = StorageService()
request_processer.register_service('storage', storage)
server.start()
