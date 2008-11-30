#!/usr/bin/python
# coding: utf8

import sys
from oodict import OODict
from util import *
import socket

import time
import hashlib

class Dev:
    def __init__(self, path = None):
        if path:
            self.config_manager = ConfigManager(os.path.join(path))
            self.config_file = 'config'
            self.config = self.config_manager.load(self.config_file, OODict())
    
    def init(self, args):
        # path, host, size are all in args
        args.used = 0
        args.status = ''
        args.mode = ''
        #args.data_type = 'chunk' #chunk, meta
        args.type = 'file'  #raid, mirror, file, log, disk
        str = '%s %s %s' % (time.time(), socket.gethostname(), args.path)
        args.id = hashlib.sha1(str).hexdigest()
        self.config = OODict(args)

    def flush(self):
        self.config_manager.save(self.config, self.config_file)

        
