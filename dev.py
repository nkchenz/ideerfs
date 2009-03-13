"""
Interface for disk device
"""

import os
import sys
import socket
import time
import hashlib

from util import *
from io import *

class Dev:
    def __init__(self, path):
        self.path = path
        self._driver = FileDB(path)
        self._config_file = 'config'
        self.config = self._driver.load(self._config_file) # Load config file if exists
    
    def flush(self):
        self._driver.store(self.config, self._config_file)

    def format(self, args):
        """Format new device, generate config file for it"""
        if not os.path.exists(self.path):
            return False
        
        # Already formatted 
        if self.config:
            return False

        # Check size and type
        size = size2int(args.size)
        if size is None:
            return False
        args.size = size
        # raid, mirror, file, log, disk
        types = ['meta', 'chunk']
        if args.type not in types:
            return False

        args.used = 0
        args.status = ''
        args.mode = ''
        # It's difficult to get a same id again
        str = '%s %s %s' % (time.time(), socket.gethostname(), args.path)
        args.id = hashlib.sha1(str).hexdigest()
        self.config = args

        self.flush()
        return True
        
    store = self._driver.store
    load = self._driver.load

