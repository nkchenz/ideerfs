"""Interface for disk device """

import os
import sys
from socket import gethostname
import time
import hashlib

from util import *
from io import *

class Dev:
    """Device class, auto read 'config' under the root directry"""
    
    def __init__(self, path):
        self._path = path
        self._db = FileDB(path)
        self._config_file = 'config'
        self.config = self._db.load(self._config_file) # Load config file if exists
        
        self.store = self._db.store
        self.load = self._db.load
        
    def flush(self):
        self._db.store(self.config, self._config_file)

    def format(self, args):
        """Format new device, generate config file for it"""
        if not os.path.exists(self._path):
            raise IOError('not exists')
        # Already formatted 
        if self.config:
            raise IOError('already formatted')

        # Check size and type
        size = size2byte(args.size)
        if size is None:
            raise IOError('wrong size ')
        args.size = size
        # raid, mirror, file, log, disk
        types = ['meta', 'chunk']
        if args.type not in types:
            raise IOError('wrong type ')

        args.used = 0
        args.status = 'offline'
        args.mode = ''
        # It's difficult to get the same id again
        str = '%s %s %s' % (time.time(), gethostname(), args.path)
        args.id = hashlib.sha1(str).hexdigest()
        self.config = args

        self.flush()
        return True
        
