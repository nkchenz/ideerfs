import sys
from oodict import OODict
from util import *

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
        args.status = 'offline'
        #args.data_type = 'chunk' #chunk, meta
        args.type = 'file'  #raid, mirror, file, log, disk
        str = '%s %s %s' % (time.time(), args.host, args.path)
        args.id = hashlib.sha1(str).hexdigest()
        self.config = OODict(args)
    
    def assert_status(self, status):
        if not self.config:
            print 'not formatted'
            sys.exit(-1)
        
        if self.config.status not in status:
            print 'status not in', status
            sys.exit(-1)
    
    def change_status(self, status):
        self.config.status = status
        self.config_manager.save(self.config, self.config_file)