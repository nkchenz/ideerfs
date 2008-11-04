#!/usr/bin/python
# coding: utf8

import os
import sys
from nlp import NLParser
from oodict import OODict
from util import *
from nio import *

import time
import hashlib


class Dev:
    def __init__(self, path = None):
        if path:
            self.config_manager = ConfigManager(os.path.join(path, '.ideerfs'))
            self.config_file = 'config'
            self.config = self.config_manager.load(self.config_file, OODict())
    
    def init(self, args):
        # path, host, capacity are all in args
        args.used = 0
        args.status = 'offline'
        args.data_type = 'chunk' #chunk, meta
        args.dev_type = 'file'  #raid, mirror, file, log, disk
        str = '%s %s %s' % (time.time(), args.host, args.path)
        args.id = hashlib.sha1(str).hexdigest()
        self.config = OODict(args)
    
    def assert_status(self, status):
        if not self.config:
            print 'not formatted'
            sys.exit(-1)
        
        if self.config.status == status:
            print 'not', status
            sys.exit(-1)
    
    def change_status(self, status):
        self.config.status = status
        self.config_manager.save(self.config, self.config_file)
        
        

class CommandSet:
    """
    1. format sda and online it
    2. replace sda with sdb
    3. offline sda # disable, data not available
    4. online sda  # enable
    5. frozen sda # readonly
    6. remove sda  # replicate data first, then offline
    """
    
    def __init__(self):
        # Create a connect to storage manager
        self.storage_manager = 'localhost'
        self.nio_storage = NetWorkIO(self.storage_manager, 1984)

    def assert_error(self, result):
        if 'error' in result:
            print result.error
            sys.exit(-1)

    #----------------------------Storage----------------------------------------
    def format(self, args):
        # Format new device
        if not os.path.exists(args.path):
            print args.path, 'not exists'
            return False
        
        size = size2byte(args.size)
        if size is None:
            print 'wrong size', args.size
            return False
        args.size = size
        
        dev = Dev(args.path)
        if dev.config:
            print 'already formatted?'
            sys.exit(-1)
            
        dev.init(args)
        dev.change_status('offline')
        print 'format ok'

    
    def online(self, args):
        dev = Dev(args.path)
        dev.assert_status('offline')
        req = OODict()
        req.method = 'storage.online'
        req.dev =  dev.config
        result = self.nio_storage.request(req)
        self.assert_error(result)
        dev.change_status('online')
        print 'online ok'
    
    def offline(self, args):
        dev = Dev(args.path)
        dev.assert_status('online')
        req = OODict()
        req.method = 'storage.offline'
        req.dev_id =  dev.config.id
        result = self.nio_storage.request(req)
        self.assert_error(result)
        dev.change_status('offline')
        print 'offline ok'
        
    def frozen(self, args):
        dev = Dev(args.path)
        dev.assert_status('online')
        req = OODict()
        req.method = 'storage.frozen'
        req.dev_id =  dev.config.id
        result = self.nio_storage.request(req)
        self.assert_error(result)
        dev.change_status('frozen')
        print 'frozen ok'

    
    def remove(self, args):
        pass

    def replace(self, args):
        print 'Frizing ', args.old_dev
        print 'Transfering data to', args.new_dev
        print 'OK'
        # First transfer data to other devs automatically, then remove
        pass
        
    def stat(self, args):
        """
        # total_disks, invalid_disks
        """
        req = OODict()
        req.method = 'storage.stat'
        result = self.nio_storage.request(req)
        self.assert_error(result)
        print 'size:', byte2size(result.statistics.size)
        print 'used:', byte2size(result.statistics.used)

        #print 'Total_disks, invalid_disks'
        #print 'Pool Capacity:'
        #print 'Used:'
        
    #-------------------------------FS---------------------------------------
    def ls(self):
        pass
        

    #-------------------------------MapReduce-------------------------------
    def map(self):
        pass


nlparser = NLParser()
nlparser.rules = {
'format': 'storage format $path size $size host $host',
'online': 'storage online $path',
'offline': 'storage offline $path',
'frozen': 'storage frozen $path',
'remove': 'storage remove $path',
'replace': 'storage replace $old_path with $new_path',
'stat': 'storage stat '
}


#commands_sets = {'storage', 'fs', 'job'}

nlparser.dispatcher = CommandSet()
input = ' '.join(sys.argv[1:])
nlparser.parse(input)