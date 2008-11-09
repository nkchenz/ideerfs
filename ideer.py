#!/usr/bin/python
# coding: utf8

import os
import sys
from nlp import NLParser
from oodict import OODict

from util import *
from nio import *
from dev import *

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
        
        types = ['meta', 'chunk']
        if args.data_type not in types:
            print 'wrong data_type, only support', types 
            return False

        dev = Dev(args.path)
        if dev.config:
            print 'already formatted?'
            sys.exit(-1)
            
        dev.init(args)
        dev.change_status('offline')
        
        if args.data_type == 'meta':
            dev.config_manager.save(0, 'seq')
            os.mkdir(os.path.join(args.path, 'META'))
        else:
            os.mkdir(os.path.join(args.path, 'OBJECTS'))
        print 'format ok'

    
    def online(self, args):
        dev = Dev(args.path)
        
        if dev.config.data_type != 'chunk':
            print 'wrong data_type'
            sys.exit(-1)

        dev.assert_status(['offline'])
        req = OODict()
        req.method = 'storage.online'
        req.dev =  dev.config
        result = self.nio_storage.request(req)
        self.assert_error(result)
        dev.change_status('online')
        print 'online ok'
    
    def offline(self, args):
        dev = Dev(args.path)
        dev.assert_status(['online', 'frozen'])
        req = OODict()
        req.method = 'storage.offline'
        req.dev_id =  dev.config.id
        result = self.nio_storage.request(req)
        self.assert_error(result)
        dev.change_status('offline')
        print 'offline ok'
        
    def frozen(self, args):
        dev = Dev(args.path)
        dev.assert_status(['online'])
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
        print result.meta_dev
        #print 'Total_disks, invalid_disks'
        #print 'Pool Capacity:'
        #print 'Used:'
        
    #-------------------------------FS---------------------------------------
    def ls(self):
        pass
        

    #-------------------------------MapReduce-------------------------------
    def map(self):
        pass

# Usage format, symbols beginning with '$' are vars which you can use directly
# in the 'args' parameter of method of class CommandSet
nlparser = NLParser()
nlparser.rules = {
'format': 'storage format $path size $size host $host for $data_type data',
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