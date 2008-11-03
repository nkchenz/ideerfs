#!/usr/bin/python
# coding: utf8

import os
import sys
from nlp import NLParser
from oodict import OODict

import time
import hashlib
from pprint import pformat

class ConfigManager:
    def __init__(self, path):
        """Set root directory of configs"""
        self.root = os.path.join(path, '.ideerfs')
    
    def load(self, file):
        f = os.path.join(self.root, file)
        if os.path.exists(f):
            return eval(open(f, 'r').read())
        else:
            return None
    
    def save(self, config, file):
        # Please check first, make sure you want overwrite 
        f = os.path.join(self.root, file)
        d = os.path.dirname(f)
        if not os.path.exists(d):
            os.makedirs(d)
        open(f, 'w+').write(pformat(config))


class Dev:
    def __init__(self, path = None):
        if path:
            self.config_manager = ConfigManager(path)
            self.config_file = 'config'
            self.config = self.config_manager.load(self.config_file)
    
    def init(self, args):
        self.config = OODict(args)
        #self.path = ''
        #self.host = ''
        #self.capacity = ''
        self.config.used = 0
        self.config.data_type = 'chunk' #chunk, meta
        self.config.dev_type = 'file'  #raid, mirror, file, log, disk
        self.config.id = self._gen_id()
        
    def _gen_id(self):
        str = '%s %s %s' % (time.time(), self.config.host, self.config.path)
        print str
        return hashlib.sha1(str).hexdigest()
        

class CommandSet:
    """
    1. format sda and online it
    2. replace sda with sdb
    3. offline sda # disable, data not available
    4. online sda  # enable
    5. frozen sda # readonly
    6. remove sda  # replicate data first, then offline
    """
    
    #----------------------------Storage----------------------------------------
    def format(self, args):
        # Format new device
        if not os.path.exists(args.path):
            print args.path, 'not exists'
            return False
            
        dev = Dev(args.path)
        if dev.config:
            print dev.config
            print 'Old config found, already formatted?', args.path
            sys.exit(-1)
            
        dev.init(args)
        dev.config_manager.save(dev.config, dev.config_file)
        print dev.config
        print 'Format %s OK' % args.path

    
    def online(self, args):
        print 'Online', args.dev
    
    def offline(self, args):
        print 'Offline', args.dev
        pass
        
    def frozen(self, args):
        print 'Frozen', args.dev
        # No more writes on dev
        pass
    
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
        print 'Total_disks, invalid_disks'
        print 'Pool Capacity:'
        print 'Used:'
        
    #-------------------------------FS---------------------------------------
    def ls(self):
        pass
        

    #-------------------------------MapReduce-------------------------------
    def map(self):
        pass


nlparser = NLParser()
nlparser.rules = {
'format': 'storage format $path size $capacity host $host',
'online': 'storage online $dev',
'offline': 'storage offline $dev',
'frozen': 'storage frozen $dev',
'remove': 'storage remove $dev',
'replace': 'storage replace $old_dev with $new_dev',
'stat': 'storage stat '
}


#commands_sets = {'storage', 'fs', 'job'}

nlparser.dispatcher = CommandSet()
input = ' '.join(sys.argv[1:])
nlparser.parse(input)