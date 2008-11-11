#!/usr/bin/python
# coding: utf8

import os
import sys
from nlp import NLParser
from oodict import OODict
from fs import FileSystem

from util import *
from nio import *
from dev import *

from obj import Object

def assert_error(result):
    if 'error' in result:
        print result.error
        sys.exit(-1)

class StorageAdmin:
    """
    1. format sda and online it
    2. replace sda with sdb
    3. offline sda # disable, data not available
    4. online sda  # enable
    5. frozen sda # readonly
    6. remove sda  # replicate data first, then offline
    """
    
    def __init__(self):
        self.storage_manager = 'localhost'
        self.nio_storage = NetWorkIO(self.storage_manager, 1984)
        
        # Usage format, symbols beginning with '$' are vars which you can use directly
        # in the 'args' parameter
        self.usage_rules = {
            'format $path size $size host $host for $data_type data': 'format',
            'online $path': 'online',
            'offline $path': 'offline',
            'frozen $path': 'frozen',
            'remove $path': 'remove',
            'replace $old_path with $new_path': 'replace',
            'stat': 'stat ',
        }

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
            root_id = 1
            dev.config_manager.save(root_id, 'seq')
            dev.config_manager.save(root_id, 'root_id')
            dev.config_manager.save(Object('/', root_id, root_id, 'dir'), \
                'META/356/a19/2b7913b04c54574d18c28d46e6395428ab')
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
        assert_error(result)
        dev.change_status('online')
        print 'online ok'
    
    def offline(self, args):
        dev = Dev(args.path)
        dev.assert_status(['online', 'frozen'])
        req = OODict()
        req.method = 'storage.offline'
        req.dev_id =  dev.config.id
        result = self.nio_storage.request(req)
        assert_error(result)
        dev.change_status('offline')
        print 'offline ok'
        
    def frozen(self, args):
        dev = Dev(args.path)
        dev.assert_status(['online'])
        req = OODict()
        req.method = 'storage.frozen'
        req.dev_id =  dev.config.id
        result = self.nio_storage.request(req)
        assert_error(result)
        dev.change_status('frozen')
        print 'frozen ok'

    
    def remove(self, args):
        pass

    def replace(self, args):
        print 'Frizing ', args.old_dev
        print 'Transfering data to', args.new_dev
        print 'OK'
        # First transfer data to other devs automatically, then remove
        
    def stat(self, args):
        """
        # total_disks, invalid_disks
        """
        req = OODict()
        req.method = 'storage.stat'
        result = self.nio_storage.request(req)
        assert_error(result)
        for k, v in result.statistics.items():
            print '%s: %s' % (k, v)
            
        #print 'Total_disks, invalid_disks'
        #print 'Pool Capacity:'
        #print 'Used:'
    

class FSShell:
    def __init__(self):
        self.fs = FileSystem()
        #self.storage_manager = 'localhost'
        self.usage_rules = {
            'lsdir': 'lsdir', 
            'lsdir $dir': 'lsdir',
            'mkdir $dirs': 'mkdir',
            'exists $file': 'exists',
            'get $attrs of $file': 'get_file_meta', 
            'set $attrs of $file to $values': 'set_file_attr',
            'delete $file $mode': 'delete',  # mode is recursively
            'mv $old $new': 'mv',
            'cp src $src dest $dest': 'cp',
            'stat': 'stat',
            'touch $files': 'touch', 
            'cd $dir': 'cd',
            'pwd': 'pwd'
            }
        
        # Is there a easy way to export envs still exists after this program exit
        # os.environ did not work
        self.cm = ConfigManager(os.path.expanduser('~/.ideerfs/'))
 
    def _getpwd(self):
        return self.cm.load('pwd', '')
    
    def _normpath(self, dir):
        # Normalize path with PWD env considered
        p = os.path.join(self._getpwd(), dir)
        if not p:
            return '' # Empty pwd and dir
        # Make sure it's an absolute path, pwd is client only, path is assumed to
        # be absolute when communicating with meta node
        return os.path.normpath(os.path.join('/', p))
    
    def cd(self, args):
        dir = self._normpath(args.dir)
        meta = self.fs.get_file_meta(dir)
        if meta and meta.type == 'dir':
            self.cm.save(dir, 'pwd')
        
    def cp(self, args):
        print args
        

    def exists(self, args):
        print self.fs.exists(args.file)
        
    def lsdir(self, args):
        if 'dir' not in args: # If no dir, then list pwd dir
            args.dir = ''
        dir = self._normpath(args.dir)
        if not dir:
            return
        files = self.fs.lsdir(dir)
        if files:
            print ' '.join(files)

    def mkdir(self, args):
        # dirs can't be empty because in that case we wont get here, cant pass nlp 
        for dir in args.dirs.split():
            dir = self._normpath(dir)
            self.fs.mkdir(dir)
                
    def pwd(self, args):
        print self._getpwd()
        
    def touch(self, args):
        for file in args.files.split():
            file = self._normpath(file)
            self.fs.create(file, replication_factor = 3, chunk_size = 67108864)

class JobController:
    def __init__(self):
        self.usage_rules = {}
    #-------------------------------MapReduce-------------------------------
    def map(self):
        pass


command_sets = {
'storage': StorageAdmin(),
'fs': FSShell(),
'job': JobController()
}

if len(sys.argv) <= 1 or sys.argv[1] == 'help':
    print 'usage:', sys.argv[0], '|'.join(command_sets), 'action'
    sys.exit(-1)

cmd_class = sys.argv[1]
if cmd_class not in command_sets:
    print 'unknown cmd class', cmd_class
    sys.exit(-1)
    
# Set dispatcher and rules for nlp
nlp = NLParser(command_sets[cmd_class])
input = ' '.join(sys.argv[2:])
try:
    nlp.parse(input)
except IOError, err:
    print err.message
