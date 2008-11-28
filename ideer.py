#!/usr/bin/python
# coding: utf8

import os
import sys
import pprint
import hashlib

from nlp import NLParser
from oodict import OODict
from fs import FileSystem

from util import *
from nio import *
from dev import *

from nio_server import *
        
from obj import Object


class StorageAdmin:
    """
    Request local chunk server, which is just a proxy agent of the global storage
    manager,  to do the storage management for local node. 
    """
    
    def __init__(self):
        # Usage format, symbols beginning with '$' are vars which you can use directly
        # in the 'args' parameter
        self.usage_rules = {
            'format $path size $size for $data_type data': 'format',
            'online $path': 'online',
            'offline $path': 'offline',
            'frozen $path': 'frozen',
            'remove $path': 'remove',
            'replace $old_path with $new_path': 'replace',
            'stat $path': 'stat',
        }

    def _pre_command(self, cmd, args):
        # Hook for pre cmd running
        if cmd not in ['format']: # No need to connect storage service while formatting
            self._connect_chunk_service()

    def _connect_chunk_service(self):
        self.cm = ConfigManager(os.path.expanduser('~/.ideerfs/'))
        self.services = self.cm.load('services', OODict())
        
        if 'chunk' not in self.services:
            print 'where is local chunk service?'
            sys.exit(-1)

        self.nio = NetWorkIO(self.services.chunk)
        

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
        dev.flush()
        
        # Create necessary files for meta device
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
        self.nio.call('chunk.admin_dev', action = 'online', path = args.path)
    
    def offline(self, args):
        self.nio.call('chunk.admin_dev', action = 'offline', path = args.path)
        
    def frozen(self, args):
        self.nio.call('chunk.admin_dev', action = 'frozen', path = args.path)
    
    def remove(self, args):
        pass

    def replace(self, args):
        self.nio.call('chunk.admin_dev', action = 'replace', \
            old_path = args.old_path, new_path = args.new_path)
        
        # First transfer data to other devs automatically, then remove
        
    def stat(self, args):
        """
        Show storage status
        
        stat all       status of the global pool
        stat local     status of local devs
        stat /data/sda status of a local dev
        
        # total_disks, invalid_disks
        """
        cache = self.nio.call('chunk.admin_dev', action = 'stat', path = args.path)
        for id, dev in cache.items():
            dev = OODict(dev)
            print '%s %s/%s %d%% %s %s' %(dev.path, byte2size(dev.used), byte2size(dev.size), \
                dev.used * 100 / dev.size, dev.status, dev.mode)


class FSShell:
    def __init__(self):
        # Is there a easy way to export envs still exists after this program exit
        # os.environ did not work
        self.cm = ConfigManager(os.path.expanduser('~/.ideerfs/'))
        
        self.usage_rules = {
            'lsdir': 'lsdir', 
            'lsdir $dir': 'lsdir',
            'mkdir $dirs': 'mkdir',
            'exists $file': 'exists',
            'get_file_meta $file': 'get_file_meta', 
            'get_chunk_info $file $chunk_id': 'get_chunk_info',
            'get $attrs of $file': 'get_file_meta', 
            'set $attrs of $file to $values': 'set_file_attr',
            'rm $files': 'rm',
            'mv $old_file $new_file': 'mv',
            'store $localfile $file': 'store', # cp from local
            'restore $file $localfile': 'restore', # cp to local
            #'cp src $src dest $dest': 'cp',
            'cp $src $dest': 'cp',
            'stat': 'stat',
            'touch $files': 'touch', 
            'cd $dir': 'cd',
            'pwd': 'pwd'
            }
        
        self.services = self.cm.load('services', OODict())
        if 'meta' not in self.services:
            print 'where is meta service?'
            sys.exit(-1)
        if 'storage' not in self.services:
            print 'where is storage service?'
            sys.exit(-1)   
        self.fs = FileSystem(self.services.meta, self.services.storage)            
            
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
    
    def get_file_meta(self, args):
        file = self._normpath(args.file)
        print self.fs.get_file_meta(file)
        
    def get_chunk_info(self, args):
        file = self._normpath(args.file)
        meta = self.fs.get_file_meta(file)
        info = self.fs.get_chunk_info(file, int(args.chunk_id))
        if not info:
            print 'no such chunk'
            return
        print chunk_path(meta.id, args.chunk_id, info.version)
        for k, v in info.locations.items():
            print '%s@%s' % (v['path'], v['host']) 
        
    def store(self, args):
        """Store local file to the fs"""
        src = args.localfile
        if not os.path.exists(src):
            print src, 'not exists'
            return
        # Read local file
        data = open(src, 'rb').read()
        dest = self._normpath(args.file)
        # Create new file and write
        self.fs.create(dest, replication_factor = 3, chunk_size = 67108864)
        f = self.fs.open(dest)
        f.write(0, data)
        
    def restore(self, args):
        """Restore file in the fs to local filesystem"""
        file = self._normpath(args.file)
        meta = self.fs.get_file_meta(file)
        f = self.fs.open(file)
        data = f.read(0, meta.size)
        open(args.localfile, 'w').write(data)
        
            
    def cp(self, args):
        src = self._normpath(args.src)
        dest = self._normpath(args.dest)
        meta = self.fs.get_file_meta(src)
        f = self.fs.open(src)
        data = f.read(0, meta.size)
        
        self.fs.create(dest, replication_factor = 3, chunk_size = 67108864)
        f = self.fs.open(dest)
        f.write(0, data)
        

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
            print ' '.join(sorted(files))

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
            self.fs.create(file, replication_factor = 3, chunk_size = 2 ** 25) #32m
        
    def rm(self, args):
        files = args.files.split()
        recursive = False
        if files[0] in ['-r', '-R']:
            recursive = True
            files.pop(0)
        for file in files:
            file = self._normpath(file)
            self.fs.delete(file, recursive)
            
    def mv(self, args):
        old_file = self._normpath(args.old_file)
        new_file = self._normpath(args.new_file)
        self.fs.mv(old_file, new_file)

class JobController:
    def __init__(self):
        self.usage_rules = {}
    #-------------------------------MapReduce-------------------------------
    def map(self):
        pass


class ServiceController:
    """
    ideer service use /data/sda as meta device
    ideer service start meta at localhost:1984 
    
    # Storage service needs to know where is meta service
    ideer service meta at localhost:1984
    ideer service start storage at localhost:1984
    
    # Chunk service needs to know where is storage service
    ideer service storage at localhost:1984
    ideer service start chunk at localhost:1984
    
    # Start all services on only one node
    ideer service use /data/sda as meta device
    ideer service start meta, storage, chunk at localhost:1984
    """
    def __init__(self):
        self.usage_rules = {
        #'start $services at $ip : $port with $dev as meta device': 'start_meta',
        'use $dev as meta device': 'use',
        '$service at $host : $port': 'at',
        'start $services at $host : $port': 'start',
        }
        
        self.cm = ConfigManager(os.path.expanduser('~/.ideerfs/'))
        self.services = self.cm.load('services', OODict())
    
    def use(self, args):
        self.cm.save(args.dev, 'meta_dev')

    def at(self, args):
        s = args.service
        if s not in ['meta', 'storage', 'chunk']:
            print 'unknown service', s
            return -1
        
        if not args.port.isdigit():
            print 'int port number required '
            return -1
        args.port = int(args.port)
        
        del args['service']
        self.services[s] = (args.host, args.port)
        self.cm.save(self.services, 'services')
        
    def start(self, args):
        server = NIOServer()
        # Remove dup and spaces
        ss = list(set([s.strip() for s in args.services.split(',')]))
        unknown = filter(lambda s: s not in ['meta', 'storage', 'chunk'], ss)
        if unknown:
            print 'unknown services', unknown
            return -1
        
        if not args.port.isdigit():
            print 'int port number required '
            return -1
        addr = (args.host, int(args.port))
        
        # Start meta service
        if 'meta' in ss:
            path = self.cm.load('meta_dev')
            if not path:
                print 'please set meta device first'
                return -1
            server.register('meta', MetaService(path))
            self.services.meta = addr

        if 'storage' in ss:
            if 'meta' not in self.services:
                print 'where is meta service?'
                return -1 
            server.register('storage', StorageService(self.services.meta))
            self.services.storage = addr
        
        if 'chunk' in ss:
            if 'storage' not in self.services:
                print 'where is storage service?'
                return -1  
            server.register('chunk', ChunkService(self.services.storage))
        
        server.bind(addr)
        debug('start server ok')
        server.mainloop()


command_sets = {
'storage': 'StorageAdmin',
'fs': 'FSShell',
'job': 'JobController',
'service': 'ServiceController',
}

if len(sys.argv) <= 1 or sys.argv[1] == 'help':
    print 'usage:', sys.argv[0], '|'.join(command_sets), 'action'
    sys.exit(-1)

cmd_class = sys.argv[1]
if cmd_class not in command_sets:
    print 'unknown cmd class', cmd_class
    sys.exit(-1)
    
# Set dispatcher and rules for nlp
dispatcher = globals()[command_sets[cmd_class]]() # Only get the needed hanlder
nlp = NLParser(dispatcher)
input = ' '.join(sys.argv[2:])
try:
    nlp.parse(input)
except IOError, err:
    print err.message
