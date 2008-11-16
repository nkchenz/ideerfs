#!/usr/bin/python
# coding: utf8

from util import *
from dev import *
import time

from obj import Object

class MetaService(Service):
    """
    object id: sha1 of path, everything is object, include attrs, metas, dir, file
    
    all object except data chunk object are stored on meta dev. There are only 
    one metadev in the whole system.
    
    """

    def __init__(self, path):
        self.dev = Dev(path)
        if not self.dev.config:
            print 'not formatted', path
            sys.exit(-1) # Fatal error
        
        self.seq_file = 'seq'
        self.meta_dir = 'META'
        self.root_id_file = 'root_id'
        self.root_id = self.dev.config_manager.load(self.root_id_file)
        if self.root_id is None or not self._lookup('/'):
            print 'root object not found'
            sys.exit(-1)
            

    def _next_seq(self):
        # We should get a multi thread lock here to protect 'SEQ' file
        seq = self.dev.config_manager.load(self.seq_file)
        if seq is None:
            return None
        seq += 1
        self.dev.config_manager.save(seq, self.seq_file)
        # Make sure successfully saved
        #return hashlib.sha1.hexdigest(str(seq))
        return seq
    


    def _get_object(self, id):
        return self.dev.config_manager.load(os.path.join(self.meta_dir, self._id2path(id)))

    def _save_object(self, obj):
        # Check if object already exists?
        self.dev.config_manager.save(obj, os.path.join(self.meta_dir, self._id2path(obj.id)))

    def _isdir(self, obj):
        return obj.meta.type == 'dir'

    def _lookup(self, file):
        """Find a absolute path"""
        if file == '/':
            return self._get_object(self.root_id)
        names = file.split('/')
        names.pop(0) # Remove
        debug('lookup ' + file)
        debug(names)
        debug('/', self.root_id)
        parent_id = self.root_id
        for name in names:
            parent = self._get_object(parent_id)
            if not parent or not self._isdir(parent):
                return None
            if name not in parent.children:
                return None
            id = parent.children[name]
            debug(name, id)
            parent_id = id
        return self._get_object(id)
        
    def exists(self, req):
        if self._lookup(req.file):
            return True
        else:
            return False

    def get(self, req):
        obj = self._lookup(req.file)
        if not obj:
            self._error('no such file or directory')
        obj.meta.id = obj.id # Object id returned for convenience
        return obj.meta

    def set(self, req):
        obj = self._lookup(req.file)
        if not obj:
            self._error('no such file or directory')
        for k,v in req.attrs.items():
            # You may want to validiate attr k here
            if k == 'chunks':
                for chunk_id, info in v.items():
                    obj.chunks[chunk_id] = info
                continue
            obj.meta[k] = v
        obj.meta.mtime = '%d' % time.time()
        self._save_object(obj)
        return 'ok'

    def lsdir(self, req): 
        obj = self._lookup(req.dir)
        if obj is None:
            self._error('no such directory')
        if not self._isdir(obj):
            self._error('not a directory')
        # This might be very large!
        return obj.children.keys()
    
    def test_payload(self, req):
        return 1, 'This is payload test'

    def create(self, req):
        # Check args: file, type, attr !fixme
        file = os.path.normpath(req.file)
    
        # Valid attrs names here
        if 'attr' not in req:
            req.attr = {}
        if req.type not in ['dir', 'file']:
            self._error('wrong type')

        parent_name = os.path.dirname(file)
        myname = os.path.basename(file)
        
        # Try to create root
        if not myname:
            self._error('root exists')
        
        # Make sure parent exists and is a dir
        parent = self._lookup(parent_name)
        if parent is None:
            self._error('no such directory: %s' % parent_name)
        if not self._isdir(parent):
            self._error('not a directory: %s' % parent_name)
        if myname in parent.children:
            self._error('file exists')
         
        id = self._next_seq()
        if not id:
            self._error('next seq error')
    
        new_file = Object(myname, id, parent.id, req.type, req.attr)
        parent.children[myname] = id
        
        # A journal-log should be created in case failure between these two ops
        self._save_object(parent)
        self._save_object(new_file)
        
        return id

    def get_chunk_info(self, req):
        """
        Get info of chunk_id. If chunk does not exist, it means that it's hole 
        in file or out of range, just return None, let upper layer handle it.
        """
        f = self._lookup(req.file)
        if f is None or self._isdir(f):
            self._error('no such file or is a directory' % req.file)
        
        if req.chunk_id not in f.chunks:
            return None
        return f.chunks[req.chunk_id]
    