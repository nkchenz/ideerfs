
from util import *
from dev import *
import time
import hashlib

from obj import Object

class MetaService:
    """
    object id: sha1 of path, everything is object, include attrs, metas, dir, file
    
    all object except data chunk object are stored on meta dev. There are only 
    one metadev in the whole system.
    
    """

    def __init__(self, path):
        self.storage_pool = None
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
    
    def _id2path(self, id):
        """Map object id number to storage path, this can be changed to other methods"""
        hash = hashlib.sha1(str(id)).hexdigest()
        return os.path.join(hash[:3], hash[3:6], hash[6:])

    def _get_object(self, id):
        return self.dev.config_manager.load(os.path.join(self.meta_dir, self._id2path(id)))

    def _save_object(self, obj):
        self.dev.config_manager.save(obj, os.path.join(self.meta_dir, self._id2path(obj.id)))

    def _isdir(self, obj):
        return obj.meta.type == 'dir'

    def _lookup(self, file):
        """Find a absolute path"""
        if file == '/':
            return self._get_object(self.root_id)
        names = file.split('/')
        names.pop(0) # Remove
        parent_id = self.root_id
        for name in names:
            parent = self._get_object(parent_id)
            if not parent or not self._isdir(parent):
                return None
            if name not in parent.children:
                return None
            id = parent.children[name]
            parent = id
        return self._get_object(id)

    
    def create(self, req):
        # Create files
        pass
        
    def exists(self, req):
        response = OODict()
        if not self._lookup(req.file):
            response.error = 'no such file or directory'
        return response

    def get(self, req):
        response = OODict()
        obj = self._lookup(req.file)
        if not obj:
            response.error = 'no such file or directory'
            return response
        response.meta = obj.meta
        return response

    def lsdir(self, req): 
        response = OODict()
        obj = self._lookup(req.dir)
        if obj is None:
            response.error = 'no such directory'
            return response
        if not self._isdir(obj):
            response.error = 'not a directory'
        # This might be very large!
        response.children = obj.children.keys()
        return response    


    def create(self, req):
        # Check args: file, type, attr !fixme
        response = OODict()
        file = os.path.normpath(req.file)
    
        # Valid attrs names here
        if 'attr' not in req:
            req.attr = {}
        if req.type not in ['dir', 'file']:
            response.error = 'wrong type'
            return response

        parent_name = os.path.dirname(file)
        myname = os.path.basename(file)
        
        if not myname:
            response.error = 'root exists'
            return response
        
        # Make sure parent exists and is a dir
        parent = self._lookup(parent_name)
        if parent is None:
            response.error = 'no such directory: %s' % parent_name
            return response
        if not self._isdir(parent):
            response.error = 'not a directory: %s' % parent_name
            return response
        if myname in parent.children:
            response.error = 'file exists'
            return response
 
        id = self._next_seq()
        if not id:
            response.error = 'next seq error'
            return response
        
        new_file = Object(myname, id, parent.id, req.type, req.attr)
        parent.children[myname] = id
        
        # A journal-log should be created in case failure between these two ops
        self._save_object(parent)
        self._save_object(new_file)

        response.id = id
        return response
