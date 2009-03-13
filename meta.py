"""
Meta service

meta should be kept in memory using a special data structure such as a B+
tree, with journal on persistent storage, and periodicly flush and checkpoint.

Big data chunks are stored to disks directly.

"""

import time

from util import *
from nio import *
from obj import *
from service import *
import config


class MetaService(Service):
    """
    All object except data chunk object are stored on meta dev. There are only 
    one metadev in the whole system.


    create root if needed, object layer know nothing about root
    """

    def __init__(self):
        self._object_shard =  ObjectShard()
        
    def _isdir(self, obj):
        return obj.type == 'dir'

    def _lookup(self, file):
        """Find a absolute path"""
        root_id = 1
        if file == '/':
            return self._object_shard.load_object(root_id)
        names = file.split('/')
        names.pop(0) # Remove
        debug('lookup ' + file)
        debug(names)
        parent_id = root_id
        for name in names:
            parent = self._object_shard.load_object(parent_id)
            if not parent or not self._isdir(parent):
                return None
            if name not in parent.children:
                return None
            id = parent.children[name]
            debug(name, id)
            parent_id = id
        return self._object_shard.load_object(id)
        
    def exists(self, req):
        """Check if a file exists"""
        if self._lookup(req.file):
            return True
        else:
            return False

    def get(self, req):
        """
        Get 'meta' attribute of a file
        
        object id is returned too for convenience
        """
        obj = self._lookup(req.file)
        if not obj:
            self._error('no such file or directory')
        obj.meta.id = obj.id
        obj.meta.type = obj.type
        return obj.meta

    def set(self, req):
        """
        Set file attributes in 'meta'.
        
        'chunks' of a file is kinda meta too, just like 'children' of a dir, we
        do not put them in 'meta' because they are usually too large. Shall we?
        """
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
        self._object_shard.store_object(obj)
        return 'ok'

    def lsdir(self, req): 
        """Get all the children names fo a dir
        This should be splitted for large dirs.
        """
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
        """Create a file with given attributes"""
        # Check args: file, type, attr !fixme
        
        # This should be performed to all 'file' arguments
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
         
        id = self._object_shard.create_object()
        if not id:
            self._error('next seq error')
    
        new_file = Object(myname, id, parent.id, req.type, req.attr)
        parent.children[myname] = id
        
        # A journal-log should be created in case failure between these two ops
        self._object_shard.store_object(parent)
        self._object_shard.store_object(new_file)
        
        return id

    def get_chunks(self, req):
        """
        Return infos of chunks in req.chunks, such as version. Need to lookup
        for storage manager to get the location infos. Should we store them
        with meta data? 
        """
        f = self._lookup(req.file)
        if f is None or self._isdir(f):
            self._error('no such file or is a directory' % req.file)
        
        data = OODict()
        data.id = f.id
        data.chunks = {}
        for chunk in req.chunks:
            if chunk in f.chunks:
                data.chunks[chunk] = f.chunks[chunk]
        return data
    
    def _delete_recursive(self, obj):
        deleted = {}
        for name, id in obj.children.items():
            if name in ['.', '..']:
                continue
            child = self._object_shard.load_object(id)
            if not child:
                # Object already missing, so dont bother to delete
                pass
            if child.type == 'dir':
                tmp = self._delete_recursive(child)
            if child.type == 'file':
                tmp = self._delete_file(child)
            deleted.update(tmp)
        # Delete self
        self._delete_object(obj.id)
        return deleted
    
        
    def _delete_file(self, obj):
        """Delete file type object"""
        self._object_shard.delete_object(obj.id)
        if obj.chunks:
            return {obj.id: obj.chunks}
        else:
            return {}

    def delete(self, req):
        """Delete file, store the free chunks to file 'deleted_chunks', tell
        it to storage manager in next hb message
    
        There shall be a .trash dir to store it first, auto delete 30 days later
        """
        req.file = os.path.normpath(req.file)
        
        if req.file == '/':
            self._error('attempt to delete root')
    
        parent = self._lookup(os.path.dirname(req.file))
        name = os.path.basename(req.file)
        if not parent or name not in parent.children:
            self._error('no such file or directory')

        obj = self.object_shard.load_object(parent.children[name])
        if obj:
            if obj.type == 'dir':
                if len(obj.children) > 2:
                    if not req.recursive:
                        self._error('dir not empty')
                # Delete dir recursively
                deleted = self._delete_recursive(obj)
        
             #Delete file
            if obj.type == 'file':
                deleted = self._delete_file(obj)
        else:
            # Shall we return error?
            #self._error('file object missing')
            pass
        # Delete entry in parent
        del parent.children[name]
        self._object_shard.store_object(parent)

        # Free chunks to storage, this should be done in another thread
        nio = NetWorkIO(config.storage_server_address)
        nio.call('storage.free', deleted = deleted)
        nio.close()

        return 'ok'
    
    
    def rename(self, req):
        """Rename oldfile to newfile
        oldfile and parent of newfile must exist
        """
        req.old_file = os.path.normpath(req.old_file)
        req.new_file = os.path.normpath(req.new_file)
        
        if req.old_file == req.new_file:
            self._error('same name')
        
        if req.old_file == '/' or req.new_file == '/':
            self._error('attempt to rename root')

        old_parent_name = os.path.dirname(req.old_file)
        old_parent = self._lookup(old_parent_name)
        old_name = os.path.basename(req.old_file)
        if not old_parent or old_name not in old_parent.children:
            self._error('old file not exists')
        
        new_parent_name = os.path.dirname(req.new_file)
        if old_parent_name == new_parent_name:
            new_parent = old_parent
        else:
            new_parent = self._lookup(new_parent_name)
            if not new_parent or new_parent.type != 'dir':
                self._error('no such directory ' + new_parent_name) 
        new_name = os.path.basename(req.new_file)
        if new_name in new_parent.children:
            self._error('new file exists') 
        
        # Let's begin
        new_parent.children[new_name] = old_parent.children[old_name]
        del old_parent.children[old_name]
        
        self._object_shard.store_object(old_parent)
        if old_parent_name != new_parent_name:
            self._object_shard.store_object(new_parent)
        return 'ok'

