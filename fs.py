"""
FS interface

refs:
http://hadoop.apache.org/core/docs/current/api/org/apache/hadoop/fs/FileSystem.html
http://hadoop.apache.org/core/docs/current/api/org/apache/hadoop/dfs/DistributedFileSystem.html
http://hadoop.apache.org/core/docs/current/api/org/apache/hadoop/fs/kfs/KosmosFileSystem.html
"""
from nio import *

class FileSystem:
    
    def __init__(self):
        self.nio_meta = NetWorkIO('localhost', 1984)

    def _create(self, file, type, attr):
        req = OODict()
        req.method = 'meta.create'
        req.file =  file
        req.type = type
        req.attr = attr
        result = self.nio_meta.request(req)
        if 'error' in result:
            print result.error
            return False
        else:
            return True


    def create(self, file, **attr):
        """Create new files with attrs: replication factor, bs, permission
        foo.create('/kernel/sched.c', replication_factor = 3, chunk_size = '64m')
        """
        return self._create(file, 'file', attr)

    def delete(file, recursive = False):
        pass
    
    def exists(self, file):
        req = OODict()
        req.method = 'meta.exists'
        req.file =  file
        if 'error' in self.nio_meta.request(req):
            return False
        else:
            return True

    def get_chunk_locations(file, start, len):
        pass
        
    def get_file_meta(self, file):
        #checksum, 
        req = OODict()
        req.method = 'meta.get'
        req.file =  file
        result = self.nio_meta.request(req)
        if 'error' in result:
            print result.error
            return None
        else:
            return result.meta
        
    def lsdir(self, dir):
        """list dir, return [] if not exists or not a dir"""
        if not dir:
            return []
        req = OODict()
        req.method = 'meta.lsdir'
        req.dir =  dir
        result = self.nio_meta.request(req)
        if 'error' in result:
            print result.error
            return []
        else:
            return result.children

    def mkdir(self, dir):
        return self._create(dir, 'dir', {})
            
    def mv():
        # Rename
        pass
        
    def open():
        """"
        'w+' write, truncate first
        'rw' read write
        'r' readonly mode
        'a' append mode
        """
        #get_write_lock(file)
        pass

    def close():
        # No more fs operations, flush fs cache
        pass
    
    def stat():
        # Default stripe_size, rf, 
        pass

    '''
    def copy_from_local_file(src, dst, overwrite?):
        pass
    
    def copy_to_local_file(src, dst):
        pass
        
    def copy(src, dst):
        # Need to check whether they are in fs or local
        pass

    def append(self, file, data, progress):
        # It's better if there is a way to get 
        pass
    '''



class FileMeta:
    def get(self):
        # Get attrs of file
        # len, atime, mtime, group, dataset, replication factor, owner, etc.
        # pathname hash
        pass
        
    def set(self):
        # Set attrs of file
        pass


class FileStream:
    def __init__():
        self.fp = 0
        
    def tell():
        pass
        
    def seek():
        pass
        
    def read():
        pass
    
    def write():
        pass

if __name__ == '__main__':

    foo = FileSystem()
    foo.exists('/')
    foo.exists('/c')
    foo.mkdir('/')
    foo.exists('/')

    foo.mkdir('/a')
    foo.mkdir('/b')
    foo.lsdir('/')