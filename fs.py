"""
FS interface

refs:
http://hadoop.apache.org/core/docs/current/api/org/apache/hadoop/fs/FileSystem.html
http://hadoop.apache.org/core/docs/current/api/org/apache/hadoop/dfs/DistributedFileSystem.html
http://hadoop.apache.org/core/docs/current/api/org/apache/hadoop/fs/kfs/KosmosFileSystem.html
"""

from nio import *

class FileSystem:

    def exists(path):
        pass

    def create(file, attr):
        # Create new files with attrs: replication factor, bs, permission
        pass

    def delete(file, recursive?):
        pass
    
    def get_block_locations(file, start, len):
        pass
        
    def get_file_meta():
        #checksum, 
        pass
        
    def ls(dir):
        """path dir"""
        pass
        
    def mkdirs():
        pass
        
    def mv():
        # Rename
        pass
        
    def open():
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
    def get_blocksize():
        # Get attrs of file
        # len, atime, mtime, group, dataset, replication factor, owner, etc.
        # pathname hash
        pass
        
    def set():
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


nio = NetWorkIO('localhost', 1984)

req = OODict()
req.method = 'meta.ls'
req.file =  '/aabdf'
print nio.request(req)

req.file =  '/'
print nio.request(req)
