"""IO layer"""

import os
from pprint import pformat
import cPickle
import gzip

from oodict import OODict


class FileDB:
    """File store layer"""
    
    def __init__(self, root):
        self._root = root
    
    def load(self, file, default = None, compress = False):
        """Load value in file, auto convert dict to OODict, if None
       return default value"""
        file = self.getpath(file)
        if not os.path.exists(file):
            return default
        if not compress:
            fp = open(file, 'rb')
        else:
            fp = gzip.open(file, 'rb')
        result = cPickle.load(fp)
        fp.close()
        if result is None:
            return default
        if isinstance(result, dict):
            return OODict(result) # Auto convert dict to OODict
        else:
            return result
    
    def store(self, value, file, compress = False):
        """Store value to file, please check first, make sure you want overwrite """
        file = self.getpath(file)
        p = os.path.dirname(file)
        if not os.path.exists(p):
            os.makedirs(p)
        if not compress:
            fp = open(file, 'w+')
        else:
            fp = gzip.open(file, 'wb')
        cPickle.dump(value, fp)
        fp.close()
    
    def remove(self, file):
        """Remove file"""
        file = self.getpath(file)
        os.remove(file)
        try:
            # Remove empty dirs as more as possible
            os.removedirs(os.path.dirname(file))
        except:
            pass

    def append(self, value, file):
        file = self.getpath(file)
        fp = open(file, 'a')
        fp.write(value)
        fp.close()

    def link(self, src, dest, overwrite = False):
        sf = self.getpath(src)
        df = self.getpath(dest)
        if overwrite and os.path.exists(df):
            os.unlink(df)
        os.symlink(sf, df)

    def getpath(self, file):
        return os.path.join(self._root, file)
