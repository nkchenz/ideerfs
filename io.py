"""IO layer"""

import os

class FileDB:
    """File store layer"""
    
    def __init__(self, root):
        self._root = root
    
    def load(self, file, default = None):
        """Load value in file, auto convert dict to OODict, if None
       return default value"""
        file = os.path.join(self._root, file)
        if not os.path.exists(file):
            return default
        content = open(f, 'r').read()
        if not content: # Empty file
            return default
        result = eval(content)
        if result is None:
            return default
        if isinstance(result, dict):
            return OODict(result) # Auto convert dict to OODict
        else:
            return result
    
    def store(self, value, file):
        """Store value to file, please check first, make sure you want overwrite """
        file = os.path.join(self._root, file)
        p = os.path.dirname(file)
        if not os.path.exists(p):
            os.makedirs(p)
        fp = open(file, 'w+')
        fp.write(pformat(value))
        fp.close()
    
    def remove(self, file):
        """Remove file"""
        file = os.path.join(self._root, file)
        os.remove(file)
        try:
            # Remove empty dirs as more as possible
            os.removedirs(os.path.dirname(file))
        except:
            pass

    def append(self, value, file):
        file = os.path.join(self._root, file)
        fp = open(file, 'a')
        fp.write(value)
        fp.close()
