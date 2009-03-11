"""
Misc utils
"""

import os
from pprint import pformat
import hashlib

from oodict import OODict

class ConfigManager:
    def __init__(self, root):
        """Set root directory of configs"""
        self.root = root
    
    def load(self, file, default = None):
        """Auto convert dict to OODict, if None return default value"""
        f = os.path.join(self.root, file)
        if os.path.exists(f):
            content = open(f, 'r').read()
            if not content: # Empty file
                return default
            result = eval(content)
            if isinstance(result, dict):
                return OODict(result) # Auto convert dict to OODict
            else:
                if result is None:
                    return default
                else:
                    return result
        else:
            return default
    
    def save(self, config, file):
        # Please check first, make sure you want overwrite 
        f = os.path.join(self.root, file)
        d = os.path.dirname(f)
        if not os.path.exists(d):
            os.makedirs(d)
        fp = open(f, 'w+')
        fp.write(pformat(config))
        fp.close()
    
    def remove(self, file):
        file = os.path.join(self.root, file)
        os.remove(file)
        try:
            # Remove empty dirs as more as possible
            os.removedirs(os.path.dirname(file))
        except:
            pass

    def append(self, data, file):
        fp = open(os.path.join(self.root, file), 'a')
        fp.write(data)
        fp.close()

# Misc functions
def size2byte(s):
	if not s:
		return None

	if s.isdigit():
		return int(s)

	size=s[:-1]
	unit=s[-1].lower()

	if not size.isdigit():
		return None
	size = int(size)

	weight ={
	'k': 1024, 'm': 1024 * 1024, 'g': 1024 ** 3,
	't': 1024 ** 4, 'p': 1024 ** 5
	}
	if unit not in weight.keys():
		return None
	return size * weight[unit]

def byte2size(n):
    units=['', 'k', 'm', 'g', 't', 'p']
    for i in range(0, len(units)):
        if n < 1024 ** (i+1) or i == len(units):
            v = float(n)  / (1024 ** i)
            if v < 10:
                # If the number is too small, display some fractions
                return '%.1f%s' % (v, units[i])
            else:
                return '%d%s' % (int(v), units[i])

def log(s):
    print str(s)
    
def debug(*vars):
    for var in vars:
        print str(var),
    print
    
def zeros(n):
    return '\0' * n

def filter_req(req):
    """Filter payload of req, for debug use"""
    tmp = {}
    for k, v in req.items():
        if k == 'payload':
            tmp['payload-length'] = len(v)
        else:
            tmp[k] = v
    return tmp


def object_hash(id):
    return hashlib.sha1(str(id)).hexdigest()

def object_path(id):
    """Map object id number to storage path, this can be changed to other methods"""
    hash = object_hash(id)
    return os.path.join(hash[:3], hash[3:6], hash[6:])

def chunk_path(id, chunk_id, version):
    return '.'.join([object_path(id), str(chunk_id), str(version)])

def chunk_id(file, chunk_id):
    return '.'.join([file, str(chunk_id)])

def get_file_real_size(file):
    s = os.stat(file)
    if hasattr(s, 'st_blocks'):
        # st_blksize is wrong on my linux box, should be 512. Some smaller file
        # about 10k use 256 blksize, I don't know why.
        #Linux ideer 2.6.24-21-generic #1 SMP Mon Aug 25 17:32:09 UTC 2008 i686 GNU/Linux
        #Python 2.5.2 (r252:60911, Jul 31 2008, 17:28:52) 
        return s.st_blocks * 512 #s.st_blksize
    else:
        # If st_blocks is not supported
        return s.st_size
