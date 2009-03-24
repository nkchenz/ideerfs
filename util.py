"""Misc utils """

import os

# Misc functions
def size2byte(s):
    """Translate human readable size number to int byte.
    unit is only one of 'k, m, g, t, p'. If it's malformed return None
    """
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
    """Translate size in byte to human readable format"""
    units=['', 'k', 'm', 'g', 't', 'p']
    for i in range(0, len(units)):
        if n < 1024 ** (i+1) or i == len(units):
            v = float(n)  / (1024 ** i)
            if v < 10:
                # If the number is too small, display some fractions
                return '%.1f%s' % (v, units[i])
            else:
                return '%d%s' % (int(v), units[i])

def zeros(n):
    return '\0' * n

def log(*vars):
    debug(*vars)
    
def debug(*vars):
    for var in vars:
        if isinstance(var, dict) and 'payload' in var:
            var = filter_req(var)
        print str(var),
    print

def filter_req(req):
    """Filter payload of req, because it's binary. For debug use only"""
    tmp = {}
    for k, v in req.items():
        if k == 'payload':
            tmp['payload-length'] = len(v)
        else:
            tmp[k] = v
    return tmp

def get_realsize(file):
    """Return the real size of a sparse file"""
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
