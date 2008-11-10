import os
from oodict import OODict
from pprint import pformat

class ConfigManager:
    def __init__(self, root):
        """Set root directory of configs"""
        self.root = root
    
    def load(self, file, default = None):
        """Auto convert dict to OODict, if None return default value"""
        f = os.path.join(self.root, file)
        if os.path.exists(f):
            result = eval(open(f, 'r').read())
            if isinstance(result, dict):
                return OODict(result)
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
        open(f, 'w+').write(pformat(config))


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
            return '%d%s' % (n / (1024 ** i), units[i])

def log(s):
    print s
    
def debug(s):
    print s
    