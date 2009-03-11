"""
Object interface
"""

import time
import hashlib
from oodict import OODict

class Object(OODict):

    def __init__(self, name, id, parent_id, type, attr = {}):
        self.id = id
        self.type = type
        self.meta = {
            'ctime': '%d' % time.time(),
            'name': name
        }
        
        for k,v in attr.items():
            self.meta[k] = v
        
        if type == 'dir':
            self.children = {
                '.': id,
                '..': parent_id
            }
        else:
            self.meta['size'] = 0
            self.chunks = {}


