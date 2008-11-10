import time
import hashlib
from oodict import OODict

class Object(OODict):

    def __init__(self, name, id, parent_id, type, attr = {}):
        self.id = id
        self.meta = {
            'type': type,
            'ctime': '%d' % time.time(),
            'name': name
        }
        
        if attr:
            self.meta.attr = attr
        
        if type == 'dir':
            self.children = {
                '.': id,
                '..': parent_id
            }
        else:
            self.chunks = {}
