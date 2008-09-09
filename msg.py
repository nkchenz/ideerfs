from oodict import *

class Message(OODict):
    def __init__(self, data = {}):
        OODict.__init__(self, data)
        self.version = 1
        
