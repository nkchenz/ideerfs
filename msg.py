from oodict import *

class Message(OODict):
    """
    Message format:
        length + newline + msg + payload if any
    """

    def __init__(self, data = {}):
        self.data = OODict(data)
        if 'version' not in self.data:
            self.data.version = 1

        # Maintainance data
        self.state = 'init'
        self.payload = None
