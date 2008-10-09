
"""
FS client module

Make a conn to MDS, get file replication and chunk infos, then make parallel
async conns with OSTs

"""
from conf import *
from msg import *
from channel import *

class StoragePool:
    pass

class DataSet:
    # Is it really neccessary to have data set?
    pass

class FileStream:
    pass

class FS:
    """
    cp
    touch
    mkdir
    """
    
    def __init__(self):
        pass

    def mount(self, ip, port):
        self.meta_channel = NewChannel(ip, port)
        if self.meta_channel:
            msg = self.meta_channel.recv()
            print msg.data
            return True
        return False

    def umount(self):
        if not self.meta_channel:
            return
        bye = Message()        
        bye.data.cmd = 'bye'
        self.meta_channel.send(bye)
        self.meta_channel.close()
        self.meta_channel = None

