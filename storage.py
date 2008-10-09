#coding: utf8
import os
import pprint
from oodict import *
from conf import *
from channel import *

class Storage:

    """The Global Storage Pool"""

    def __init__(self):
        self.devs = OODict()
        self.dev_sn = 0

    def add(self, msg, channel):
        dev = OODict(msg.data.dev)
        dev.id = self.dev_sn
        self.dev_sn += 1
        dev.host = channel.host 
        dev.path = msg.data.path
        self.devs[dev.id] = dev

        ret = Message()        
        data = ret.data
        data.cmd = 'return' # 用于传输返回值
        data.status = 'committed'
        data.caller = msg.data.sn
        data.id = dev.id
        channel.send(ret)

        
    def list(self, msg, channel):
        ret = Message()        
        data = ret.data
        data.cmd = 'return' # 用于传输返回值
        data.status = 'committed'
        data.caller = msg.data.sn
        
        data.total_size = 0
        data.used_size = 0
        for v in self.devs.values():
            data.total_size += v.total_size 
            data.used_size += v.used_size 
        channel.send(ret)


class StorageAdmin:
    """Storage admin tool"""

    def __init__(self):
        pass

    def mount(self, ip, port):
        self.storage_channel = NewChannel(ip, port)
        if self.storage_channel:
            msg = self.storage_channel.recv()
            print msg.data
            return True
        return False

    def umount(self):
        if not self.storage_channel:
            return
        bye = Message()        
        bye.data.cmd = 'bye'
        self.storage_channel.send(bye)
        self.storage_channel.close()
        self.storage_channel = None


    def add(self, path, size, type):
        config = os.path.join(path, STORAGE_CONFIG_PATH)
        if os.path.isfile(config):
            print 'Found config, seems %s has been added already, try \'online\'' % path
            return False
        
        dev = OODict()
        dev.total_size = size
        dev.used_size = 0
        dev.type = type
        
        msg = Message()
        msg.data.cmd = 'storage.add' 
        msg.data.dev = dev
        msg.data.path = path
        self.storage_channel.send(msg)
        # AIO
        msg = self.storage_channel.recv()
        if msg and msg.data.status == 'committed':
            print msg.data
            dev.id = msg.data.id
            os.makedirs(os.path.dirname(config))
            open(config, 'w+').write(pprint.pformat(dev))
            return True
        return False

    def list(self):
        msg = Message()
        msg.data.cmd = 'storage.list' 
        self.storage_channel.send(msg)
        msg = self.storage_channel.recv()
        if msg and msg.data.status == 'committed':
            print 'used %d of %d' % (msg.data.used_size, msg.data.total_size)
        
