
import hashlib
import pprint
from oodict import OODict

seq = 0

class Object(OODict):
    def __init__(self, data):
        self.id = next_seq()
        self.data = data
        self.checksum = ''
        self.version = ''
        self.ref = 0
        self.replications = [devx, devy, devz]
        self.compress = ''
        self.type = ''
    
    def serialize()
        """
        # Save (type, compress, checksum, version, data) to file id
        if isinstance(data, 'str'):
            self.data = data
        else:
            self.data = pprint.pformat(data)
        """
        self.checksum = hashlib.sha1(self.data).hexdigest()
        pass

class ChunkObject(Object):
    def __init__(self, data):
        self.data = data
        self.type = 'chunk'
        f.replicate_facotr = 3
        pass


class Meta(OODict):
    pass
    

class FileObject(Object):
    def __init_(self):
        f.type = 'file'
        f.name = 'zfs snapshot'
        f.size = len(data)
        f.chunk_size = 128
        f.replicate_facotr = 1
        f.chunks = [
        1: (chunk1 id, chunk1 locations)
        3: (chunk3 id, chunk3 locations)
        ]   
        
    def serialize():
        self.data = ...
        super.serialize()
        

self.data.meta = Meta()
        self.chunks.chunks



class Locations:
    def __init__(self):
        self.id = 0
        self.replications = 0


def next_seq():
    global seq
    seq += 1
    return seq



"""
新建一个Locations目录 (key, locations)
存放所有对象的位置信息

86f7e437faa5a7fce15d1ddcb9eaeaea377667b8:
1
2
3


Meta目录存放所有对象之间所属关系



Object存放所有对象
如果有cow，则不修改原有object，就不需要version。只有当修改原对象时，才需要version以示区别。

        self.id = next_seq()
        self.data = data
        self.checksum = ''
        self.version = ''
        self.ref = 0
        self.rf = 3
        self.compress = ''
        self.type = ''

ROOT文件 存放整个文件系统根对象，可以有多份



dir = {
 id: 3aef
 type: 'dir'
 version: 0
 rf: 1          # 对象复制3份，那meta数据是否要复制三份？
 ref: 0
 data:{
    name: '/'
    size: 800
 
    attr: {
        private
        inheritable
    }
 
    children: { # For children
        '.':  self@version
        '..': parent@version
        'a':  adb3@version
        ...
    }
 checksum: hash(data)
 compress: '' #compress data before hash
}

data_replications = 'chunk'
meta_replications = 'dir', 'file'

file = {
 id: b2a4
 type: 'file'
 version: 1
 rf: 1  # File meta only has one replications
 ref:0
 data:{      
      name: 'a'
      size: 168
      chunk_size: 128

     attr:{
        data_replications: 3 # File chunks has three replications
     }
     chunks:{ # For chunks, allow holes in file
      1: b45a@version
      3: f78a@version
     }
 }
 checksum: hash(data)
 compress: '' #compress data before hash
}

chunk = {
 id: ad4e
 type: 'chunk'
 version: 1
 rf: 3  # File meta only has one replications
 ref:0
 data: {'chunk data'
       file: b2a4
 }
 checksum: hash(data)
 compress: '' #compress data before hash
}


dir, file are both object container:) object container itself is a object too.



fid.chunk_id = {   
   version:3,
   sha1: b2a4...
}



"""





class Chunk(OODict):
    pass
    


class File(Object):
    
    def __init__(self):
        pass
        
    def serialize():
        pass


data = '''
ZFS uses a copy-on-write, transactional object model. All block pointers within 
the filesystem contain a 256-bit checksum of the target block which is verified 
when the block is read. Blocks containing active data are never overwritten in 
place; instead, a new block is allocated, modified data is written to it, and 
then any metadata blocks referencing it are similarly read, reallocated, and 
written. To reduce the overhead of this process, multiple updates are grouped 
into transaction groups, and an intent log is used when synchronous write 
semantics are required.
'''

def split_chunks(data, size):
    # If size is 0, or data is empty, return []
    chunks = []
    if not data or not size:
        return chunks
    offset = 0
    l = len(data)
    while True:
        offset2 = offset + size
        if offset2 >= l:
            chunks.append(data[offset:])
            break
        chunks.append(data[offset: offset2])
        offset = offset2
    return chunks

f = File()
f.name = 'zfs snapshot'
f.size = len(data)
f.chunk_size = 128
f.replicate_facotr = 3
f.chunks = []

#pprint.pprint(f)

for chunk in split_chunks(data, f.chunk_size):
    o = Object(chunk)
    f.chunks.append(o.id)

print f.chunks

print Object(f).id