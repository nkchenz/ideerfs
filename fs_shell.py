"""
FS shell
"""

from fs import *
from io import *
from oodict import *

class FSShell:
    def __init__(self):
        self._db = FileDB(config.home)
        self.usage_rules = {
            'lsdir': 'lsdir',
            'lsdir $dir': 'lsdir',
            'mkdir $dirs': 'mkdir',
            'exists $file': 'exists',
            'get_file_meta $file': 'get_file_meta',
            'get_chunk_info $file $chunk_id': 'get_chunk_info',
            'get $attrs of $file': 'get_file_meta',
            'set $attrs of $file to $values': 'set_file_attr',
            'rm $files': 'rm', # match rm [-r|-R]
            'mv $old_file $new_file': 'mv',
            'store $localfile $file': 'store', # cp from local
            'restore $file $localfile': 'restore', # cp to local
            #'cp src $src dest $dest': 'cp',
            'cp $src $dest': 'cp',
            'stat': 'stat',
            'touch $files': 'touch',
            'cd $dir': 'cd',
            'pwd': 'pwd'
            }
        self.fs = FileSystem()

    def _getpwd(self):
        return self._db.load('pwd', '')

    def _normpath(self, dir):
        # Normalize path with PWD env considered
        p = os.path.join(self._getpwd(), dir)
        if not p:
            return '' # Empty pwd and dir
        # Make sure it's an absolute path, pwd is client only, path is assumed to
        # be absolute when communicating with meta node
        return os.path.normpath(os.path.join('/', p))

    def cd(self, args):
        dir = self._normpath(args.dir)
        meta = self.fs.get_file_meta(dir)
        if meta and meta.type == 'dir':
            self._db.save(dir, 'pwd')

    def get_file_meta(self, args):
        file = self._normpath(args.file)
        print self.fs.get_file_meta(file)

    def get_chunk_info(self, args):
        file = self._normpath(args.file)
        meta = self.fs.get_file_meta(file)
        info = self.fs.get_chunk_info(file, [args.chunk_id])
        if not info:
            print 'no such chunk'
            return
        print info

    def store(self, args):
        """Store local file to the fs"""
        src = args.localfile
        if not os.path.exists(src):
            print src, 'not exists'
            return
        # Read local file
        data = open(src, 'rb').read()
        dest = self._normpath(args.file)
        # Create new file and write
        self.fs.create(dest, replica_factor = 3, chunk_size = 67108864)
        f = self.fs.open(dest)
        f.write(0, data)

    def restore(self, args):
        """Restore file in the fs to local filesystem"""
        file = self._normpath(args.file)
        meta = self.fs.get_file_meta(file)
        f = self.fs.open(file)
        data = f.read(0, meta.size)
        open(args.localfile, 'w').write(data)


    def cp(self, args):
        src = self._normpath(args.src)
        dest = self._normpath(args.dest)
        meta = self.fs.get_file_meta(src)
        f = self.fs.open(src)
        data = f.read(0, meta.size)

        self.fs.create(dest, replica_factor = 3, chunk_size = 67108864)
        f = self.fs.open(dest)
        f.write(0, data)


    def exists(self, args):
        print self.fs.exists(args.file)

    def lsdir(self, args):
        if 'dir' not in args: # If no dir, then list pwd dir
            args.dir = ''
        dir = self._normpath(args.dir)
        if not dir:
            dir = '/'
        files = self.fs.lsdir(dir)
        if files:
            print ' '.join(sorted(files))

    def mkdir(self, args):
        # dirs can't be empty because in that case we wont get here, cant pass nlp
        for dir in args.dirs.split():
            dir = self._normpath(dir)
            self.fs.mkdir(dir)

    def pwd(self, args):
        print self._getpwd()

    def touch(self, args):
        for file in args.files.split():
            file = self._normpath(file)
            self.fs.create(file, replica_factor = 3, chunk_size = 2 ** 25) #32m

    def rm(self, args):
        files = args.files.split()
        recursive = False
        if files[0] in ['-r', '-R']:
            recursive = True
            files.pop(0)
        for file in files:
            file = self._normpath(file)
            self.fs.delete(file, recursive)

    def mv(self, args):
        old_file = self._normpath(args.old_file)
        new_file = self._normpath(args.new_file)
        self.fs.mv(old_file, new_file)
