"""FS shell """

from fs import *
from io import *
from oodict import *

class FSShell:
    def __init__(self):
        self._db = FileDB('.')
        self.usage_rules = {
            'lsdir': 'lsdir',
            'lsdir $dir': 'lsdir',
            'mkdir $dirs': 'mkdir',
            'exists $file': 'exists',
            'stat $file': 'stat',
            'rm $files': 'rm', # match rm [-r|-R]
            'mv $old_file $new_file': 'mv',
            'store $localfile $file': 'store', # cp from local
            'restore $file $localfile': 'restore', # cp to local
            #'cp src $src dest $dest': 'cp',
            'cp $src $dest': 'cp',
            'touch $files': 'touch',
            'cd $dir': 'cd',
            'set $attr of $file to $value': 'set',
            'pwd': 'pwd'
            }
        self._fs = FileSystem()

    def _getpwd(self):
        return self._db.load('.pwd', '')

    def _normpath(self, path):
        """Normalize path with PWD env considered"""
        full_path = os.path.join(self._getpwd(), path)
        if not full_path:
            return '' # Empty pwd and dir
        # Make sure it's an absolute path, PWD is client only, path is assumed to
        # be absolute when communicating with meta server 
        return os.path.normpath(os.path.join('/', full_path))

    def cd(self, args):
        dir = self._normpath(args.dir)
        meta = self._fs.stat(dir)
        if meta and meta.type == 'dir':
            self._db.store(dir, '.pwd')

    def stat(self, args):
        file = self._normpath(args.file)
        print self._fs.stat(file)

    def store(self, args):
        """Store local file to the fs

        @localfile
        @file
        """
        src = args.localfile
        if not os.path.exists(src):
            print src, 'not exists'
            return

        dest = self._normpath(args.file)
        if os.path.isdir(src):
            self._copy_dir(src, dest)
        else:
            self._copy_file(src, dest)

    def _copy_file(self, src, dest):
        """Store local file """
        # Read local file
        sf = open(src, 'rb')
        self._fs.create(dest, replica_factor = 1, chunk_size = 2 ** 25)
        df = self._fs.open(dest)
        buf_size = 2 ** 25 # 32M
        while True:
            data = sf.read(buf_size)
            if not data:
                break
            df.write(data)
        sf.close()
        df.close()

    def _copy_dir(self, src, dest):
        """Store all the things in dir src under dir dest"""
        self._fs.mkdir(dest)
        for child in os.listdir(src):
            sf = os.path.join(src, child)
            df = os.path.join(dest, child)
            print sf
            if os.path.isdir(sf):
                self._copy_dir(sf, df)
            else:
                self._copy_file(sf, df)

    def restore(self, args):
        """Restore file in the fs to local filesystem
        
        @file
        @localfile
        """
        src = self._fs.open(self._normpath(args.file))
        data = src.read()
        src.close()
        dest = open(args.localfile, 'w')
        dest.write(data)
        dest.close()


    def cp(self, args):
        src = self._fs.open(self._normpath(args.src))
        destf = self._normpath(args.dest)
        self._fs.create(destf, replica_factor = 3, chunk_size = 2 ** 25)
        data = src.read()
        src.close()
        dest = self._fs.open(destf)
        dest.write(data)
        dest.close()


    def exists(self, args):
        print self._fs.exists(args.file)

    def lsdir(self, args):
        if 'dir' not in args: # If no dir, then list pwd dir
            args.dir = ''
        dir = self._normpath(args.dir)
        if not dir:
            dir = '/'
        files = self._fs.lsdir(dir)
        if files:
            print ' '.join(sorted(files))

    def mkdir(self, args):
        # dirs can't be empty because in that case we wont get here, cant pass nlp
        for dir in args.dirs.split():
            dir = self._normpath(dir)
            self._fs.mkdir(dir)

    def pwd(self, args):
        print self._getpwd()

    def touch(self, args):
        for file in args.files.split():
            file = self._normpath(file)
            self._fs.create(file, replica_factor = 3, chunk_size = 2 ** 25) #32m

    def rm(self, args):
        files = args.files.split()
        recursive = False
        if files[0] in ['-r', '-R']:
            recursive = True
            files.pop(0)
        for file in files:
            file = self._normpath(file)
            self._fs.delete(file, recursive)

    def mv(self, args):
        old_file = self._normpath(args.old_file)
        new_file = self._normpath(args.new_file)
        self._fs.mv(old_file, new_file)

    def set(self, args):
        file = self._normpath(args.file)
        meta = self._fs.stat(file)
        self._fs.setattr(meta.id, {args.attr: args.value})
