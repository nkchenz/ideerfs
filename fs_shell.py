"""FS shell """

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
            'stat $file': 'stat',
            'rm $files': 'rm', # match rm [-r|-R]
            'mv $old_file $new_file': 'mv',
            'store $localfile $file': 'store', # cp from local
            'restore $file $localfile': 'restore', # cp to local
            #'cp src $src dest $dest': 'cp',
            'cp $src $dest': 'cp',
            'touch $files': 'touch',
            'cd $dir': 'cd',
            'pwd': 'pwd'
            }
        self._fs = FileSystem()

    def _getpwd(self):
        return self._db.load('pwd', '')

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
            self._db.store(dir, 'pwd')

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
        data = open(src, 'rb').read()
        # Create new file and write
        self._fs.create(dest, replica_factor = 3, chunk_size = 2 ** 25)
        f = self._fs.open(dest)
        f.write(0, data)
        f.close()

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
        file = self._normpath(args.file)
        meta = self._fs.stat(file)
        f = self._fs.open(file)
        data = f.read(0, meta.size)
        open(args.localfile, 'w').write(data)


    def cp(self, args):
        src = self._normpath(args.src)
        dest = self._normpath(args.dest)
        meta = self._fs.stat(src)
        f = self._fs.open(src)
        data = f.read(0, meta.size)

        self._fs.create(dest, replica_factor = 3, chunk_size = 2 ** 25)
        f = self._fs.open(dest)
        f.write(0, data)


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
