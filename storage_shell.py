
from io import *
from dev import *
from obj import *
from nio import *

class StorageShell:
    """Manage local disks"""

    def __init__(self):
        """Usage format, symbols beginning with '$' are vars which you can use directly
        in the 'args' parameter"""
        self.usage_rules = {
            'format $path size $size type $type': 'format',
            'online $path': 'online',
            'offline $path': 'offline',
            'frozen $path': 'frozen',
            'remove $path': 'remove',
            'replace $old_path with $new_path': 'replace',
            'status $path': 'status',
        }

        self._db = FileDB(config.home)
        self._devices_file = 'exported_devices'

        # Lookup table for chunk service, from id to path
        self._devices = self._db.load(self._devices_file, {})

    def _flush(self):
        self._db.store(self._devices, self._devices_file)

    def _pre_command(self, cmd, args):
        """Hook for pre cmd running"""
        if cmd not in ['format']: # No need to connect storage service while formatting
            self._nio = NetWorkIO(config.storage_server_address)

    def _get_device(self, path):
        """Get device by path"""
        dev = Dev(path)
        if not dev.config:
            raise IOError('not formatted')
        
        if dev.config.type != 'chunk':
            raise IOError('wrong type')

        return dev

    def format(self, args):
        """Format device, create root object if type is meta"""
        dev = Dev(args.path)
        dev.format(args)
        shard = ObjectShard()
        if args.type == 'meta':
            shard.create_root_object(args.path)

    def get_chunks(self, dev):
        """Get chunk list of a device"""
        log('Scanning chunks on ', dev.config.path)
        return dev.load('chunks', {})

    def online(self, args):
        """Online device, send reports to storage server"""
        dev = self._get_device(args.path)
        id = dev.config.id
        if id not in self._devices:
            self._devices[id] = dev.config.path # Add entry
        if dev.config.status != 'offline':
            raise IOError('not offline')
        self._nio.call('storage.online', conf = dev.config, addr = config.chunk_server_address, report = self.get_chunks(dev))
        dev.config.status = 'online'
        dev.flush()
        self._flush()
        return 'ok'

    def offline(self, req):
        """Offline device, data on this device is not available anymore unless
        you online it again. You can decide whether to replicate data first."""
        dev = self._get_device(args.path)
        id = dev.config.id
        if id not in self._devices:
            raise IOError('not found')
        if dev.config.status != 'online':
            raise IOError('not online')
        self._nio.call('storage.offline', did = id, replicate = False)
        dev.config.status = 'offline'
        dev.flush()
        return 'ok'

    def frozen(self, req):
        """Make device readonly"""
        dev = self._get_device(args.path)
        id = dev.config.id
        if id not in self._devices:
            raise IOError('not found')
        if dev.config.status != 'online':
            raise IOError('not online')
        self._nio.call('storage.frozen', did = id)
        dev.config.mode = 'frozen'
        dev.flush()
        return 'ok'

    def replace(self, args):
        """Replace device"""
        # frozen old_dev
        # cp old_dev new_dev
        # online new_dev
        # offline new_dev with replicate = False
        pass

    def status(self, args):
        """Show storage status
        all       status of the global pool
        local     status of all local devs
        /data/sda status of a local dev
        """
        if args.path == 'local':
            print self._devices
        elif args.path == 'all':
            print self._nio.call('storage.status')
        else:
            dev = self._get_device(args.path)
            print dev.config
        #print '%s %s/%s %d%% %s %s %s' %(dev.path, byte2size(dev.used), byte2size(dev.size), dev.used * 100 / dev.size, dev.status, dev.mode, host)
