import zlib
from pprint import pformat

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

        # Further init 
        if args.type == 'meta':
            shard = ObjectShard()
        if args.type == 'chunk':
            shard = ChunkShard()

        shard.format(dev)

    def _get_chunks(self, dev):
        """Generate chunk reports for device"""
        info('Scanning chunks on %s', dev.config.path)
        chunks = {}
        for name in os.listdir(os.path.join(dev.config.path, 'CHUNKS')):
            c = Chunk()
            c.fid, c.cid, c.version = map(int, name.split('.'))
            chunks[(c.fid, c.cid)] = c
        info('%d chunks found', len(chunks))
        return chunks

    def online(self, args):
        """Online device, send reports to storage server
        The 'online' status information is only maintained by storage server.
        """
        dev = self._get_device(args.path)
        id = dev.config.id
        if id not in self._devices:
            self._devices[id] = dev.config.path # Add entry

        if dev.config.type != 'chunk':
            raise IOError('wrong type')

        compressed_report = zlib.compress(pformat(self._get_chunks(dev)))
        self._nio.call('storage.online', conf = dev.config, addr = config.chunk_server_address, payload = compressed_report)
        dev.flush()
        self._flush()
        return 'ok'

    def offline(self, args):
        """Offline device, data on this device is not available anymore unless
        you online it again. You can decide whether to replicate data first."""
        dev = self._get_device(args.path)
        id = dev.config.id
        if id not in self._devices:
            raise IOError('not found')
        self._nio.call('storage.offline', did = id, replicate = False)
        dev.flush()

        del self._devices[id]
        self._flush()
        return 'ok'

    def frozen(self, args):
        """Make device readonly"""
        dev = self._get_device(args.path)
        id = dev.config.id
        if id not in self._devices:
            raise IOError('not found')
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

    def _print_device(self, conf):
         print '%s %s/%s %d%% %s %s' %(conf.path, byte2size(conf.used), \
             byte2size(conf.size), conf.used * 100 / conf.size, conf.mode, conf.id)

    def status(self, args):
        """Show storage status
        all       status of the global pool
        local     status of all local devs
        /data/sda status of a local dev
        """
        if args.path == 'local':
            for did, path in self._devices.items():
                print did, path
        elif args.path == 'all':
            status = self._nio.call('storage.status')
            for k in status.nodes.keys():
                node = status.nodes[k]
                print '%s:%d' % (k[0], k[1]), time.ctime(node.update_time)
                for did in node.devs:
                    print '     ',
                    self._print_device(OODict(status.devices[did].conf))
        else:
            dev = self._get_device(args.path)
            self._print_device(dev.config)
       
