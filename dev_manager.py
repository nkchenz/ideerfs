"""
Disk management

Run on local node, send information to storage manager. If you want to online
a device, you must send all the chunks it has aka chunkreports to the storage
manager. If you find a invalid device, you need also to inform the storage
manager.

"""

class DevManager(Service):
    """
    how to send chunkreport? push or poll? use a independent thread or piggiback
    with heart beat message?

    Maybe ideer.py shall contact with storage manager directly, chunk serivce only
    provides read, write, replicate, delete, report operations etc, it's a clear design.

    The diffcult is when allocating a new chunk, how to deal with device used size,
    we have no idea about the real size of a sparse chunk file
    """

    def __init__(self, storage_addr):
        self._storage_service_addr = storage_addr

        self.cache_file = 'exported_devices'
        self.cm = ConfigManager(os.path.expanduser('~/.ideerfs/'))
        self.cache = self.cm.load(self.cache_file, OODict())

        self.chunks = {}
        self.changed = set()
        self.need_send_report = set()

        # If you use a[k] access a dict item, it's converted to OODict automatically
        # For later convenience
        for k, v in self.cache.items():
            v = OODict(v)
            self.cache[k] = v
            if v.status == 'online':
                thread.start_new_thread(self.scan_chunks, (v,))

    def send_chunk_report():
            chunks_report = {}
            for id in need_send_report:
                chunks_report[id] = self.devices.chunks[id]
                del self.devices.chunks[id]

            rc = nio.call('storage.hb', addr = self._addr, changed_devs = changed_devs, \
                chunks_report = chunks_report)


    
    def scan_chunks(self, dev):
        debug('scanning chunks', dev.path)
        self.chunks[dev.id] = self.get_chunks_list(dev.path)
        self.changed.add(dev.id)
        self.need_send_report.add(dev.id)

    def get_chunks_list(self, path):
        """Get all the chunks of a device"""
        root = os.path.join(path, 'OBJECTS')
        chunks = []
        for n1 in os.listdir(root):
            l1 = os.path.join(root, n1)
            for n2 in os.listdir(l1):
                l2 = os.path.join(l1, n2)
                chunks.append(map(lambda f: n1 + n2 +f, os.listdir(l2)))
        return reduce(lambda x, y : x +y, chunks, [])

    def _flush(self, id):
        """Flush the changes of dev indicated by id"""
        self.changed.add(id)
        self.cm.save(self.cache, self.cache_file)

    def _get_dev(self, path):
        """Return a dev object, but check status first"""
        dev = Dev(path)
        if not dev.config:
            self._error('not formatted')

        if dev.config.data_type != 'chunk':
            self._error('wrong data type')
        return dev

    def online(self, req):
        """Update dev status in devices cache to 'online', notice that we do not
        update the config file on disk"""
        dev = self._get_dev(req.path) # Get dev from path
        id = dev.config.id
        if id not in self.cache:
            self.cache[id] = dev.config # Add entry
        else:
            if self.cache[id].status != 'offline':
                self._error('not offline')

        self.cache[id].status = 'online'
        self._flush(id)

        # Start another thread to scan all the chunks the new device holds
        thread.start_new_thread(self.scan_chunks, (dev.config,))
        return 'ok'

    def offline(self, req):
        """Update dev status in devices cache to 'offline'
        """
        dev = self._get_dev(req.path) # Get dev from path
        id = dev.config.id
        if id not in self.cache:
            self._error('not in cache')

        if self.cache[id].status != 'online':
            self._error('not online')

        self.cache[id].status = 'offline'
        self._flush(id)
        # Notify stroage manager dev offline
        return 'ok'

    def remove(self, req):
        dev = self._get_dev(req.path) # Get dev from path
        id = dev.config.id
        if id not in self.cache:
            self._error('not in cache')
        del self.cache[id]
        self._flush(id)
        return 'ok'

    def frozen(self, req):
        dev = self._get_dev(req.path)
        id = dev.config.id
        if id not in self.cache:
            self._error('not in cache')

        self.cache[id].mode = 'frozen'
        self._flush(id)
        return 'ok'

    def stat(self, req):
        if req.path == 'all':
            nio = NetWorkIO(self._storage_service_addr)
            return nio.call('storage.stat')

        if req.path == 'local':
            return {'disks': self.cache}

        for k, v in self.cache.items():
            if v['path'] == req.path:
                return {'disks': {k: v}}

        self._error('no such dev')

        # Iterate dev cache to get realtime stat here, disk stat is carried with
        # chunk server's heart-beat message
        #return  {'summary': self.statistics, 'disks': self.cache}
