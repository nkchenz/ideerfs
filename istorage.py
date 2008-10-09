from storage import *


sadmin = StorageAdmin()
sadmin.mount(LISTEN_ADDRESS, LISTEN_PORT)

sadmin.add('/data/sda00', 1073741824, 'chunk')
sadmin.add('/data/sda01', 1073741824, 'chunk')
sadmin.list()
sadmin.umount()
