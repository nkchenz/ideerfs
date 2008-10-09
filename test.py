from fs import *

# Two clients
foo = FS()
bar = FS()
foo.mount(LISTEN_ADDRESS, LISTEN_PORT)
bar.mount(LISTEN_ADDRESS, LISTEN_PORT)
foo.umount()
bar.umount()
