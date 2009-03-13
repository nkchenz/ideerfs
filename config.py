"""
Config file for the filesystem
"""
import os

meta_server_address = ('localhost', 1984)
storage_manager_address = ('localhost', 1985)
chunk_server_address = ('localhost', 1986)

# Where to store the test files?
root = '~/ideerfs.test'

# Disk for meta data 
meta_dev = os.path.join(root, 'disk1')

# All the disks this node has for chunk data
devices= []
for n in range(2, 10):
    devices.append(os.path.join(root, 'disk%d' % n))

# Disks whose states have been changed, need update to storage manager
devices_changed = []

print locals()
