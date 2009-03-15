"""
Config file for the filesystem
"""
import os

meta_server_address = ('localhost', 1984)
storage_manager_address = ('localhost', 1985)
chunk_server_address = ('localhost', 1986)

home = '~/.ideerfs'

# Where to store the test files?
root = '/tmp/ideerfs.test'
# Disk for meta data 
meta_dev = os.path.join(root, 'disk1')