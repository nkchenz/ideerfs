"""Config file for the filesystem """

import os

#--------------Please Modify these to proper values-------------
meta_server_address = ('localhost', 1984)
storage_server_address = ('localhost', 1985)
chunk_server_address = ('localhost', 1986)
# Meta device
meta_dev = '/home/chenz/source/ideerfs/test/tmp/sd1'


# Chunk checksum algo when updating, only support '', 'adler32', 'sha1'
# This better be a attribute of file
checksum_algo = '' # No checksum needed

#--------------No need to change these----------------------------
config_dir = '.ideerfs'
home = os.path.join(os.getenv('HOME'), config_dir)
if not os.path.exists(home):
    os.mkdir(home)

daemon = False #True # Servers run as daemon

