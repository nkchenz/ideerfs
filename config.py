"""Config file for the filesystem """

import os

#--------------Please Modify these to proper values-------------
meta_server_address = ('localhost', 1984)
storage_server_address = ('localhost', 1985)
chunk_server_address = ('localhost', 1986)
# Meta device
meta_dev = 'tmp/sd1'


#--------------No need to change these----------------------------
config_dir = '.ideerfs'
home = os.path.join(os.getenv('HOME'), config_dir)
