"""Config file for the filesystem """

import os

# Modify thi
meta_server_address = ('localhost', 1984)
storage_server_address = ('localhost', 1985)
chunk_server_address = ('localhost', 1986)
# Meta device
meta_dev = 'tmp/sd1'



config_dir = '.ideerfs'
home = os.path.join(os.getenv('HOME'), config_dir)
