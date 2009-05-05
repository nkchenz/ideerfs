"""Config file for the filesystem """

import os
import socket

#--------------Please Modify these to proper values-------------
# Please modify your firewall setting to open these ports
meta_server_address = ('localhost', 41984)
storage_server_address = ('localhost', 41985)
chunk_server_address = ('localhost', 41986)
# Meta device
meta_dev = '/home/chenz/source/ideerfs/test/tmp/sd1'

# Chunk checksum algo when updating, only support '', 'adler32', 'sha1'
# This better be a attribute of file
checksum_algo = '' # No checksum needed

daemon = True # Servers run as daemon


#--------------No need to change the following----------------------------
config_dir = '.ideerfs'
home = os.path.join(os.getenv('HOME'), config_dir)
if not os.path.exists(home):
    os.mkdir(home)
