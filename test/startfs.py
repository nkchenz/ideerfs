#!/usr/bin/python
"""Start a empty new fs, please specify test directory and number of devices
in tf.py"""

import os
import sys

from tf import *

#------------------------------Prepare for a clean environment-------------------

run('./stopfs.py')

# Generate devices path
devices = [os.path.join(root, 'sd' + str(x+1)) for x in range(n_devices)]
# Cleanup test dir
run('rm -rf %s' % root)
# Create dir as virtual devices
for dev in devices:
    run('mkdir -p %s' % dev)
# Cleanup cache dir
run('rm -rf %s/*' % config_home)

#----------------------------Format devices----------------------------------------
def format(device, size, type):
    ish('storage format %s size %s type %s' % (device, size, type))

# Use the first device as meta device 
meta_dev = devices.pop(0)
format(meta_dev, '10g', 'meta')
for device in devices:
    format(device, '10g', 'chunk')

#---------------------------Start Servers --------------------------------------
servers = ['meta', 'storage', 'chunk']
for s in servers:
    run('python ../%s_server.py &' % s)

#---------------------------Online devices-------------------------------
for device in devices:
    ish('storage online %s' % device)
ish('storage status all')
