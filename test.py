#!/usr/bin/python
#coding: utf8

"""Simple test script"""


import os
import time
import sys
import config

# -------------------------------Test config --------------------------------
# Server address
config.meta_server_address = ('localhost', 1984)
config.storage_server_address = ('localhost', 1985)
config.chunk_server_address = ('localhost', 1986)

# Where to store the test files?
root = 'tmp'
# How many chunk devices do you want?
n_devices = 6

#--------------------------------Misc functions-------------------------------
def run(cmd):
    print '$', cmd
    try:
        os.system(cmd)
        pass
    except:
        print 'ctl+c'
        sys.exit(-1)

def format(device, size, type):
    run('./ideer.py storage format %s size %s type %s' % (device, size, type))

#------------------------------Prepare for a clean environment-------------------
# Generate devices path
devices = [os.path.join(root, 'sd' + str(x+1)) for x in range(n_devices)]
# Cleanup test dir
run('rm -rf %s' % root)
# Create dir as virtual devices
for dev in devices:
    run('mkdir -p %s' % dev)
# Cleanup cache dir
run('rm -rf %s/*' % config.home)


#----------------------------Format devices----------------------------------------
# Use the first device as meta device 
config.meta_dev = devices.pop(0)
format(config.meta_dev, '10g', 'meta')
for device in devices:
    format(device, '10g', 'chunk')

#---------------------------Start Servers --------------------------------------
servers = ['meta', 'storage', 'chunk']
for s in servers:
    run('python %s_server.py &' % s)

#---------------------------Online devices-------------------------------
for device in devices:
    run('./ideer.py storage online %s' % device)
run('./ideer.py storage status all')

#-------------------------------FS Tests-----------------------------------

"""touch a b c
lsdir
store ideer.py test.py
restore test.py ideer.py.new
diff ideer.py ideer.py.new


storage
status all
status local
mkdir

mkdir foo
mkdir foo/bar
rm -r foo
"""

#-------------------------------Cleanup---------------------------------------
for s in servers:
    pid_file = os.path.join(config.home, s + '_server.pid')
    if os.path.exists(pid_file):
        pid = open(pid_file, 'r').readline()
        run('kill %s' % pid)
