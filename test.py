#!/usr/bin/python
"""
Run this script directly, after you seeing something like this, 

./ideer.py storage online /data/test/disk-0
socket.error (111, 'Connection refused') 0s used
sleep 1 seconds, 5 more retrys
socket.error (111, 'Connection refused') 0s used
sleep 2 seconds, 4 more retrys
socket.error (111, 'Connection refused') 0s used
sleep 4 seconds, 3 more retrys

please open another terminal and start the servers, here is the command:
ideer@ideer:/chenz/source/ideerfs$ ./ideer.py service start meta,storage,chunk at localhost:1984

'localhost:1984' is the 'addr' below.
"""


import os
import time

root = '/data/test'
disks = 3
addr = 'localhost:1984'


meta_dev = os.path.join(root, 'disk-meta')
chunk_devs = [os.path.join(root, 'disk-' + str(x)) for x in range(disks)]

def run(cmd):
    print '$', cmd
    os.system(cmd)

# Prepare disks
def cleanup(devs):
    for dev in devs:
        run('mkdir -p ' +  dev)
        run('rm -rf %s/*' % dev)
        
    run('rm -rf ~/.ideerfs/*')

cleanup([meta_dev] + chunk_devs)

# Format devices
def _format(dev, type):
    run('./ideer.py storage format %s size 10g for %s data' % (dev, type))
def format():
    _format(meta_dev, 'meta')
    for dev in chunk_devs:
        _format(dev, 'chunk')
format()

init = [
'./ideer.py service use %s as meta device' % meta_dev, # Setting meta device

# For client use
'./ideer.py service meta at %s' % addr,
'./ideer.py service storage at %s' % addr,
'./ideer.py service chunk at %s' % addr,

]


for cmd in init:
    run(cmd)
    
    
def online(devs):
    for dev in devs:
        run('./ideer.py storage online ' + dev)
    
    # Show local disk status
    run('./ideer.py storage stat local')
    
# Online device
online(chunk_devs)

# Silent time, waitting for status of online devices got updated
time.sleep(10)

# Create some files
run('./ideer.py fs touch a b c ')
run('./ideer.py fs lsdir')

# Write and read
run('./ideer.py fs store ideer.py /ideer.py')
run('./ideer.py fs restore /ideer.py ideer.py.new')
run('diff ideer.py ideer.py.new')


# Waitting for disks status got updated
time.sleep(10)

# Global Status
run('./ideer.py storage stat all')
