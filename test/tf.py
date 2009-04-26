#!/usr/bin/python

"""Common test configs and functions"""

import os
import sys


# -------------------------------Test config --------------------------------
# Where to store the test files?
root = os.path.join(os.getcwd(), 'tmp')
# How many chunk devices do you want?
n_devices = 6
devices = [os.path.join(root, 'sd' + str(x+1)) for x in range(n_devices)]

config_home = os.path.expanduser('~/.ideerfs')

def ish(args):
    run('../ideer.py %s' % (args))

def run(cmd):
    print '$', cmd
    try:
        os.system(cmd)
        pass
    except:
        raise
        sys.exit(-1)
