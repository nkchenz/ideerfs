#!/usr/bin/python

import os
import sys

from tf import *

#---------------------------Start Servers --------------------------------------
servers = ['meta', 'storage', 'chunk']
for s in servers:
    run('python ../%s_server.py &' % s)

#---------------------------Online devices-------------------------------
for device in devices:
    ish('storage online %s' % device)
ish('storage status all')
