#!/usr/bin/python

"""Shutdown fs, did not delete test fs data"""

import os

from tf import *

servers = ['meta', 'storage', 'chunk']
for s in servers:
    pid_file = os.path.join(config_home, s + '_server.pid')
    if os.path.exists(pid_file):
        pid = open(pid_file, 'r').readline().strip()
        run('kill %s' % pid)
