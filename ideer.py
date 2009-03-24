#!/usr/bin/python

"""ideer shell """

import os
import sys

from nlp import NLParser
import fs_shell
import job_shell
import storage_shell

controller = {
'storage': storage_shell.StorageShell,
'fs': fs_shell.FSShell,
'job': job_shell.JobShell,
}

if len(sys.argv) <= 1 or sys.argv[1] == 'help':
    print 'Usage:', sys.argv[0], '|'.join(subcmds), 'action'
    sys.exit(-1)

cmd = sys.argv[1]
if cmd not in controller:
    print 'Unknown command', cmd
    sys.exit(-1)

# Set dispatcher and rules for nlp
nlp = NLParser()
nlp.set_handler(controller[cmd]())
args = ' '.join(sys.argv[2:])
try:
    nlp.parse(args)
except IOError, err:
    print err
