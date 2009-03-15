#!/usr/bin/python

"""
ideer shell
"""

import os
import sys

from nlp import NLParser
import fs_shell
import job_shell
import storage_shell

subcmds = {
'storage': storage_shell.StorageController,
'fs': fs_shell.FSShell,
'job': job_shell.JobController,
}

if len(sys.argv) <= 1 or sys.argv[1] == 'help':
    print 'usage:', sys.argv[0], '|'.join(subcmds), 'action'
    sys.exit(-1)

cmd = sys.argv[1]
if cmd not in subcmds:
    print 'unknown sub cmd', cmd
    sys.exit(-1)

# Set dispatcher and rules for nlp
nlp = NLParser()
nlp.set_handler(subcmds[cmd]())
input = ' '.join(sys.argv[2:])
try:
    nlp.parse(input)
except IOError, err:
    print err.message
