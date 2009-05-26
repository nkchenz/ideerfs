#!/usr/bin/python

"""ideer shell """

import os
import sys
import logging

from nlp import NLParser
import fs_shell
import job_shell
import storage_shell
import config
from util import *


controller = {
'storage': storage_shell.StorageShell,
'fs': fs_shell.FSShell,
'job': job_shell.JobShell,
}

if len(sys.argv) <= 1 or sys.argv[1] == 'help':
    print 'Usage:', sys.argv[0], '|'.join(controller.keys()), 'action'
    sys.exit(-1)

cmd = sys.argv[1]
if cmd not in controller:
    print 'Unknown command', cmd
    sys.exit(-1)

# Set client debug log
init_logging(os.path.join(config.home, 'ideer.client.log'))
logging.getLogger('').setLevel(logging.DEBUG)

# Set dispatcher and rules for nlp
nlp = NLParser()
nlp.set_handler(controller[cmd]())
args = ' '.join(sys.argv[2:])
try:
    nlp.parse(args)
except IOError, err:
    print err
