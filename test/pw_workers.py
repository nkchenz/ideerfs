#!/usr/bin/python

from tf import *

# init
run('dd if=/dev/zero of=tmpdata.pw bs=1M count=33')

# run
ish_batch('''
fs rm a1
fs touch a1
fs stat a1
fs set pw_workers of a1 to 3
fs append tmpdata.pw a1
fs stat a1
fs restore a1 tmpdata.pw2
''')

#cleanup
#ish('fs rm a1')
#run('rm pw.tmp')
