#!/usr/bin/python
#coding: utf8

"""Simple test script"""


import os
import time

from tf import *

#-------------------------------FS Tests-----------------------------------

N = 1000
ish('fs rm -r /test-create')
ish('fs mkdir /test-create')
ish('fs cd /test-create')
for n in range(0, N):
    ish('fs touch f%d' % n)


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

basic fs operations

large size files
large number files
server crash
scale client number
"""
