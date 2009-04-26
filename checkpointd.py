"""Checkpoint for in memory data structure

In-mem data, on-disk journal, and checkpoint files periodically generated.
Journal can only gurrantee ondisk data consistency, but can't protect
transaction loss

journal_id: journal file id being used currently
last_cp: symbol link to the last CP file

algo
replay:  CP + all the journals except the current one
pros: no need for lock, parallel with request handling
cons: double memory use, double request processing

algo2
flush current data to disk directly
pros: no need to replay journals, only record largest checkpointed journal 
      id, and replay journals which are newer than that when restarting.
cons: need to acquire lock to protect data being written while checkpointing

algo3
COW based snapshot
"""

import os
from logging import info, debug
import thread
import time

from oodict import OODict
from io import FileDB

class CheckPointD:
    """CheckPoint Processer"""

    def __init__(self, dir):
        self.db = FileDB(dir)
        self.last_cp_file = 'last_cp'

    def start(self):
        self.do_CP(True) # Do a initial CP
        info('Start checkpoint thread')
        thread.start_new_thread(self.mainloop, ())

    def mainloop(self):
        """Checkpoint thread"""
        while True:
            time.sleep(1800)
            self.do_CP()

    def get_cp_name(self):
        return 'cp.%s' % time.strftime('%Y%m%d%H%M%S')

    def get_journal_name(self, id):
        return 'journal.%d' % id

    def load_cp(self):
        return self.db.load(self.last_cp_file, compress = True)

    def save_cp(self, cp):
        f = self.get_cp_name()
        info('Creating new CP %s', f)
        self.db.store(cp, f, compress = True)
        self.db.link(f, self.last_cp_file, True)

    def do_CP(self, commit_all = False):
        """If last_cp doesn't exist, try to replay the remain journals. If no
        journals are found, do nothing. If there are replayed journals, create
        a CP and remove the journals.

        Should we raise exception when last_cp is missing?
        """

        journal_id = self.db.load('journal_id') # The journal which might being written currently
        if journal_id is None: # It's possible that id is 0. Becareful!!!
            return # All clean, Nothing to replay

        # Load meta image from last_cp
        old_cp = self.load_cp()
        self.handler._load_image(old_cp)

        # Whether to replay the last journal
        if commit_all:
            journal_id += 1

        # Figure out next journal to checkpoint
        if old_cp:
            next = old_cp.committed_journal_id + 1
        else:
            next = 0
        if next >= journal_id:
            return # Nothing to do

        # Replay  journals
        for jid in range(next, journal_id):
            self.replay(jid)

        # Save new cp and name link
        new_cp = self.handler._store_image()
        new_cp.committed_journal_id = jid
        if commit_all:
            new_cp.committed_journal_id = -1
        self.save_cp(new_cp)
       
        # Delete old journal files
        for jid in range(next, journal_id):
            self.db.remove(self.get_journal_name(jid))

    def replay(self, id):
        name = self.get_journal_name(id)
        f = self.db.getpath(name)
        if not os.path.exists(f):
            raise IOError('%s missing' % f)
        info('Replaying %s', name)
        fp = open(f, 'r')
        for line in fp.readlines():
            req = OODict(eval(line))
            service, method = req.method.split('.')
            getattr(self.handler, method)(req)
        fp.close()

