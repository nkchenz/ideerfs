"""Checkpoint for in memory data structure

in-mem data, on-disk journal, and checkpoint files periodically generated

Journal can only gurrantee ondisk data consistency, but can't protect
transaction loss

algo:
replay = cp + all the journals
cp: name of latest checkpoint file 
journal_id: current using journal file id

pros: no need for lock, parallel with request handling
cons: double memory use, double request processing


another algo:
flush current data to disk directly
pros: no need to replay journals, only record largest checkpointed journal 
      id, and replay journals which are newer than that when restarting.
cons: need to acquire lock to protect data being written while checkpointing

"""

import os
from logging import info, debug
import thread
import time

from oodict import OODict
from io import FileDB

class CPProcesser:
    """CheckPoint Processer"""

    def __init__(self, dir):
        self.db = FileDB(dir)
        self.latest = self.db.load('cp')

    def set_handler(self, ops):
        self.handler= ops

    def start(self):
        info('Start checkpoint thread')
        thread.start_new_thread(self.checkpoint, ())

    def checkpoint(self):
        """Checkpoint thread"""
        while True:
            time.sleep(1800)
            self.do_CP()

    def get_cp_name(self):
        return 'cp.%s' % time.strftime('%Y%m%d%H%M%S')

    def get_journal_name(self, id):
        return 'journal.%d' % id

    def save_cp(self, cp):
        cpfile = self.get_cp_name()
        info('Creating new CP %s', cpfile)
        self.db.store(cp, cpfile, compress = True)
        self.db.store(cpfile, 'cp')
        self.cp = cp

    def do_CP(self, commit_all = False):
        cpfile = self.db.load('cp')
        if not cpfile:
            raise IOError('cp file not found')
        debug('Loading old CP %s', cpfile)
        cp = self.db.load(cpfile, compress = True)
        if not cp:
            raise IOError('%s corrupted' % cpfile)

        # Using old cp to init meta tree
        self.cp = cp
        self.handler.init_tree(self.cp)

        id = self.db.load('journal_id')
        if id is None: # It's possible that id is 0. Becareful!!!
            return # All clean, Nothing to replay

        debug('journal_id is %d, commit_all %s', id, commit_all)
        # Whether to checkpoint the last journal
        if commit_all:
            id += 1
        # Figure out next journal to checkpoint
        next = cp.committed_journal_id + 1
        if next >= id:
            return # Nothing to do

        # Replay  journals
        for jid in range(next, id):
            self.replay(jid)

        # Save new cp and name link
        cp = self.handler.get_tree()
        cp.committed_journal_id = jid
        if commit_all:
            cp.committed_journal_id = -1
        self.save_cp(cp)
       
        # Delete old journal files
        for jid in range(next, id):
            os.remove(self.db.getpath(self.get_journal_name(jid)))

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

