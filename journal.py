"""Journal for in memory data structure

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

class CheckPoint:

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
        self.db.store(cp, cpfile)
        self.db.store(cpfile, 'cp')
        info('New CP %s created', cpfile)
        self.cp = cp

    def do_CP(self, commit_all = False):
        # Read old cp
        cpfile = self.db.load('cp')
        if not cpfile:
            raise IOError('cp file not found')
        cp = self.db.load(cpfile)
        if not cp:
            raise IOError('%s corrupted' % cpfile)

        # Using old cp to init meta tree
        self.cp = cp
        self.handler.init_tree(self.cp)

        id = self.db.load('journal_id')
        if id is None: # It's possible that id is 0. Becareful!!!
            return # All clean, Nothing to replay

        debug('Do CP, journal_id is %d, commit_all %s', id, commit_all)
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
        info('Replay %s', name)
        fp = open(f, 'r')
        for line in fp.readlines():
            req = OODict(eval(line))
            service, method = req.method.split('.')
            getattr(self.handler, method)(req)
        fp.close()


class Journal:
    """Journal, start from id 0, and rollover every 300s"""
    def __init__(self, dir):
        """
        @dir   path to save journal and CP files
        """
        self.db = FileDB(dir)
        self.record_id = 0
        self.journal_id = 0
        # Lock for journal rollover
        self.journal_lock = thread.allocate_lock()
        
        info('Start new journal')
        self.open_journal(self.journal_id)
        info('Start journal rollover thread')
        thread.start_new_thread(self.rollover, ())

    def open_journal(self, id):
        self.journal = open(self.get_journal_path(id), 'a+')
        # Save the journal id, we can safely checkpoint or compact journals older than it
        self.db.store(id, 'journal_id')

    def get_journal_path(self, id):
        return self.db.getpath(self.get_journal_name(id))

    def get_journal_name(self, id):
        return 'journal.%d' % id
            
    def append(self, record):
        """Each request is handled by a thread, so we must get a lock here
        Perhaps we should use only one thread to process requests, the worker-queue model

        # Rollover by journal size?
        if self.record_id % 10000 == 0:
            self.rollover()
        """
        self.journal_lock.acquire()
        self.journal.write(str(record) + '\n')
        self.journal.flush()#Make sure all journals are written to the disk
        self.journal_lock.release()
        
        self.record_id += 1
        
    def rollover(self):
        while True:
            time.sleep(300)
            self.journal_lock.acquire()
            debug('Rollover journal.%d', self.journal_id)
            self.journal.close() # Close old file
            self.journal_id += 1 # Increase id
            self.open_journal(self.journal_id) # Open new file
            self.journal_lock.release()

