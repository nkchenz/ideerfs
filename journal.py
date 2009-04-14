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

from io import FileDB


class CheckPointer:

    def __init__(self, dir):
        """@dir   path to save journal and CP files """
        self.db = FileDB(dir)
        self.latest = self.db.load('cp')

    def set_handler(self, ops):
        self.handler= ops

    def load_cp(self):
        # Do replay and checkpoint all the journals
        self.do_CP(self.journal_id + 1, True)

        self.record_id = 0
        self.journal_id = 0
        # Lock for journal rollover
        self.journal_lock = thread.allocate_lock()
        
        info('Start new journal')
        self.open_journal(self.journal_id)
        info('Start journal rollover thread')
        thread.start_new_thread(self.rollover, ())

    def start(self):
        info('Start checkpoint thread')
        thread.start_new_thread(self.checkpoint, ())

    def get_cp_name(self):
        return 'cp.%s' % time.strftime('%Y%m%d%H%M%S')

    def open_journal(self, id):
        self.journal = open(self.get_journal_path(id), 'a+')
        # Save the journal id, we can safely checkpoint or compact journals older than it
        self.db.store(id, 'journal_id')

    def get_journal_path(self, id):
        return self.db.getpath(self.get_journal_name(id))

    def get_journal_name(self, id):

    def checkpoint(self):
        """Checkpoint thread"""
        while True:
            time.sleep(60)
            self.do_CP(self.journal_id)

    def save_cp(self, cp):
        self.cp = cp
        cpfile = self.get_cp_name()
        self.db.store(cp, cpfile)
        self.db.store(cpfile, 'cp')
        info('New checkpoint %s', cpfile)

    def do_CP(self, id, clear_committed = False):
        """Checkpoint journals older than id"""
        # Read old cp
        cpfile = self.db.load('cp')
        if not cpfile:
            raise IOError('cp file not found')
        cp = self.db.load(cpfile)
        if not cp:
            raise IOError('%s corrupted' % cpfile)

        # Figure out next journal to checkpoint
        next = cp.committed_journal_id + 1
        debug('do CP older than journal.%d, next is %d', id, next)
        if next >= id:
            return # Nothing to do

        # Replay  journals
        for id in range(next, id):
            self.replay(id)

        # Save new cp and name link
        cp.committed_journal_id = id
        if clear_committed:
            cp.committed_journal_id = -1
        self.save_cp(cp)
       
        # Delete old journal files
        for id in range(next, id + 1):
            os.remove(self.get_journal_path(id))

    def replay(self, id):
        f = self.get_journal_path(id)
        if not os.path.exists(f):
            raise IOError('%s missing' % f)
        info('Replay %s', self.get_journal_name(id))


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
            time.sleep(10)
            self.journal_lock.acquire()
            debug('Rollover journal.%d', self.journal_id)
            self.journal.close() # Close old file
            self.journal_id += 1 # Increase id
            self.open_journal(self.journal_id) # Open new file
            self.journal_lock.release()

