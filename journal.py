"""Journal for in memory data structure

in-mem data, on-disk journal, and checkpoint files periodically generated

Journal can only gurrantee ondisk data consistency, but can't protect
transaction loss

replay = cp + all the journals

cp: latest checkpoint file
journal_id: current using journal file

"""

import os
from logging import info, debug
import thread
import time

from io import FileDB

class Journal:
    def __init__(self, dir):
        """
        @dir   path to save journal and CP files
        """
        self.db = FileDB(dir)
        
        self.cp_name = self.db.load('cp')
        self.journal_id = self.db.load('journal_id')
        if not self.cp_name:
            self.init_cp() # Creat a new tree
        else:
            self.do_CP(self.journal_id + 1, True) # Do replay and checkpoint if journal exists

        self.record_id = 0
        self.journal_id = 0
        self.journal_lock = thread.allocate_lock()
        
        info('Start new journal')
        self.open_journal(self.journal_id)

        info('Start journal rollover thread')
        thread.start_new_thread(self.rollover, ())
        
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

    def checkpoint(self):
        """Checkpoint thread"""
        while True:
            time.sleep(60)
            self.do_CP(self.journal_id)

    def init_cp(self):
        cp = {'committed_journal_id': -1}
        self.save_cp(cp)

    def save_cp(self, cp):
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
