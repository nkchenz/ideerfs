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
        
        #if not os.path.exists(self.cp_files):
        #    raise IOError('CP file not found') # Fatal error

        # Load CP file

        #if os.path.exists(self.journal_file):
        #    # Replay journals
        #    pass
    

        self.record_id = 0
        self.journal_id = 0
        self.journal_lock = thread.allocate_lock()
        
        info('Start new journal')
        self.open_journal(self.journal_id)

        info('Start journal rollover thread')
        thread.start_new_thread(self.rollover, ())

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
       """
        self.journal_lock.acquire()
        self.journal.write(str(record) + '\n')
        self.journal.flush()#Make sure all journals are written to the disk
        self.journal_lock.release()
        
        self.record_id += 1
        # Rollover by journal size?
        #if self.record_id % 10000 == 0:
        #    self.rollover()
       
    def rollover(self):
        while True:
            time.sleep(600)
            self.journal_lock.acquire()
            debug('Rollover journal.%d', self.journal_id)
            self.journal.close() # Close old file
            self.journal_id += 1 # Increase id
            self.open_journal(self.journal_id) # Open new file
            self.journal_lock.release()

    def checkpoint(self):
        # Checkpoint thread
        pass

    def do_CP(self, journal_id):
        """Checkpoint journals equal or older than id"""
        # Read old cp
        #
        # Get committed journal id from cp file

        # cp.committed_journal_id

        if self.committed_journal_id < 0:
            next = 0
        else:
            next = self.committed_journal_id + 1

        if next == self.journal_id:
            return # Nothing to do

        # for id in range(next, journal_id):
        #   self.replay(id)

        # Save cp file
        # cp.committed_journal_id = self.journal_id - 1
        # self.db.store(cp_name, 'cp') # Save softlink file

        # Delete old journal files
        #
        pass

    def replay(self):
        pass
