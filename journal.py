"""Journal for in memory data structure

in-mem data, on-disk journal, and checkpoint files periodically generated
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
        
        self.cp_file = self.db.getpath('cp')
        self.journal_file = self.db.getpath('journal')
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

    def open_journal(self, id):
        self.journal = open(self.get_journal_path(id), 'a+')
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
            self.journal.close()
            self.journal_id += 1
            # Save the journal id, we can safely checkpoint or compact journals older than it
            self.open_journal(self.journal_id)
            self.journal_lock.release()

    def replay(self):
        pass
