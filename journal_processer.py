"""Journal processer for meta server"""

from logging import info, debug
from threading import Timer

from processer import Processer
from io import FileDB

class JournalProcesser(Processer):
    """Journal, start from id 0, and rollover every 300s, dir is the path to
    save journal files"""

    def __init__(self, dir):
        self.db = FileDB(dir)
        self.record_id = 0
        self.journal_id = 0
        self.rollover = False

        info('Open new journal')
        self.open_journal(self.journal_id)
        Processer.__init__(self)

    def rollover_handler(self):
        self.rollover = True

    def start(self):
        info('Starting journal rollover Timer')
        Timer(300, self.rollover_handler).start()
        Processer.start(self)

    def open_journal(self, id):
        self.journal = open(self.get_journal_path(id), 'a+')
        # Save the journal id, we can safely checkpoint or compact journals older than it
        self.db.store(id, 'journal_id')

    def get_journal_path(self, id):
        return self.db.getpath('journal.%d' % id)

    def processing(self, response):
        """ Journal the request operation of response
        # Rollover by journal size?
        if self.record_id % 10000 == 0:
            self.rollover()
        """
        if self.rollover:
            debug('Rolling over journal.%d', self.journal_id)
            self.journal.close() # Close old file
            self.journal_id += 1 # Increase id
            self.open_journal(self.journal_id) # Open new file
            self.rollover = False

        if 'error' not in response: # If error happens, do not journal
            req = response._req
            self.journal.write(str(req) + '\n')
            self.journal.flush()#Make sure all journals are written to the disk
            self.record_id += 1
        
        return response # Ready to send reponse back 
