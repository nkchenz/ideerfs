"""
Base service class
"""

import os
import hashlib

from exception import *

class Service:
    """Base class for chunk, meta, storage services"""

    def _error(self, message):
        """Some thing bad happens while processing request, exit with given error message"""
        raise RequestHandleError(message)
    
    def _hash2path(self, hash):
        return os.path.join(hash[:3], hash[3:6], hash[6:])
    
    def _id2path(self, id):
        """Map object id number to storage path, this can be changed to other methods"""
        hash = hashlib.sha1(str(id)).hexdigest()
        return self._hash2path(hash)

