"""Base service class """

from exception import *

class Service:
    """Base class for chunk, meta, storage services"""

    def _error(self, message):
        """Some thing bad happens while processing request, exit with given error message"""
        raise RequestHandleError(message)
    
