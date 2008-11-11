# -- client
class ResponseError(IOError):
    pass

class NoMoreRetryError(IOError):
    pass

# -- server
class RequestHandleError(IOError):
    pass