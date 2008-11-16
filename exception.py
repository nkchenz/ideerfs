#!/usr/bin/python
# coding: utf8

# -- client
class ResponseError(IOError):
    pass

class NoMoreRetryError(IOError):
    pass

# -- server
class RequestHandleError(IOError):
    pass