"""OODict: object view of dict

Copyright (C) 2008-2009 Chen Zheng <nkchenz@gmail.com>
Distributed under terms of GPL v2
"""

class OODict(dict):
    """
    OODict
        OO style dict

    Examples:
    >>> a = OODict({'a': 1, 'c': {'d': 2}, 'b': 2})
    >>> a
    {'a': 1, 'c': {'d': 2}, 'b': 2}
    >>> a.a=0
    >>> a
    {'a': 0, 'c': {'d': 2}, 'b': 2}
    >>> a.e=0
    >>> a
    {'a': 0, 'c': {'d': 2}, 'b': 2, 'e': 0}
    >>> a.c = 5
    >>> a
    {'a': 0, 'c': 5, 'b': 2, 'e': 0}
    >>> a.f = OODict({'f':'f'}) 
    >>> a
    {'a': 0, 'c': 5, 'b': 2, 'e': 0, 'f': {'f': 'f'}}
    >>> a.f.f
    'f'
    >>> a.c = {'d': 2}
    >>> a
    {'a': 0, 'c': {'d': 2}, 'b': 2, 'e': 0, 'f': {'f': 'f'}}
    >>> a.c
    {'d': 2}
    >>> a.c.d
    2
    >>> a.c.e = {'e': 'e'}
    >>> a
    {'a': 0, 'c': {'e': {'e': 'e'}, 'd': 2}, 'b': 2, 'e': 0, 'f': {'f': 'f'}}
    >>> a.c.e.e
    'e'
    >>> a['c']['e'].e
    'e'
    


    Problems:
    
    * can't use del a.c, must use a['c']
    
    *If a.k is a dict, v is returned still as a dict in the following code: 
        for k, v in a.items():
            pass              # v is still a dict

    You can use like this instead:
        for k in a.keys():
            v = a[k]          # v is a OODict now

    Perhaps we should define our own 'items'.
    """
    def __init__(self, data = {}):
        dict.__init__(self, data)

    def __getitem__(self, key):
        value = dict.__getitem__(self, key)
        if isinstance(value, dict) and not isinstance(value, OODict):
            # Fixme! There maybe a problem here when value is a subclass of dict
            value = OODict(value)
            self[key] = value # Auto covert children dict to OODict 
        return value
        
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError, err:
            raise AttributeError(err)

    def __setattr__(self, key, value):
        self[key] = value


if __name__ == "__main__":
    import doctest
    doctest.testmod()

