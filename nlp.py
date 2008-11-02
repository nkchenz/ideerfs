#!/usr/bin/python
#coding: utf8

import re
from oodict import OODict
import sys

class NLParser:
    def __init__(self):
        self.CHARS = '.*?'
        self.VAR = '(.+?)'
        self.magic_ending = 'mNaLgP1c'
        self.rules = OODict()
        self.dispatcher = None

    def _parse(self, pattern, sentence):
        """
        patterns are like 'create $type on $dev of $host', items must be seperated
        with space. Words with leading $ are vars. The other literal words in pattern
        act as grammer sugar and seperaters of var.
                

        Return OODict, you can use something like
                d = parse(...)
                d.type, d.dev, d.host
        return values are all stripped.
        """
        # We need to know when to stop.
        sentence += self.magic_ending	
        values = OODict()
        vars = []
        regex = ''
        last_is_var = False # If two vars are consecutive, we need to use one space to seperate them 
        for item in pattern.split():
            if item[0] == '$':
                if last_is_var:
                        regex += ' '
                vars.append(item[1:])
                regex += self.VAR
                last_is_var = True
            else:
                if not last_is_var:
                        regex +='\s*?' # eat spaces of two adjacent literals
                regex += item 		
                last_is_var = False
        regex += self.magic_ending 
        #print regex, sentence
        r = re.match(regex, sentence)
        if not r:
            return None
        i = 0
        for var in vars:
            v = r.groups()[i].strip()
            #print v
            if not v:
                print '$%s is empty: %s' % (var, pattern)
                return None
            values[var] = v 
            i += 1	
        return values

    def parse(self, sentence):
        if sentence == 'help':
            import pprint
            pprint.pprint(self.rules)
            sys.exit(0)
            
        for name, pattern in self.rules.items():
            args = self._parse(pattern, sentence)
            if args is None:
                continue
            # Found
            op = getattr(self.dispatcher, name)
            if not op:
                print 'Operation not supported', name
                sys.exit(-1)
            op(args)
            sys.exit(0)
            
        print 'Sorry, cant understand:', sentence
        return -1

