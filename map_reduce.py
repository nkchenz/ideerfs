#!/usr/bin/python
# coding: utf8

"""
 hadoop@ubuntu:/usr/local/hadoop$ bin/hadoop jar contrib/streaming/hadoop-0.18.0
 -streaming.jar -mapper /home/hadoop/mapper.py -reducer /home/hadoop/reducer.py 
 -input gutenberg/* -output gutenberg-output
Maper:
#!/usr/bin/env python
#A more advanced Mapper, using Python iterators and generators.
 
import sys
 
def read_input(file):
    for line in file:
        # split the line into words
        yield line.split()
 
def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_input(sys.stdin)
    for words in data:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        for word in words:
            print '%s%s%d' % (word, separator, 1)
 
if __name__ == "__main__":
    main()


Reducer:
#!/usr/bin/env python
#A more advanced Reducer, using Python iterators and generators.
 
from itertools import groupby
from operator import itemgetter
import sys
 
def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)
 
def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
    # groupby groups multiple word-count pairs by word,
    # and creates an iterator that returns consecutive keys and their group:
    #   current_word - string containing a word (the key)
    #   group - iterator yielding all ["<current_word>", "<count>"] items
    for current_word, group in groupby(data, itemgetter(0)):
        try:
            total_count = sum(int(count) for current_word, count in group)
            print "%s%s%d" % (current_word, separator, total_count)
        except ValueError:
            # count was not a number, so silently discard this item
            pass
 
if __name__ == "__main__":
    main()
    
"""