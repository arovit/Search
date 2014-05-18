#!/usr/bin/python


__Author__ = "Arovit Narula <arovit.kv@gmail.com>"  


""" 
Input: 

1. File containing lines with format
<spaces separated strings> 
2. Search strings - Interactive mode 

Output:

Lines of input file which contains the input strings.

Usage:

<script> --datafile/-f <path_to_file>

This will create datafiles in the datastore directory '<input_file_name>.datastore.db' in the same directory as the input file
The datastore is created only when it is not existing (Eg. First time when it is run or when it is deleted) 

When the datastore is not present it is again created, else it will use the existing datastore to search the strings 
Use -b to again forcefully rebuild the datastores.

Multiple datastores can be created for different input files

Algorithm: 

Trie data structures are known to be efficent data structures for prefixes and dictionary matches.
The time complexity of the retrival is Order(length of the string) * the number of datastore files. It comes with a cost of creating the trie.

The large input file is divided into smaller trie trees and stored in datafiles.
The search is done using these files by loading up the trie from files one by one in memory and retrieving from it.

"""
import os
import sys
import time
import shelve                      #  Use a data persistence library for storing dictinary
import logging 
from optparse import OptionParser
               
FINISH_MARKER = "_"                # Marker to mark end of strings in trie

def generate_substrings(strng):
    """ Return all the possible substrings for a input string """
    temp = [] 
    for start in range(0, len(strng)):
        for end in range(start,len(strng)):
            temp.append(strng[start: end+1])
    return temp

def make_trie(words, parent_string, file_db):
    """ Make the trie tree - throw words into the trie to build larger trie """
    for word in words:
        current = file_db
        for char in word:
            current = current.setdefault(char, {})
        current = current.setdefault(FINISH_MARKER, set())
        current.add(parent_string)

def search_trie(word, current):
    """ Searching word from a trie """
    for letter in word:
        if letter in current:
            current = current[letter]
        else:
            return []
    else:
        if FINISH_MARKER in current:
            return current[FINISH_MARKER]
        else:
            return []


class ParseStore:       			 # Old classes are sufficient
    """ Parse and store the results in a Trie for easy retrieval """
    def __init__(self, file):
        self.file = file
        # Datastore file in the same directory as source text file with extension .datastore.db
        self.datastores = []
        self.datastore_directory = os.path.join(os.path.dirname(os.path.abspath(file)),\
                         os.path.basename(file) + '.datastore.db')

    def manage_datastore(self, force_rebuild):
        """ Construct datastore """
        self.initialize_datastore() 
        if force_rebuild:
            for i in self.datastores:
                if os.path.exists(i):
                    os.unlink(i)
            self.datastores = []

        if not self.datastores:
            self.parse_and_insert()

    def initialize_datastore(self):
        """ Initialize datastore - Look where to search """
        if not os.path.exists(self.datastore_directory):
            os.makedirs(self.datastore_directory)
        for i in os.listdir(self.datastore_directory):
            self.datastores.append(os.path.join(self.datastore_directory, i))
        
    def parse_and_insert(self):
        """ Parse the file and insert the substring into trie tree and save the tree in datastore """  
        logger.info("Populating the datastore for faster retrieval. Please wait .")
        fp = open(self.file, 'r')    # Open the input file and parse it 
        count = 0
        temp_dict = dict()
        for line in fp:      			       # This is a lazy read 
            line = line.strip()
            if not line:                               # Empty line - continue             
                continue
            if count and (count % 100) == 0:            # For every 100 lines , flush to disk
                dsfile = os.path.join(self.datastore_directory , str(count)+'.db')
                file_db = shelve.open(dsfile,  writeback=True)
                if (count % 5000) == 0: 
                    logger.info("Datastore completed for %sk.."%(count/1000))
                for k in temp_dict.keys():
                    file_db[k] = temp_dict[k]
                file_db.close()
                self.datastores.append(dsfile)
                del temp_dict
                temp_dict = dict()
            try:
                strings = line.split(' ')
            except Exception, e: 
                logger.critical("Error while parsing line '%s', Skipping this line.."%line)
                continue 
            for strng in strings:  
                fsubs = generate_substrings(strng)
                t1 = time.time()
                make_trie(fsubs, line, temp_dict)
                t2 = time.time()
                count = count + 1

        dsfile = os.path.join(self.datastore_directory , str(count)+'.db')
        file_db = shelve.open(dsfile,  writeback=True)
        for k in temp_dict.keys():
            file_db[k] = temp_dict[k]
        file_db.close()
        self.datastores.append(dsfile)
        fp.close()

    def search(self, search_str):
        """ Search the datastore for the input keyword """
        t1 = time.time()
        print '-'*10
        logger.info("Keyword : %s"%search_str)
        result_set = set()
        for i in self.datastores:
            file_db = shelve.open(i)
            result_set.update(search_trie(search_str, file_db))
        t2 = time.time()
        search_time = t2 - t1
        if not result_set:
            logger.info("No results found (%.4s secs)"%search_time)
            return
        logger.info("Results (%.4s seconds): %s\n%s"%(search_time, len(result_set), '\n'.join(list(result_set))))

def parse_options():
    usage = "Usage: %prog [options] <search_word1> <searchword2> <search_word3> <search word4> ... n"
    parser = OptionParser(usage)
    parser.add_option("-f", "--datafile", dest="filename",
                  help="input file with data", metavar="FILE")

    parser.add_option("-b", "--rebuild", action="store_true", default=False, dest="rebuild",
                  help="Force rebuild datastore for the file" )

    (options, args) = parser.parse_args()
    if not options.filename:
        logger.critical("--datafile is required option")
        parser.print_help() 
        sys.exit(1)
    return options, args

if __name__ == "__main__":
    global logger
    logging.basicConfig(format='[%(levelname)s]:%(message)s', level=logging.DEBUG)
    logger = logging
    (options, args) = parse_options() 
    dataobj = ParseStore(options.filename)
    dataobj.manage_datastore(options.rebuild)
    for kws in args: 
        dataobj.search(kws)
    while True:
        search_str = raw_input("\n > Enter the search phrase (Press '.' do stop) : ")
        if search_str.strip() == '.':
            break
        dataobj.search(search_str)
