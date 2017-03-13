#!/usr/bin/python
from __future__ import print_function, absolute_import

import sys
import os
import re
from pymongo import MongoClient

def metadata2mongo(fullpath):
    file = open(fullpath, 'r')

    metadata = {}
    for lastline in file:
        #lastline = lastline[:-2]
        item = re.search(r"(.+?)\=(.+)$", lastline)
        if item:
            try:
                if item.group(1):
                    metakey = str(item.group(1))
                    metakey = metakey.replace('.', ' ')
                    metadata[metakey] = str(item.group(2))
            except:
                skip = item
    return metadata

def cleanline(thisline):
    l = re.compile("\s+\;\s+").split(thisline)
    maininfo = {}
    for i in range(0,5):
#        print l[i]
        maininfo[str(i)] = l[i]
    
    # date, time and action
    attributes = re.search(r"(\d{4}\-\d+\-\d+)\s+(\S+)\,\d+\s+\-\s+(.+)", maininfo["0"])
    if attributes:
        maininfo["date"] = attributes.group(1)
        maininfo["time"] = attributes.group(2)
        maininfo["action"] = attributes.group(3)
        maininfo["user"] = maininfo["1"]
        
    # datetime - action; user; roles; groups; ip
    l = l[5:]
    
    # dataset description
    for item in l:
        try:
            info = re.compile("\:\s+").split(item)
            info[1] = info[1].replace('"', '')
            info[1] = re.sub(r'\)$', '', info[1])
            maininfo[info[0]] = info[1]
            #print "%s = %s" % (info[0], info[1])
        except:
            skip = item
    
    return (maininfo, l)

def file2mongo(path, col):
    fullpath = path
    outpath = "%s.done" % path
    file = open(fullpath, 'r')
    outfile = open(outpath,'w')
    lastline = file.readline()

    intstats = {}
    for lastline in file:
        lastline = lastline[:-2]
        l = re.compile("\s+\;\s+").split(lastline)
        try:
            intstats[str(len(l))] = intstats[str(len(l))] + 1
        except:
            intstats[str(len(l))] = 1
    
        # Try to recognize datasets
        finddataset = re.search(r"(easy\-dataset\:(\d+))", lastline) 
        dataset = '0'
        if finddataset:
            dataset = finddataset.group(0)
        report = "%s %s %s\n" % (str(l[4]), str(len(l)), dataset)
    
#        if (len(l) > 20000):
#            print str(l)
    
        (main, newline) = cleanline(lastline)
        col.insert_one(main)
        outfile.write(report)
    outfile.close()

