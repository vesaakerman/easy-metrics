#!/usr/bin/python
# coding: utf-8

# In[67]:

import re
from pymongo import MongoClient
from os import walk

path = "/Users/vyacheslavtykhonov/projects/management"
#testfilename = "statistics.log.2016-09-26"

client = MongoClient()
management = client.get_database('management')
col = management.data

f = []
for (dirpath, dirnames, filenames) in walk("%s/logs" % path):
    f.extend(filenames)

for filename in f:
    fullpath = "%s/logs/%s" % (path, filename)
    outpath = "%s/reports/%s.sum" % (path, filename)
    print fullpath
    file2mongo(fullpath)


# In[68]:

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



# In[69]:

def file2mongo(path):
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
    
        if (len(l) > 20000):
            print str(l)
    
        (main, newline) = cleanline(lastline)
        col.insert_one(main)
        outfile.write(report)
    outfile.close()


# In[70]:

print "Done"

