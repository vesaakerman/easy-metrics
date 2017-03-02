#!/usr/bin/python
# coding: utf-8

# In[31]:

import re
from pymongo import MongoClient
from os import walk

path = "/Users/vyacheslavtykhonov/projects/management"

client = MongoClient()
metadatadb = client.get_database('metadata')
col = metadatadb.data

f = []
for (dirpath, dirnames, filenames) in walk("%s/metadata" % path):
    f.extend(filenames)


# In[32]:

def metadata2mongo(fullpath):
    #easy-dataset:998
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

for filename in f:
#filename = "easy-dataset:1006"
#if filename:
    metadata = metadata2mongo("%s/metadata/%s" % (path, filename))
    #print metadata
    if metadata:
        try:
            col.insert_one(metadata)
        except:
            skip = 'yes'

print "Done"

