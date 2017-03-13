#!/usr/bin/python
from __future__ import print_function, absolute_import

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

import re
from os import walk
from pymongo import MongoClient
from easy.settings import *
from easy.core.database import *

path = "%s/../../tests" % HERE
metadatapath = "%s/metadata" % path
print("Importing metadata from %s" % metadatapath)

client = MongoClient()
metadatadb = client.get_database('metadata')
col = metadatadb.data

f = []
for (dirpath, dirnames, filenames) in walk("%s" % metadatapath):
    f.extend(filenames)

for filename in f:
    metadata = metadata2mongo("%s/metadata/%s" % (path, filename))
    if metadata:
        try:
            col.insert_one(metadata)
        except:
            skip = 'yes'

print("Metadata imported")

