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
from pprint import pprint 

path = "%s/../../tests" % HERE
logspath = "%s/logs" % path
print("Importing logs from %s" % logspath)

client = MongoClient()
management = client.get_database('management')
col = management.data

f = []
for (dirpath, dirnames, filenames) in walk("%s" % logspath):
    pprint(filenames)
    f.extend(filenames)

for filename in f:
    fullpath = "%s/logs/%s" % (path, filename)
    outpath = "%s/reports/%s.sum" % (path, filename)
    print(fullpath)
    file2mongo(fullpath, col)

print("Import is finished.")

