#!/usr/local/bin/python
from __future__ import print_function, absolute_import

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from os import walk
from easy.settings import *
from easy.core.database import *
from pprint import pprint

import logging
logging.basicConfig(filename='logs/importmetadata.log',format='%(asctime)s %(levelname)s  %(message)s', datefmt='%Y-%m-%d %H:%M:%S',level=logging.DEBUG)

path = "%s/../../tests" % HERE
#path = "%s/../../easyimports/" % HERE
metadatapath = "%s/metadata" % path
print("Importing metadata from %s" % metadatapath)
logging.info("Importing metadata from %s" % metadatapath)

client = MongoClient()
datasetdb = client.easy
col = datasetdb.dataset

f = []
for (dirpath, dirnames, filenames) in walk("%s" % metadatapath):
    pprint(filenames)
    f.extend(filenames)

for filename in f:
    filepath = "%s/metadata/%s" % (path, filename)
    logging.info("Processing %s" % filepath)
    print("Processing %s" % filepath)
    metadata = metadata2mongo(filepath, logging)
    if metadata:
        try:
            col.insert_one(metadata)
        except:
            logging.error("Error in inserting %s into 'dataset' database. Error: %s" % (path + "/" + filename, sys.exc_info()[0]))

print("Metadata imported")
logging.info("Metadata imported")

