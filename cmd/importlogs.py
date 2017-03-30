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

logging.basicConfig(filename='logs/importlogs.log',format='%(asctime)s %(levelname)s  %(message)s', datefmt='%Y-%m-%d %H:%M:%S',level=logging.DEBUG)
path = "%s/../../tests" % HERE
logspath = "%s/logs" % path
print("Importing logs from %s" % logspath)
logging.info("Importing logs from %s" % logspath)

client = MongoClient()
easy_logs = client.get_database('logs')
col = easy_logs.data

f = []
for (dirpath, dirnames, filenames) in walk("%s" % logspath):
    pprint(filenames)
    f.extend(filenames)

for filename in f:
    fullpath = "%s/logs/%s" % (path, filename)
    report = "%s/reports/%s.done" % (path, filename)
    print(fullpath)
    log_file2mongo(fullpath, col, report)

print("Importing logs is finished.")
logging.info("Importing logs is finished.")
