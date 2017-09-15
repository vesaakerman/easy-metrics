#!/usr/local/bin/python
from __future__ import print_function, absolute_import

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from os import listdir
from os.path import isfile, split
from easy.settings import *
from easy.core.database import *
import logging

INFO = 'info'
ERROR = 'error'
WARN = 'warn'

def log_message(level, message):
    if level == INFO:
        logging.info(message)
    elif level == ERROR:
        logging.error(message)
    elif level == WARN:
        logging.warn(message)
    print(message)


logging.basicConfig(filename='logs/importlogs.log',format='%(asctime)s %(levelname)s  %(message)s', datefmt='%Y-%m-%d %H:%M:%S',level=logging.DEBUG)
path = "%s/../../easyimports" % HERE
logs_path = "%s/logs" % path
reports_path = "%s/reports" % path
imported_logs_path = "%s/imported_logs" % logs_path

client = MongoClient()
easy_logs = client.easy
col = easy_logs.logs

log_message(INFO, "IMPORTING LOGS started at %s \n" % datetime.now())
log_message(INFO, "Importing logs from %s" % logs_path)

f = []
for filename in listdir(logs_path):
    if filename.startswith('statistics.log'):
        f.append(filename)

sorted_files = sorted(f)

imported_count = 0
for filename in sorted_files:
    file_path = "%s/%s" % (logs_path, filename)
    processed_file_path = "%s/%s" % (imported_logs_path, filename)
    report = "%s/%s.done" % (reports_path, filename)

    log_message(INFO, "Starting to parse %s " % filename)

    # check that the file has not already been imported
    if isfile(processed_file_path):
        log_message(ERROR, "%s has already earlier been imported into the database" % filename)
        log_message(ERROR, "LOGS IMPORT ABORTED")
        break

    log_file2mongo(file_path, col, report)
    imported_count += 1
    log_message(INFO, "%s imported into the database" % filename)
    # move to imported_logs folder
    os.rename(file_path, processed_file_path)
    log_message(INFO, "%s moved into %s" % (filename, split(imported_logs_path)[1]))
    log_message(INFO, "Finished parsing %s " % filename)


log_message(INFO, "Number of imported log files: %s" % imported_count)
log_message(INFO, "IMPORTING LOGS finished at %s \n" % datetime.now())
