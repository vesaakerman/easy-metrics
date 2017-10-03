#!/usr/local/bin/python
from __future__ import print_function, absolute_import

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from os import listdir
from os.path import split
import fnmatch
from easy.settings import *
from easy.core.database import *
from datetime import datetime
from glob import glob
import tarfile
import shutil
import logging

def by_dataset_number(file_name):
  number_as_string = re.findall(r"\d+", file_name)
  return map(int, number_as_string)


def get_tar_file_path(directory):
    for file in os.listdir(directory):
        if fnmatch.fnmatch(file, '*.tgz'):
            return directory + "/" + file


def extract_datasets(tar_file, directory):
    tar = tarfile.open(tar_file)
    tar.extractall(directory)
    tar.close()

def add_dataset_submitted_log_event(metadata, filename):
    if 'dateSubmitted' in metadata:
        submit_event_added = dataset_submitted_event_2mongo(metadata['pid'], metadata['dateSubmitted'], metadata['audience'], metadata['files'])
    else:
        return True

    if submit_event_added:
        return True
    else:
        log_message(ERROR, "Error in adding DATASET_SUBMITTED event for dataset %s" % filename)
        return False

# ====================================================================
# MAIN

logging.basicConfig(filename='logs/importdatasets.log',format='%(asctime)s %(levelname)s  %(message)s', datefmt='%Y-%m-%d %H:%M:%S',level=logging.DEBUG)
path = "%s/../../easyimports" % HERE
datasets_path = "%s/datasets" % path
datasets_to_import_path = "%s/datasets_to_import" % datasets_path
imported_datasets_path = "%s/imported_datasets" % datasets_path

client = MongoClient()
datasetdb = client.easy
col = datasetdb.dataset

log_message(INFO, "IMPORTING DATASETS started at %s \n" % datetime.now())
log_message(INFO, "Importing metadata from %s" % datasets_to_import_path)

# check that there is exactly one tar-file
tar_files = len(glob(datasets_path + '/*.tgz'))
if tar_files <> 1:
    if tar_files > 1:
        log_message(ERROR, "There are more than one .tgz file")
    elif tar_files < 1:
        log_message(ERROR, "There is no .tgz file")
    log_message(ERROR, "DATASET IMPORT ABORTED")
    sys.exit(1)

# first remove datasets_to_import directory and then extract tar-file into datasets_to_import directory
shutil.rmtree(datasets_to_import_path, ignore_errors=True)
tar_file = get_tar_file_path(datasets_path)
log_message(INFO, "Extracting %s to %s" % (tar_file, datasets_to_import_path))
extract_datasets(tar_file, datasets_to_import_path)
log_message(INFO, "Extracted %s to %s" % (tar_file, datasets_to_import_path))

f = []
for filename in listdir(datasets_to_import_path):
    if filename.startswith('easy-dataset'):
        f.append(filename)

# sort by dataset number
sorted_files = sorted(f, key=by_dataset_number)

last_imported_dataset = get_last_imported_dataset_number()
first_dataset_imported = False
imported_count = 0

# process only those datasets whose number is bigger than the last_imported_dataset (stored in the database from the previous import)
for filename in sorted_files:
    dataset_number = int(re.findall(r"\d+", filename)[0])
    if dataset_number > last_imported_dataset:

        file_path = "%s/%s" % (datasets_to_import_path, filename)
        processed_file_path = "%s/%s" % (imported_datasets_path, filename)
        log_message(INFO, "Processing %s" % filename)

        metadata = metadata2mongo(file_path, logging)
        if metadata:
            try:
                col.insert_one(metadata)
                log_message(INFO, "%s imported into the database" % filename)
                if not first_dataset_imported:
                    first_dataset_imported = True
                    log_message(INFO, "First dataset imported: %s" % filename)
                update_last_imported_dataset_number(dataset_number)
                imported_count += 1
                os.rename(file_path, processed_file_path)
                log_message(INFO, "%s moved into %s" % (filename, split(imported_datasets_path)[1]))

                # To be able to find in the logs collection also those datasets that don't have any DATASET_DEPOSIT or DATASET_PUBLISH events
                # we add a DATA_SUBMITTED event.
                if not add_dataset_submitted_log_event(metadata, filename):
                    log_message(ERROR, "DATASET IMPORT ABORTED")
                    break
            except:
                log_message(ERROR, "Error in inserting %s into database. Error: %s" % (filename, sys.exc_value))
                log_message(ERROR, "DATASET IMPORT ABORTED")
                break
        else:
            log_message(ERROR, "Error in processing metadata of dataset %s" % filename)
            log_message(ERROR, "DATASET IMPORT ABORTED")
            break

# remove .tgz file
if imported_count > 0:
    os.remove(tar_file)

log_message(INFO, "Number of imported datasets: %s" % imported_count)
log_message(INFO, "IMPORTING DATASETS finished at %s \n" % datetime.now())

