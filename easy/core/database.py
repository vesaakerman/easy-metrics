#!/usr/bin/python
from __future__ import print_function, absolute_import

import sys
import re
from pymongo import MongoClient
from datetime import datetime
import logging

client = MongoClient()
filedb = client.easy
collection_file = filedb.file
collection_logs = filedb.logs

def metadata2mongo(fullpath, logging):
    file = open(fullpath, 'r')

    metadata = {}
    audience, coverage, title, rights, creator, format, type, subject = [], [], [], [], [], [], [], []
    dataset_files = {}

    for lastline in file:
        try:
            year_and_month = None
            if lastline.startswith('FILE['):
                metakey = lastline[: lastline.rindex("=")]
                data = lastline[lastline.rindex("=") + 1 :].rstrip()
            else:
                metakey = lastline[: lastline.index("=")]
                data = lastline[lastline.index("=") + 1 :].rstrip()

            if metakey == 'DATASET-PID':
                metadata["pid"] = data
            elif metakey == 'AMD:depositor':
                metadata["depositor"] = data
            elif metakey == 'AMD:datasetState':
                metadata["datasetState"] = data
            elif metakey == 'EMD:dateCreated':
                metadata["dateCreated"] = data
            elif metakey == 'EMD:dateAvailable':
                metadata["dateAvailable"] = data
            elif metakey == 'EMD:dateSubmitted':
                metadata["dateSubmitted"] = datetime(int(data[:4]), int(data[5:7]), int(data[8:10]))
            elif metakey == 'EMD:audience':
                audience.append(data)
            elif metakey == 'EMD:coverage':
                coverage.append(data)
            elif metakey == 'EMD:title':
                title.append(data)
            elif metakey == 'EMD:rights':
                if data in ['OPEN_ACCESS', 'OPEN_ACCESS_FOR_REGISTERED_USERS', 'GROUP_ACCESS', 'NO_ACCESS', 'ANONYMOUS_ACCESS', 'REQUEST_PERMISSION', 'ACCESS_ELSEWHERE', 'FREELY_AVAILABLE']:
                    if data == 'NO_ACCESS':
                        rights.append('CLOSED ACCESS')
                    else:
                        rights.append(data.replace('_', ' '))
            elif metakey == 'EMD:creator':
                creator.append(data)
            elif metakey == 'EMD:format':
                format.append(data)
            elif metakey == 'EMD:type':
                type.append(data)
            elif metakey == 'EMD:subject':
                subject.append(data)
            elif metakey.startswith('FILE['):
                name = re.search(r".+\[(.+)\].+$", metakey).group(1)
                if name:
                    if not dataset_files.has_key(name):
                        dataset_files[name] = {}
                    # 'pid' is excluded because it is not needed in the produced reports
                    # if metakey.endswith("PID"):
                    #     dataset_files[name]['pid'] = data
                    elif metakey.endswith("size"):
                        size = long(data)
                    elif metakey.endswith("mimeType"):
                        dataset_files[name]['mimeType'] = data.lower()
                    elif metakey.endswith("creatorRole"):
                        dataset_files[name]['creatorRole'] = data
                    # 'accessibleTo' and 'visibleTo' are excluded because they are not needed in the produced reports
                    # elif metakey.endswith("accessibleTo"):
                    #     dataset_files[name]['accessibleTo'] = data
                    # elif metakey.endswith("visibleTo"):
                    #     dataset_files[name]['visibleTo'] = data
                else:
                    logging.error("No filename found in item %s", metakey)
        except:
            logging.error("while processing line %s. Error: %s" % (lastline.rstrip(), sys.exc_info()[0]))

    metadata['audience'] = audience
    metadata['coverage'] = coverage
    metadata['title'] = title
    metadata['rights'] = rights
    metadata['creator'] = creator
    metadata['format'] = format
    metadata['type'] = type
    metadata['subject'] = subject
    nr_files = 0
    for file_name, file_data in dataset_files.iteritems():
        nr_files += 1
        # at this moment there are no queries where we would need more information about the files
        # dataset_file2mongo(metadata['pid'], metadata.get('dateSubmitted', None), file_name, file_data, size)
    metadata['files'] = nr_files

    # To be able to find in the logs collection also those datasets that don't have any DATASET_DEPOSIT or DATASET_PUBLISH events
    # we add a DATA_SUBMITTED event.
    if 'dateSubmitted' in metadata:
        dataset_submitted_event_2mongo(metadata['pid'], metadata['dateSubmitted'], metadata['audience'], metadata['files'])

    return metadata

def dataset_file2mongo(dataset_pid, date_submitted, file_name, file_data, size):

    # 'name' is excluded because it is not needed in the produced reports
    # file_data['name'] = file_name
    if file_name.rfind('.') > 0:
        file_data['extension'] = file_name[file_name.rfind('.') + 1:].lower()
    file_data['datasetPid'] = dataset_pid
    file_data['dateSubmitted'] = date_submitted
    try:
        # If a document with identical values is found, the count value of the document is increased by 1
        # and the size value is accumulated. Otherwise a new document is created.
        collection_file.find_one_and_update(file_data, {'$inc': {'count': 1, 'size': size}}, upsert=True)
    except:
        logging.error("in inserting file %s of dataset %s into 'file' database. Error: %s" % (file_name, dataset_pid, sys.exc_info()[0]))


def dataset_submitted_event_2mongo(dataset, date, discipline, nr_of_files):
    logging.info("Writing DATASET_SUBMITTED event to log file for dataset %s " % dataset)

    details = {}
    details['dataset'] = dataset
    details['discipline'] = discipline
    details['date'] = date
    details['type'] = 'DATASET_SUBMITTED'
    details['user'] = None
    details['roles'] = None
    details['groups'] = None
    details['ip'] = 'INSERTED BY THE IMPORT TOOL'
    if nr_of_files > 0:
        details['files'] = nr_of_files

    try:
        collection_logs.insert_one(details)
    except:
        logging.error("in inserting DATASET_SUBMITTED event for dataset %s into 'logs' database. Error: %s" % (dataset, sys.exc_info()[0]))


def log_file2mongo(path, col, report):
    fullpath = path
    file = open(fullpath, 'r')
    outfile = open(report,'w')
    file.readline()

    logging.info("Starting to parse file %s " % (fullpath))
    for lastline in file:
        lastline = lastline[:-1]
        try:
            nr_of_files = int(lastline.count("FILE_NAME"))
            log_details = get_log_details(lastline, nr_of_files, outfile)
            if log_details:
                logging.info("adding line %s of file %s " % (lastline, fullpath))
                # If a document with identical values is found, the count value of the document is increased by 1.
                # Otherwise a new document is created.
                nr_of_files = int(lastline.count("FILE_NAME"))
                # if nr_of_files > 0:
                #     col.find_one_and_update(get_log_details(lastline, outfile), {'$inc': { 'count' : 1, 'files' : nr_of_files}}, upsert=True)
                # else:
                #     col.find_one_and_update(get_log_details(lastline, outfile), {'$inc': {'count': 1}}, upsert=True)
                col.insert_one(log_details)
        except:
            logging.error("in inserting line %s into 'logs' database. Error: %s" % (lastline, sys.exc_info()[0]))
    logging.info("Finished parsing file %s " % (fullpath))

    outfile.close()

def get_log_details(line, nr_of_files, outfile):
    details = {}
    parts = re.compile("\s+\;\s+").split(line)

    date = get_value(re.search(r"^(\d{4}-\d{2}-\d{2}).*", parts[0]))
    details['date'] = datetime(int(date[:4]), int(date[5:7]), int(date[8:10]))
    details['type'] = get_value(re.search(r"^.+ - (.*).*", parts[0]))
    details['user'] = get_value(re.search(r"(.*)", parts[1]))
    details['roles'] = get_value(re.search(r".+\((.*)\).*", parts[2]))
    details['groups'] = get_value(re.search(r".+\((.*)\).*", parts[3]))
    details['ip'] = get_value(re.search(r"(.*)", parts[4]))
    if nr_of_files > 0:
        details['files'] = nr_of_files

    if details['type'] in ('DATASET_DEPOSIT', 'DATASET_PUBLISHED', 'DOWNLOAD_DATASET_REQUEST', 'DOWNLOAD_FILE_REQUEST', 'FILE_DEPOSIT'):
        details['dataset'] = get_value(re.search(r".*DATASET_ID.*\"(.*)\".*", parts[5]))
        if details['type'] in ('DATASET_VIEWED', 'DATASET_PUBLISHED', 'DOWNLOAD_DATASET_REQUEST', 'DOWNLOAD_FILE_REQUEST'):
            details['discipline'] = get_value(re.search(r".*SUB_DISCIPLINE_LABEL\: *\"([a-zA-Z \-\(\)\,]*)\".*", line))

        report = "type: %s date: %s user: %s roles: %s ip: %s" % (details['type'], details['date'], details['user'], details['roles'], parts[4])
        if details.get('dataset', None):
            report += (" dataset: %s" % details['dataset'])
        if details.get('discipline', None):
            report += (" discipline: %s" % details['discipline'])
        outfile.write(report + "\n")
        return details
    else:
        return None


def get_value(search_result):
    if search_result:
        if search_result.group(1):
            return search_result.group(1)
    return ""
