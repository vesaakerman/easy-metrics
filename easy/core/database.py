#!/usr/bin/python
from __future__ import print_function, absolute_import

import re
from pymongo import MongoClient
from datetime import datetime
import logging

client = MongoClient()
filedb = client.get_database('file')
col = filedb.data

def metadata2mongo(fullpath, logging):
    file = open(fullpath, 'r')

    metadata = {}
    audience, coverage, title, rights, creator, format = [], [], [], [], [], []
    dataset_files = {}

    for lastline in file:
        item = re.search(r"(.+?)\=(.+)$", lastline)
        if item:
            try:
                if item.group(1):
                    metakey = str(item.group(1))
                    data = str(item.group(2))

                    if metakey in ('DATASET-PID', 'AMD:depositor', 'AMD:datasetState', 'EMD:dateCreated'):
                        metadata[metakey] = data
                    if metakey == 'EMD:dateSubmitted':
                        metadata[metakey] = datetime(int(data[:4]), int(data[5:7]), int(data[8:10]))
                    elif metakey == 'EMD:audience':
                        audience.append(data)
                    elif metakey == 'EMD:coverage':
                        coverage.append(data)
                    elif metakey == 'EMD:title':
                        title.append(data)
                    elif metakey == 'EMD:rights':
                        rights.append(data)
                    elif metakey == 'EMD:creator':
                        creator.append(data)
                    elif metakey == 'EMD:format':
                        format.append(data)
                    elif metakey.startswith('FILE['):
                        name = re.search(r".+\[(.+)\].+$", metakey).group(1)
                        if name:
                            if not dataset_files.has_key(name):
                                dataset_files[name] = {}
                            if metakey.endswith("PID"):
                                dataset_files[name]['pid']= data
                            elif metakey.endswith("size"):
                                dataset_files[name]['size']= data
                            elif metakey.endswith("mimeType"):
                                dataset_files[name]['mimeType']= data
                            elif metakey.endswith("creatorRole"):
                                dataset_files[name]['creatorRole']= data
                            elif metakey.endswith("accessibleTo"):
                                dataset_files[name]['accessibleTo']= data
                            elif metakey.endswith("visibleTo"):
                                dataset_files[name]['visibleTo'] = data
                        else:
                            logging.error("No filename found in item %s", metakey)
                else:
                    logging.error("in processing line %s" % lastline.rstrip())
            except:
                logging.error("in processing line %s" % lastline.rstrip())

    metadata['audience'] = audience
    metadata['coverage'] = coverage
    metadata['title'] = title
    metadata['rights'] = rights
    metadata['creator'] = creator
    metadata['format'] = format
    for file_name, file_data in dataset_files.iteritems():
        dataset_file2mongo(metadata['DATASET-PID'], metadata.get('EMD:dateSubmitted', None), file_name, file_data)

    return metadata

def dataset_file2mongo(dataset_pid, date_submitted, file_name, file_data):
    file_data['name'] = file_name
    if file_name.rfind('.') > 0:
        file_data['extension'] = file_name[file_name.rfind('.') + 1:]
    file_data['datasetPid'] = dataset_pid
    file_data['dateSubmitted'] = date_submitted
    try:
        col.insert_one(file_data)
    except:
        logging.error("in inserting file %s of dataset %s into 'file' database" % (file_name, dataset_pid))


def log_file2mongo(path, col, report):
    fullpath = path
    file = open(fullpath, 'r')
    outfile = open(report,'w')
    file.readline()

    for lastline in file:
        lastline = lastline[:-1]
        try:
            logging.info("inserting line %s of file %s " % (lastline, fullpath))
            col.insert_one(get_log_details(lastline, outfile))
        except:
            logging.error("in inserting line %s into 'logs' database" % lastline)

    outfile.close()

def get_log_details(line, outfile):
    search_results = {}
    parts = re.compile("\s+\;\s+").split(line)
    search_results['date'] = re.search(r"^(\d{4}-\d{2}-\d{2}).*", parts[0])
    search_results['type'] = re.search(r"^.+ - (.*).*", parts[0])
    search_results['user'] = re.search(r"(.*)", parts[1])
    search_results['roles'] = re.search(r".+\((.*)\).*", parts[2])
    search_results['groups'] = re.search(r".+\((.*)\).*", parts[3])
    search_results['ip'] = re.search(r"(.*)", parts[4])

    details = {}
    for k, v in search_results.iteritems():
        value = get_value(v)
        if k == 'date':
            details[k] = datetime(int(value[:4]), int(value[5:7]), int(value[8:10]))
        else:
            details[k] = value
    report = "ip: %s type: %s" % (parts[4], get_value(search_results['type']))

    if len(parts) >= 6:
        value = get_value(re.search(r".*DATASET_ID.*\"(.*)\".*", parts[5]))
        if value:
            details['dataset'] = value
            report += (" dataset: %s" % value)

    outfile.write(report + "\n")

    return details

def get_value(search_result):
    if search_result:
        if search_result.group(1):
            return search_result.group(1)
    return ""
