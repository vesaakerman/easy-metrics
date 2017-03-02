#!/usr/bin/python
# coding: utf-8

# In[121]:

import re
from pymongo import MongoClient

client = MongoClient()
logsdb = client.get_database('management')
metadatadb = client.get_database('metadata')
logscol = logsdb.data
metacol = metadatadb.data

def get_metadata(ids):
    metapipe = { 'DATASET-PID' : {'$in' : ids} } 
    result = metacol.find( metapipe )
    meta = {}
    for x in result:
        try:
            meta[x['DATASET-PID']] = x['EMD:title']
#            print x['EMD:identifier']
        except:
            skip = x['DATASET-PID']
    return meta

def top_downloaded_files(limit):
#    pipe = [{'$match': { 'action': 'DOWNLOAD_FILE_REQUEST'}} , {'$group': {'_id': '$dataset(DATASET_ID', 'count' : { '$sum' : 1 }}}, { '$sort':{'count':-1} },  { '$limit' : limit }]
    pipe = [{'$match': { 'action': 'DOWNLOAD_FILE_REQUEST'}} , {'$group': {'_id': '$file(FILE_NAME(0)', 'count' : { '$sum' : 1 }}}, { '$sort':{'count':-1} },  { '$limit' : limit }]
    resultdata = logscol.aggregate(pipeline=pipe)
    return list(resultdata)

def deposited_files_by_user(limit):
    pipe = [{'$match': { 'action': 'FILE_DEPOSIT'}} , {'$group': {'_id': '$user', 'count' : { '$sum' : 1 }}}, { '$sort':{'count':-1} },  { '$limit': limit }]
    resultdata = logscol.aggregate(pipeline=pipe)
    return list(resultdata)

def most_downloaded_datasets(limit):
    pipe = [{'$group': {'_id': '$dataset(DATASET_ID', 'count' : { '$sum' : 1 }}}, { '$sort':{'count':-1} },  { '$limit' : limit }]
    resultdata = logscol.aggregate(pipeline=pipe)
    datasets = {}
    datalist = []
    copy = resultdata
    for dataset in resultdata:
        datalist.append(dataset['_id'])
        datasets[dataset['_id']] = dataset
    metadata = get_metadata(datalist)
    
    for dataset in datalist:
        try:
            datainfo = datasets[dataset]
            datainfo['title'] = metadata[dataset]
            datasets[dataset] = datainfo
        except:
            skip = 'yes'
    return datasets


# #### Top downloaded datasets
# Easy2 2Aoud: UC 15 (*) 
# For the DANS website: Titles of the most downloaded datasets from the entire collection
# 
# A download a dataset is defined as a download from a user from one computer one on one dataset one day or several times one or more files download. 
# 
# This definition is identical to the definition of a 'download' in EASY1.
# Files that are downloaded by a user more than once in a day, are counted more than eemaal.
# Downloads by users with archivist and / or admin role, are not counted.

# In[122]:

result = most_downloaded_datasets(10)
for line in result:
    print result[line]
#ids = ["easy-dataset:58245", "easy-dataset:44426"]
#get_metadata(ids)
#downloaded_files(5)


# #### Top downloaded files
# A download a file is defined as a download from a user from one computer one on one dataset one day or several times one or more files download. 
# 
# Files that are downloaded by a user more than once in a day, are counted more than eemaal.
# Downloads by users with archivist and / or admin role, are not counted.

# In[123]:

top_downloaded_files(10)


# #### Deposited files by user

# In[124]:

deposited_files_by_user(10)

