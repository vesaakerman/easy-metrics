#!/usr/local/bin/python
# This script reads a sql-dump where all log-events of Easy-1 are found
# and converts each dump-item into Easy-2 compatible log-event

from __future__ import print_function, absolute_import

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from easy.settings import *
from easy.core.database import *
import logging

stats_events = {
'1': 'START_PAGE_VISIT',
'2': 'USER_URL_REQUEST',
'3': 'REFERRER_URL',
'4': 'USER_LOGIN',
'5': 'USER_LOGOUT',
'6': 'BROWSE',
'7': 'DOWNLOAD_DATASET_REQUEST',
'8': 'DOWNLOAD_FILE_REQUEST',
'9': 'SEARCH_TERM',
'10': 'ADVANCED_SEARCH_TERM',
'11': 'SEARCHENGINE_TERM',
'12': 'DATASET_VIEWED',
'13': 'DATASET_DEPOSIT',
'14': 'DATASET_PUBLISHED',
'15': 'FILE_DEPOSIT',
'16': 'USER_REGISTRATION'
}

logging.basicConfig(filename='logs/importlogs.log',format='%(asctime)s %(levelname)s  %(message)s', datefmt='%Y-%m-%d %H:%M:%S',level=logging.DEBUG)
path = "%s/../../easyimports" % HERE
logs_path = "%s/logs" % path
sql_dump_file =  "%s/stats_easy1.sql" % logs_path
log_event_file =  "%s/statistics.log.easy1" % logs_path

log_message(INFO, "%s STARTED Creating Easy2 compatible log-event file from sql-dump file %s\n" % (datetime.now(), sql_dump_file))

file = open(sql_dump_file, 'r')
outfile = open(log_event_file, 'w')

for line in file:
    if line.startswith("INSERT INTO `easy_stats` VALUES"):
        events = line.split('),')
        events[0]=events[0][32:]
        for event in events:
            items = re.search('.(\d*).*(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\',\'(.*)\',\'(.*)\',(\d*),(.*)', event)
            id = items.group(1)
            timestamp = items.group(2)
            username = items.group(3)
            ip_address = items.group(4)
            stats_event_id = items.group(5)
            dataset_id = items.group(6)

            stats_event = "NOT FOUND"
            if stats_events.get(stats_event_id):
                stats_event = stats_events.get(stats_event_id)
            else:
                log_message(ERROR, "Incorrect stats_event_id: %s (id = %s)" % (stats_event_id, id))

            outfile.write('%s - %s ; %s ; roles: () ; groups: () ; %s' % (timestamp, stats_event, username, ip_address))
            if dataset_id != 'NULL':
                outfile.write(' ; dataset(DATASET_ID: "easy-dataset:%s")' % dataset_id)
            if stats_event == 'DOWNLOAD_FILE_REQUEST':
                outfile.write(' ; file(FILE_NAME(0): "")')
            outfile.write('\n')

log_message(INFO,"%s FINISHED creating Easy2 compatible log-event file from sql-dump file %s\n" % (datetime.now(), sql_dump_file))