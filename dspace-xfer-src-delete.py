#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Delete transfer sources that have been already processed by
   automation tools
"""

from __future__ import print_function
import argparse
import logging
import logging.config
import ConfigParser
import re
import os
import shutil
import sqlite3
import sys


LOGGER = logging.getLogger(__name__)


def parse_config_file(conffile):
    cparser = ConfigParser.RawConfigParser()
    cparser.read(conffile)
    global LOGFILE, PIDFILE, AUTOMATION_TOOLS_DB_FILE, UPLOAD_DIR
    LOGFILE = cparser.get('dspace_delete', 'logfile')
    PIDFILE = cparser.get('dspace_delete', 'pidfile')
    AUTOMATION_TOOLS_DB_FILE = cparser.get('dspace_delete', 'automation_tools_db_file')
    UPLOAD_DIR = cparser.get('dspace_delete', 'upload_dir')


def main(arguments):

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('conffile', help="Config file")

    args = parser.parse_args(arguments)

    # parse config file
    parse_config_file(args.conffile)

    # configure logging
    CONFIG = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'default': {
                'format': '%(levelname)-8s  %(asctime)s  %(filename)s:%(lineno)-4s %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'default',
            },
            'file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'default',
                'filename': LOGFILE,
                'backupCount': 20,
                'maxBytes': 10 * 1024 * 1024,
            },
        },
        'loggers': {
            '': {
                'level': 'INFO',
                'handlers': ['console', 'file'],
            },
        },
    }
    logging.config.dictConfig(CONFIG)

    # Check for evidence that this is already running
    try:
        # Open PID file only if it doesn't exist for read/write
        f = os.fdopen(os.open(PIDFILE, os.O_CREAT | os.O_EXCL | os.O_RDWR), 'r+')
    except:
        LOGGER.info('This script is already running, exiting. To start a new run, remove %s', PIDFILE)
        return 0
    else:
        pid = os.getpid()
        f.write(str(pid))
        f.close()

    # get list of transfer sources in uploads directory
    dirlist = os.listdir(UPLOAD_DIR)
    LOGGER.debug("dirlist: {}".format(dirlist))
    regex = r'^ITEM@(\w+)-(\w+).zip'
    p = re.compile(regex)
    matches = [p.match(e) for e in dirlist]
    found = [e.group() for e in matches if e]
    LOGGER.debug("Items found in TS location: {}".format(found))
    if not found or len(found) == 0:
        LOGGER.debug("Exiting... no TS items found")
        os.remove(PIDFILE)
        return 0

    # Delete the transfer source if it appears as completed in the
    # automation tools database
    # (assuming only one entry in the database per unique transfer name)
    conn = sqlite3.connect(AUTOMATION_TOOLS_DB_FILE)
    c = conn.cursor()
    for a in found:
        t = (a,)
        c.execute('select count(*) from unit where path like ? and unit_type="ingest" and status="COMPLETE"', t)
        # query should return a list containing a single tuple with the count value
        # i.e., it should be [(1,)] or [(0,)]
        count = list(c)[0][0]
        LOGGER.debug("query for {} completed ingest returned {} hits".format(a, count))
        if count == 1:
            path_to_delete = os.path.join(UPLOAD_DIR, a)
            LOGGER.info("Deleting {} from TS location".format(path_to_delete))
            # note that item to delete is a directory
            shutil.rmtree(path_to_delete)

    os.remove(PIDFILE)
    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
