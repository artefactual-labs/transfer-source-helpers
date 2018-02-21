#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Fetch dspace files from and upload to xfer source location
"""

from __future__ import print_function
import argparse
import ConfigParser
import logging
import logging.config
import os
import re
import shlex
import sqlite3
import subprocess
import sys


LOGGER = logging.getLogger(__name__)


def parse_config_file(conffile):
    cparser = ConfigParser.RawConfigParser()
    cparser.read(conffile)
    global RSYNC_PASSWORD, RSYNC_PATH, LOGFILE, PIDFILE, DATABASE_FILE, UPLOAD_DIR
    RSYNC_PASSWORD = cparser.get('dspace_upload', 'rsync_password')
    RSYNC_PATH = cparser.get('dspace_upload', 'rsync_path')
    LOGFILE = cparser.get('dspace_upload', 'logfile')
    PIDFILE = cparser.get('dspace_upload', 'pidfile')
    DATABASE_FILE = cparser.get('dspace_upload', 'database_file')
    UPLOAD_DIR = cparser.get('dspace_upload', 'upload_dir')


def get_dspace_available_items():
    # Form command string used to get entries
    command = ['rsync',
               '-a',
               '--list-only',
               '--exclude', '.*',  # Ignore hidden files
               RSYNC_PATH
               ]

    LOGGER.info('rsync list command: %s', command)
    LOGGER.debug('"%s"', '" "'.join(command))  # For copying to shell
    try:
        env = os.environ.copy()
        env['RSYNC_PASSWORD'] = RSYNC_PASSWORD
        output = subprocess.check_output(command, env=env)
    except Exception as e:
        LOGGER.warning("rsync list failed: %s", e, exc_info=True)
    else:
        output = output.splitlines()

        # example output of rsync --list-only 
        #
        # -rw-r--r--    118,919,773 2017/03/09 22:31:24 ITEM@2429-100.zip
        # -rw-r--r--     10,631,105 2016/11/30 08:23:35 ITEM@2429-10000.zip
        # -rw-r--r--     13,298,401 2016/11/30 06:39:05 ITEM@2429-10001.zip
        # -rw-r--r--     10,212,180 2016/11/29 10:21:49 ITEM@2429-10002.zip
        # -rw-r--r--     10,333,444 2016/11/29 16:20:38 ITEM@2429-10003.zip


        regex = r'^(?P<type>.)(?P<permissions>.{9}) +(?P<size>[\d,]+) (?P<timestamp>..../../.. ..:..:..) (?P<name>.*)$'
        p = re.compile(regex)
        matches = [p.match(e) for e in output]
        # First get list of files, items whose type (first char in each line output) is '-'
        files = [e.group('name') for e in matches
                       if e and e.group('name') != '.' and e.group('type') == '-']
        LOGGER.debug('files in server: %s', len(files))

        # Now get ITEM@xxx-xxxx.zip entries
        regex = r'^ITEM@(\w+)-(\w+).zip'
        p = re.compile(regex)
        matches = [p.match(e) for e in files]
        items = [e.group() for e in matches if e]
        LOGGER.debug('ITEM@xxxxx.zip files: %s', len(items))

        # return set of items
        return set(items)


def get_dspace_uploaded_items():
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    # query for all records in the table uploaded
    c.execute('select * from uploaded')
    # c is an iterator containing all the query results (one tuple per row)
    # unpack each tuple, add to a list, and return as set
    complete_set = set()
    for row in c:
        complete_set.add(row[0])
    c.close()
    conn.close()
    return complete_set


def main(arguments):

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('conffile', help="Config file")
    parser.add_argument("--file", help="dspace file to upload (if omitted, upload the next one not already uploaded)")
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

    # if file was specified in the args, download the file
    if args.file:
        LOGGER.debug("Specified file: {}".format(args.file))
        commstr = 'rsync -a {}{} {}'.format(RSYNC_PATH, args.file, UPLOAD_DIR)
        LOGGER.debug('rsync command: {}'.format(commstr))
        env = os.environ.copy()
        env['RSYNC_PASSWORD'] = RSYNC_PASSWORD
        subprocess.check_call(shlex.split(commstr), env=env)

        # assuming that when a file is specified in the args, it is for 
        # debugging purposes so: 
        #  - do not do check/add to database of uploaded items
        #  - do not check limit of maximum uploads

        os.remove(PIDFILE)
        return 0

    else:
        LOGGER.debug("No file specified in command line, assuming automated upload flow")

        # In order to avoid filling up disk space,
        # just allow max_upload_items at a time
        # Items are zip files like: ITEM@2429-10003.zip, ITEM@2429-10003.zip
        max_upload_items = 1
        dirlist = os.listdir(UPLOAD_DIR)
        LOGGER.debug("files in UPLOAD_DIR: {}".format(dirlist))
        regex = r'^ITEM@(\w+)-(\w+).zip'
        p = re.compile(regex)
        matches = [p.match(e) for e in dirlist]
        found = [e.group() for e in matches if e]
        if found and len(found) >= max_upload_items:
            LOGGER.debug("Exiting... (found {} already uploaded item)".format(len(found)))
            os.remove(PIDFILE)
            return 0
        else:
            LOGGER.debug("Proceed to upload a new source...")

        # fetch list of directories from dspace server
        available_set = get_dspace_available_items()
        available_list = sorted(available_set)
        # LOGGER.debug('available list: %s', available_list)
        LOGGER.info('len(available_list): %s', len(available_list))

        # check existing database of uploaded videos
        uploaded_set = get_dspace_uploaded_items()
        uploaded_list = sorted(uploaded_set)
        # LOGGER.debug('uploaded list: %s', uploaded_list)
        LOGGER.info('len(uploaded_list): %s', len(uploaded_list))

        # check how many pending directories are
        pending_set = available_set - uploaded_set
        pending_list = sorted(pending_set)
        # LOGGER.debug('pending list: %s', pending_list)
        LOGGER.info('len(pending_list): %s', len(pending_list))

        # will pick up the first entry in the pending set, download files and arrange
        # in a way that can be ingested by archivematica/automation tools
        if pending_list:    # do only when list not empty
            LOGGER.debug('Item to download/upload: %s', pending_list[0])

            # note that we are copying the zip file to a directory with the same name 
            # (to replace the 00_file_to_folder.py script in automation tools)
            
            # first create temp dir (name starting with . so that automation tools can't see it yet
            # and transfers do not start by accident if the script was called by crontab for example)
            temp_transfer_full_path = os.path.join(UPLOAD_DIR, "." + pending_list[0])
            if not os.path.exists(temp_transfer_full_path):
                LOGGER.debug('Creating directory: {}'.format(temp_transfer_full_path))
                os.mkdir(temp_transfer_full_path)

            # upload dspace zip to the temp dir
            commstr = 'rsync -a {}{} {}'.format(RSYNC_PATH, pending_list[0], temp_transfer_full_path)
            LOGGER.debug('rsync command: {}'.format(commstr))
            env = os.environ.copy()
            env['RSYNC_PASSWORD'] = RSYNC_PASSWORD
            subprocess.check_call(shlex.split(commstr), env=env)

            # done with the copy, now rename the temp directory (same name as zip file)
            transfer_full_path = os.path.join(UPLOAD_DIR, pending_list[0])
            os.rename(temp_transfer_full_path, transfer_full_path)

            # add an entry to the uploaded table of the database (if not already there)
            # database schema is:
            #   sqlite> .schema
            #   CREATE TABLE uploaded(item TEXT);

            conn = sqlite3.connect(DATABASE_FILE)
            c = conn.cursor()
            t = (pending_list[0],)
            c.execute('select * from uploaded where item=?', t)
            l = list(c)
            if l:
                LOGGER.debug("{} already in uploaded table".format(pending_list[0]))
            else:
                c.execute('insert into uploaded values (?)', t)
                conn.commit()
                c.close()
                conn.close()
                LOGGER.info("{} added to uploaded table".format(pending_list[0]))

        os.remove(PIDFILE)
        return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
