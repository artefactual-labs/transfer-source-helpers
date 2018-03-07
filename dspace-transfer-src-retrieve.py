#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Fetch dspace files from and upload to xfer source location
"""

from __future__ import print_function
import argparse
import ConfigParser
import datetime
import logging
import logging.config
import os
import re
import shlex
import subprocess
import sqlalchemy
import sys
from sqlalchemy.ext.declarative import declarative_base

LOGGER = logging.getLogger(__name__)

Base = declarative_base()


# sqlalchemy table definition
class Item(Base):
    __tablename__ = 'items'
    id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.Sequence('user_id_seq'), primary_key=True)
    path = sqlalchemy.Column(sqlalchemy.Binary())
    time = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True))  # UTC time
    ignore = sqlalchemy.Column(sqlalchemy.Boolean, default=False)  # ignore (so it can be retrieved again)

    def __repr__(self):
        return "<Unit(id={s.id}, name={s.name}, time={s.time}>".format(s=self)


def parse_config_file(conffile):
    cparser = ConfigParser.RawConfigParser()
    cparser.read(conffile)
    global RSYNC_PASSWORD, RSYNC_PATH, LOGFILE, LOGLEVEL, PIDFILE, DATABASE_FILE, TRANSFER_SOURCE_DIR
    section = 'dspace_retrieve'
    RSYNC_PASSWORD = (cparser.has_option(section, 'rsync_password') and
                      cparser.get(section, 'rsync_password') or
                      "DUMMY")
    RSYNC_PATH = cparser.get(section, 'rsync_path')
    LOGFILE = cparser.get(section, 'logfile')
    LOGLEVEL = (cparser.has_option(section, 'loglevel') and
                cparser.get(section, 'loglevel') or
                "INFO")
    PIDFILE = cparser.get(section, 'pidfile')
    DATABASE_FILE = cparser.get(section, 'dspace_retrieve_db_file')
    TRANSFER_SOURCE_DIR = cparser.get(section, 'transfer_source_dir')


def get_dspace_available_items():
    # Form command string used to get entries
    command = ['rsync',
               '-a',
               '--list-only',
               '--exclude=.*',  # Ignore hidden files
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


def get_retrieved_items(session):
    # get all retrieved items with ignore=Flase
    uploaded_items = session.query(Item).filter_by(ignore=False)
    uploaded_set = set()
    for i in uploaded_items:
        uploaded_set.add(i.path)

    return uploaded_set


def add_retrieved_item(session, path, time):
    new_item = Item(path=path, time=time)
    session.add(new_item)
    session.commit()
    return


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
                'level': LOGLEVEL,
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
        commstr = 'rsync -a {}{} {}'.format(RSYNC_PATH, args.file, TRANSFER_SOURCE_DIR)
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
        dirlist = os.listdir(TRANSFER_SOURCE_DIR)
        LOGGER.debug("files in TRANSFER_SOURCE_DIR: {}".format(dirlist))
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

        # check all exported items in dspace server
        available_set = get_dspace_available_items()
        available_list = sorted(available_set)
        LOGGER.debug('available list: %s', available_list)
        LOGGER.info('len(available_list): %s', len(available_list))

        # Initialize  database
        if not os.path.isfile(DATABASE_FILE):
            # Create database file if it does not exist already
            with open(DATABASE_FILE, "a"):
                pass
        engine = sqlalchemy.create_engine('sqlite:///{}'.format(DATABASE_FILE), echo=False)
        Session = sqlalchemy.orm.sessionmaker(bind=engine)
        Base.metadata.create_all(engine)
        session = Session()

        # check database for retrieved items
        uploaded_set = get_retrieved_items(session)
        uploaded_list = sorted(uploaded_set)
        LOGGER.debug('uploaded list: %s', uploaded_list)
        LOGGER.info('len(uploaded_list): %s', len(uploaded_list))

        # check pending items
        pending_set = available_set - uploaded_set
        pending_list = sorted(pending_set)
        LOGGER.debug('pending list: %s', pending_list)
        LOGGER.info('len(pending_list): %s', len(pending_list))

        # will pick up the first entry in the pending set, download files and arrange
        # in a way that can be ingested by archivematica/automation tools
        if pending_list:    # do only when list not empty
            LOGGER.debug('Item to download/upload: %s', pending_list[0])

            # copying the zip file to a directory (with the same name)
            # (to replace the 00_file_to_folder.py script in automation tools)

            # first create temp dir (name starting with . so that automation tools can't see it yet
            # and transfers do not start by accident if the script was called by crontab for example)
            temp_transfer_full_path = os.path.join(TRANSFER_SOURCE_DIR, "." + pending_list[0])
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
            transfer_full_path = os.path.join(TRANSFER_SOURCE_DIR, pending_list[0])
            os.rename(temp_transfer_full_path, transfer_full_path)

            # add an entry to the uploaded table of the database
            add_retrieved_item(session, pending_list[0], datetime.datetime.utcnow())
            LOGGER.info("{} added to retrieved items table".format(pending_list[0]))

        session.close()
        os.remove(PIDFILE)
        return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
