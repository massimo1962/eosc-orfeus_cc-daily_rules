#! /usr/bin/env python
"""
from __future__ import absolute_import
from __future__ import print_function

import os
import sys
import socket
import json
import hashlib
import base64
import random
import string
import unittest


 

import logging

import warnings
import fnmatch
import signal
import uuid
import csv
import http.client

  
"""


import os
import sys
import socket
import json
import argparse
import datetime

import mongomanager
import irodsmanager
import collector
import ruler


# Load configuration from JSON
cfg_dir = os.path.dirname(os.path.realpath(__file__))
print (cfg_dir)
with open(os.path.join(cfg_dir, 'config.json'), "r") as cfg:
  CONFIG = json.load(cfg)

if CONFIG['MONGO']['ENABLED']:
    mongo = mongomanager.MongoDatabase(CONFIG)

if CONFIG['IRODS']['ENABLED']:
    irods = irodsmanager.irodsUtil(CONFIG)

#  from pymongo import MongoClient
"""
def handler(signum, frame):
    raise Exception("Metric calculation has timed out")

    from logging.handlers import TimedRotatingFileHandler

    # ObsPy mSEED-QC is required
    try:
      from obspy.signal.quality_control import MSEEDMetadata
    except ImportError as ex:
      raise ImportError('Failure to load MSEEDMetadata; ObsPy mSEED-QC is required.')

    # Load configuration from JSON
    cfg_dir = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(cfg_dir, 'config.json'), "r") as cfg:
      CONFIG = json.load(cfg)

    if CONFIG['MONGO']['ENABLED']:
      from pymongo import MongoClient
"""

def mainProcess( options, mongo, irods ):
    """
    WFCatalogCollector.process
    > processes data with options
    """

    timeInitialized = datetime.datetime.now()


    WFCollector = collector.WFCatalogCollector( mongo, irods, args['logfile'])
    #WFCollector.test_register()

    #WFCollector.process(args)

    # Attempt connection to the database
    #print (self.mongo._connected)
    """
    if not self.mongo._connected:
      if CONFIG['MONGO']['ENABLED']:
        try:
          self.mongo._connect()
          self.log.info("Connection to the database has been established")
        except Exception as ex:
          self.log.critical("Could not establish connection to the database"); sys.exit(0)
      else:
        self.log.info("Connection to the database is disabled");
    """
    WFCollector._setOptions(options)
    print ("START Process")

    ################################################ iREG
    #
    # Register objects
    #
    # 1. Get files for processing,
    # 2. filter them,
    # 3. process them

    """
    files = WFCollector._getFiles()

    for file in files:
        print ("file: "+file)

        dirname, filename = os.path.split(file)
        irods._register(dirname, filename)

        print(" path: "+ dirname)
        print(" file: "+ filename)
        print(" irods_path: "+CONFIG['IRODS']['BASE_PATH'])

        
    """
    
    #self._filterFiles()
    #print (self.files)   RegRule.r   eudatGetV.r

    ################################################ PID


    #
    # Exec Rule
    #
    print("call ruleExec")

    rule_path = '/var/lib/irods/myrules/source_final/RegRule.r'
    myvalue = irods._ruleExec(rule_path)

    print (myvalue)
    print ("done!")


    ################################################ REPLICATION





    ################################################ ReplicaReg






    print ("END ***")

    # Delete or process files
    #if self.args['delete']:
    #  self._deleteFiles()
    #else:
    #  self._processFiles()

    # self.log.info("WFCollector synchronization completed in %s." % (datetime.datetime.now() - self.timeInitialized))

if __name__ == '__main__':

    # Parse cmd line arguments
    parser = argparse.ArgumentParser(description='Processes mSEED files and ingests waveform metadata to a Mongo repository.')

    # Input file options
    parser.add_argument('--dir', help='directory containing the files to process')
    parser.add_argument('--file', help='specific file to be processed')
    parser.add_argument('--list', help='specific list of files to be processed ["file1", "file2"]')
    parser.add_argument('--past', help='process files in a specific range in the past', choices=['day', 'yesterday', 'week', 'fortnight', 'month'], default=None)
    parser.add_argument('--date', help='process files for a specific date', default=None)
    parser.add_argument('--range', help='number of days after a specific date', default=1)

    # Options to show config/versioning
    parser.add_argument('--config', help='view configuration options', action='store_true')
    parser.add_argument('--version', action='version', version=CONFIG['VERSION'])

    # Add flags and continuous segments
    parser.add_argument('--flags', help='include mSEED header flags in result', action='store_true')
    parser.add_argument('--csegs', help='include continuous segments in result', action='store_true')
    parser.add_argument('--hourly', help='include hourly granules in result', action='store_true')

    # Set custom logfile
    parser.add_argument('--logfile', help='set custom logfile')

    # Options to update documents existing in the database, normally
    # files that are already processed are skipped
    # Updates can be forced (without checksum check)
    parser.add_argument('--update', help='update existing documents in the database', action='store_true')
    parser.add_argument('--force', help='force file updates', action='store_true')
    parser.add_argument('--delete', help='delete files from database', action='store_true')

    # Option for Catalog DublinCore dc_on 
    parser.add_argument('--dc_on', help='extract meta Dublin Core for do_wf collection', action='store_true')

    # Get parsed arguments as a JSON dict to match
    # compatibility with an imported class
    args = vars(parser.parse_args())

    
    
    mainProcess(args, mongo, irods)
    

    #WFCollector = collector.WFCatalogCollector( mongo, irods, args['logfile'])
    #WFCollector.test_register()

    #WFCollector.process(args)