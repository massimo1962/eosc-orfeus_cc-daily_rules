#! /usr/bin/env python

from __future__ import print_function

import os
import json
import logging
import datetime
import hashlib
import warnings
import sys
import fnmatch
import signal

import uuid
import csv
import http.client
from logging.handlers import TimedRotatingFileHandler


# Load configuration from JSON
cfg_dir = os.path.dirname(os.path.realpath(__file__))
print (cfg_dir)
with open(os.path.join(cfg_dir, 'config.json'), "r") as cfg:
  CONFIG = json.load(cfg)

class WFCatalogCollector():
    """
    WFCatalogCollector class for ingesting waveform metadata
    """

    def __init__(self,  mongo, irods, logfile=None):
        """
        WFCatalogCollector.__init__
        > initialize the class, set up logger and database connection
        """
        print("collectore called")
        self.mongo = mongo
        self.irods = irods

        self._setupLogger(logfile)


    def _setupLogger(self, logfile):
        """
        WFCatalogCollector._setupLogger
        > logging setup for the WFCatalog Collector
        """

        # Set up WFCatalogger
        self.log = logging.getLogger('WFCatalog Collector')

        log_file = logfile or CONFIG['DEFAULT_LOG_FILE']

        self.log.setLevel('INFO')

        self.file_handler = TimedRotatingFileHandler(log_file, when="midnight")
        self.file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        self.log.addHandler(self.file_handler)

    def process(self, options):
        """
        WFCatalogCollector.process
        > processes data with options
        """

        self.timeInitialized = datetime.datetime.now()

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
        self._setOptions(options)
        print ("START Process")

        # 1. Get files for processing,
        # 2. filter them,
        # 3. process them
        self._getFiles()
        #self._filterFiles()
        #print (self.files)
        for file in self.files:
            print ("file: "+file)
            dirname, filename = os.path.split(file)
            print(" path: "+ dirname)
            print(" file: "+ filename)
            print(" irods_path: "+CONFIG['IRODS']['BASE_PATH'])

            self.irods._register(dirname, filename)


        print ("END ***")

        # Delete or process files
        #if self.args['delete']:
        #  self._deleteFiles()
        #else:
        #  self._processFiles()

        self.log.info("WFCollector synchronization completed in %s." % (datetime.datetime.now() - self.timeInitialized))


    def _getFiles(self):
        """
        WFCatalogCollector._getFiles
        reads all files from a given input directory
        """

        self.file_counter = 0

        # Past was given, collect files from the past
        if self.args['past']:
          self.files = self._getPastFiles()
          self.log.info("Collected %d file(s) from the past %s" % (len(self.files), self.args['past']))
          
        # List was given
        elif self.args['list']:
          self.files = [f for f in json.loads(self.args['list']) if os.path.isfile(f)]
          self.log.info("Collected %d file(s) from list" % len(self.files))

        # Raise if an invalid input directory is given
        elif self.args['dir']:
          if not os.path.isdir(self.args['dir']):
            raise Exception("Input is not a valid directory on the file system.")

          # Collect all the files (recursively) from a directory and add them
          self.files = [os.path.join(root, f) for root, dirs, files in os.walk(self.args['dir']) for f in files if os.path.isfile(os.path.join(root, f))]
          self.log.info("Collected %d file(s) from directory %s" % (len(self.files), self.args['dir']))

        # If globbing match all files
        elif self.args['glob']:
          self.files = glob.glob(self.args['glob'])
          self.files = [f for f in self.files if os.path.isfile(f)]

        # Single file as input
        elif self.args['file']:
          if not os.path.isfile(self.args['file']):
            raise Exception("Argument --file requires a valid file.")

          self.files = [self.args['file']]
          self.log.info("Collected file %s" % (self.args['file']))

        # Specific date as input (with optional range)
        elif self.args['date']:
          self.files = []
          specific_date = datetime.datetime.strptime(self.args['date'], "%Y-%m-%d")
          n_days = int(self.args['range'])
          # Include a given range (default to 1)
          for day in range(abs(n_days)):
            if n_days > 0:
              self.files += self._collectFilesFromDate(specific_date + datetime.timedelta(days=day))
            else:
              self.files += self._collectFilesFromDate(specific_date - datetime.timedelta(days=day))
          self.log.info("Collected %d file(s) from date %s +%d days" % (len(self.files), self.args['date'], n_days))

        # Raise on no input
        else:
          raise Exception("Input is empty. Use --dir, --file, or --list to specify a directory, file, or list of files to process.")
        
    
    def _getPastFiles(self):
        """
        WFCatalogCollector._getPastFiles
        > returns files from today, yesterday, last week, 
        > last two weeks, or last month
        """

        now = datetime.datetime.now()
        start, end = self._getWindow()

        # Loop over the window and collect files
        pastFiles = []
        for day in range(start, end):
          pastFiles += self._collectFilesFromDate(now - datetime.timedelta(days=day))

        return pastFiles    
    
    def _getWindow(self):
        """
        WFCatalogCollector._getWindow
        > returns window for given --past option
        """
        # Set the day tuple window for reprocessing [inclusive, exclusive)
        if self.args['past'] == 'day':
          window = (0, 1)
        elif self.args['past'] == 'yesterday':
          window = (1, 2)
        elif self.args['past'] == 'week':
          window = (1, 8)
        elif self.args['past'] == 'fortnight':
          window = (1, 15)
        elif self.args['past'] == 'month':
          window = (1, 32)

        return window[0], window[1]


    def _collectFilesFromDate(self, date):
        """
        WFCatalogCollector._collectFilesFromDate
        > collects the files for a given year and day
        """

        # Get the year & day of year
        jday = date.strftime("%j")
        year = date.strftime("%Y")
        print ("jday " + jday)
        print("year " + year)

        # ODC directory structure makes it simple to loop over years and days
        if CONFIG['STRUCTURE'] == 'ODC':
          directory = os.path.join(CONFIG['ARCHIVE_ROOT'], year, jday)
          collectedFiles = [os.path.join(directory, f) for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]

        # SDS structure is slightly more complex, loop over all directories
        # in a year and extract files ending with a given jday
        elif CONFIG['STRUCTURE'] == 'SDS':
          collectedFiles = []
          directory = os.path.join(CONFIG['ARCHIVE_ROOT'], year)
          print("dir:" + directory)
          for subdir, dirs, files in os.walk(directory):

            if len(dirs) > 1  :
                print ("dirs --> imkdir " + str(dirs))
            elif len(dirs) > 0 :
                for thisDir in dirs :
                    print("curr-dir : imkdir " + str(thisDir))
                  
            #if subdir : print ("subdir --> " + str(subdir))
            for file in files:
              if file.endswith(jday) and os.path.isfile(os.path.join(subdir, file)):
                print ("file added : " + file)
                collectedFiles.append(os.path.join(subdir, file))
        
        else:
          raise Exception("WFCatalogCollector.getFilesFromDirectory: unknown directory structure.")

        return collectedFiles    

    
    def _setOptions(self, user_options):
        """
        WFCatalogCollector._setOptions
        > returns default options for the Collector
        > and replaces with user options given
        > through JSON dictionary
        """

        # Standard options
        default_options = {
          'range': 1,
          'file': None,
          'dir': None,
          'list': None,
          'past': None,
          'date': None,
          'csegs': False,
          'flags': False,
          'hourly': False,
          'delete': False,
          'update': False,
          'force': False,
          'config': False,
          'version': False,
        }

        # Loop over user options and replace any default options
        for key in user_options:
          default_options[key] = user_options[key]

        self.args = default_options

        # Check if there is a single input method
        nInput = 5 - [self.args['date'], self.args['file'], self.args['dir'], self.args['list'], self.args['past']].count(None)
        if nInput == 0:
          raise Exception("No input was given");
        if nInput > 1:
          raise Exception("Multiple file inputs were given; aborting")

        if not CONFIG['MONGO']['ENABLED'] and self.args['update']:
          raise Exception("Cannot update files when database connection is disabled")

        # Show configuration and exit
        if self.args['config']:
          self.showConfig(); sys.exit(0)
        if self.args['version']:
          self.showVersion(); sys.exit(0)

        self._printArguments()
        self._setGranularity()

    def _printArguments(self):
        """
        WFCatalogCollector._printArguments
        > neatly prints some of the init settings
        """

        if self.args['force'] and not self.args['update']:
          raise Exception("Only catalog updates can be forced. Give --update --force")

        self.log.info("Begin collection of new waveform metadata.")

        if self.args['update']:
          self.log.info("Begin updating of waveform documents in database")

        if self.args['force']:
          self.log.info("Update is being forced")

    def _setGranularity(self):
        """
        WFCatalogCollector._setGranularity
        > Set the granularity
        """

        # For daily granularity take steps of 24h
        self.gran = 1 if self.args['hourly'] else 24      

