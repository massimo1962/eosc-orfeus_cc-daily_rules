#! /usr/bin/env python
"""
#
#
#
  
"""

import os
import sys
import json
import argparse
import datetime
import logging
from logging.handlers import TimedRotatingFileHandler

import wfcollector
import wfsequencer

#
# Load configuration from JSON
#
cfg_dir = os.path.dirname(os.path.realpath(__file__))
print("load conf")
with open(os.path.join(cfg_dir, 'config.json'), "r") as cfg:
  config = json.load(cfg)


  

#
# main()
#
class main():

    def __init__(self, parsedargs, config):

        self.config = config
        self._setupLogger(parsedargs['logfile'])
        self.parsedargs = parsedargs 

        # mongo
        if self.config['MONGO']['ENABLED']:
            import mongomanager
            self.mongo = mongomanager.MongoDAO(self.config, self.log)

    #
    # Main Run
    #        
    def mainProcess(self ):

        print ("START Main")      
        timeInitialized = datetime.datetime.now()

        # connect to mongo DB
        self.mongo._connect()

        # WF Collector
        self.WFcollector = wfcollector.WFCatalogCollector( self.parsedargs, self.config, self.mongo, self.log)
        print("WFcollector ")

        # iRODS
        if self.config['IRODS']['ENABLED']:
            import irodsmanager
            self.irods = irodsmanager.irodsDAO(self.config, self.log)

        # Dublin Core
        if self.config['DUBLIN_CORE']['ENABLED']:
            import wfdublincore
            self.dublinCore = wfdublincore.dublinCore(self.config, self.log) 

        #  file property      
        digitObjProperty = {}        
       
        # get stations informations via webservices
        print("get datastations")        
        digitObjProperty["datastations"] = self.dublinCore.getDataStations()

        # get *filtered*  DigitalObject list to process
        print("get FileList") 
        files = self.WFcollector.getFileList()
        # set sequencer 
        sequencer = wfsequencer.sequencer(self.config, self.log, self.irods, self.mongo, self.WFcollector, self.dublinCore )

        #
        # applay rules on each file
        #
        for file in files:

            # Digital Object Property extraction            
            dirname, filename = os.path.split(file)
            digitObjProperty["file"] = file
            digitObjProperty["start_time"] = self.WFcollector._getDateFromFile(file)
            collname = self.irodsPath ( file, self.config['IRODS']['BASE_PATH'])
            colltarget = self.irodsPath ( file, self.config['IRODS']['REMOTE_PATH'])
            digitObjProperty["dirname"] = dirname
            digitObjProperty["filename"] = filename
            digitObjProperty["collname"] = collname
            digitObjProperty["object_path"] = '{collname}/{filename}'.format(**locals())
            digitObjProperty["target_path"] = '{colltarget}/{filename}'.format(**locals())

            #
            # run rules sequence 
            sequencer.doSequence(digitObjProperty)


        print ("END Main ")

        self.log.info(" ** Sequence is done, collector synchronization completed in %s." % (datetime.datetime.now() - timeInitialized))
    
    #
    # irods path maker
    # 
    def irodsPath (self, file, irodsPathBase):
        
        fileSplit = os.path.basename(file).split('.')
        if irodsPathBase[:-1] != '/' : irodsPathBase = irodsPathBase+"/"

        # with filename
        #irodsPath = irodsPathBase+fileSplit[5]+"/"+fileSplit[0]+"/"+fileSplit[1]+"/"+fileSplit[3]+".D"+"/"+os.path.basename(file)

        # without filename
        irodsPath = irodsPathBase+fileSplit[5]+"/"+fileSplit[0]+"/"+fileSplit[1]+"/"+fileSplit[3]+".D"

        return irodsPath

    #
    # build logger   
    #  
    def _setupLogger(self, logfile):

        # Set up WFCatalogger
        self.log = logging.getLogger('WFCatalog Collector')

        log_file = logfile or config['DEFAULT_LOG_FILE']

        # Set Level
        self.log.setLevel('INFO')

        self.file_handler = TimedRotatingFileHandler(log_file, when="midnight")
        self.file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        self.log.addHandler(self.file_handler)





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
    parser.add_argument('--version', action='version', version=config['VERSION'])

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
    parsedargs = vars(parser.parse_args())

    ## wake-up
    main = main(parsedargs, config)
    
    ## rock-n-roll
    main.mainProcess() 
    
    
