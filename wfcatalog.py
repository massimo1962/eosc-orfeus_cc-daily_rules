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

#
# Load configuration from JSON
#
cfg_dir = os.path.dirname(os.path.realpath(__file__))
print (cfg_dir)

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

        if self.config['MONGO']['ENABLED']:
            import mongomanager
            self.mongo = mongomanager.MongoDatabase(self.config, self.log)

    #
    # Main
    #        
    def mainProcess(self ):

        print ("START Main")
      
        timeInitialized = datetime.datetime.now()

        #collector = collector.WFCatalogCollector(parsedargs, config, mongo, self.log)
        self.mongo._connect()

        WFcollector = wfcollector.WFCatalogCollector( self.parsedargs, self.config, self.mongo, self.log)

        if self.config['IRODS']['ENABLED']:
            print ("irods enabled")
            import irodsmanager
            self.irods = irodsmanager.irodsDAO(self.config, self.log)


        if self.config['DUBLIN_CORE']['ENABLED']:
            import dublinCore
            dublinCore = dublinCore.dublinDAO(self.config, self.log) 


        # get stations infrmations via webservices
        print("get datastations")
        datastations = dublinCore.getDataStations()

        # get file list 
        # filtered
        #
        print("get FileList") 
        files = WFcollector.getFileList()

        #
        # applay rules on each file
        #
        print ("start loop")
        for file in files:

            # useful variables
            start_time = WFcollector._getDateFromFile(file)
            collname = self.irodsPath ( file, self.config['IRODS']['BASE_PATH'])
            dirname, filename = os.path.split(file)
            object_path = '{collname}/{filename}'.format(**locals())

            # Log header for each file processed
            self.log.info("#.................................................................START: "+file)
            self.log.info( "file: "+file)
            self.log.info( "collname: " + collname)
            self.log.info( "dirname: "+ dirname)
            self.log.info( "filename: "+ filename)
            

            #................................................................. iREG_INGESTION iCOMMAND - ok
            #
            # Exec Proc: Register Digital objects into iRODS
            #
            self.log.info("iREG on iRODS of : "+file)
            try:
                self.irods.doRegister( dirname, collname, filename)
            except Exception as ex:
                self.log.error("Could not execute a icommand iReg ")
                self.log.error(ex)
                pass
            

            #................................................................. TEST_RULE - ok
            #
            """
            # rule execution w/o params (called directly)
            rule_path = '/var/lib/irods/myrules/source_final/eudatGetV.r'
            self.log.info("exec TEST rule "+ rule_path+" on file : "+file)

            try:
                myvalue = self.irods._ruleExec(rule_path)
            except Exception as ex:
                self.log.error("Could not execute a rule")
                self.log.error(ex)
                pass
            """


            #................................................................. PID - ok
            #
            # Exec Rule:  Make a PID and register into EPIC
            #

            # rule execution w params (called w rule-body, params, and output -must-)
            self.log.info("call PID rule  on file : "+file)
    
            # make a pid
            retValue = rulePIDsingle(self, object_path)

            self.log.info(" PID for digitalObject: "+object_path+" is: " retValue[5])


            #................................................................. REPLICATION
            #
            # Exec Rule: DO a Remote Replica 
            #
            self.log.info("call REPLICATION rule  on file : "+file)

            # make a replica
            retValue = ruleReplication(self, object_path)

            self.log.info(" REPLICA for digitalObject: "+object_path+" in: " retValue[5])


            #................................................................. REGISTRATION_REPLICA
            #
            # Exec Rule: Registration of Rmote PID into local ICAT
            #
            self.log.info("call REGISTRATION rule  on file : "+file)

            # make a pid
            retValue = ruleRegistration(self, object_path)

            self.log.info(" REGISTRATION for digitalObject: "+object_path+" with: " retValue[5])


            #................................................................. DUBLINCORE_META - ok
            #
            # Exec Proc: Store DublinCore metadata into mongo WF_CATALOG
            #           
            self.log.info("_processDCmeta of : "+file)
            try:
                
                dublinCore._processDCmeta(self.mongo, self.irods, collname, start_time, file, datastations)
            except Exception as ex:
                self.log.error("Could not process DublinCore metadata")
                self.log.error(ex)
                pass


            #................................................................. WFCATALOG_META -ok
            #
            # Exec Proc: Store WF_CATALOG metadata into mongo WF_CATALOG
            #            
            self.log.info("_collectMetadata of : "+file)
            try:
                WFcollector._collectMetadata(file)
            except Exception as ex:
                self.log.error("Could not compute WF metadata")
                self.log.error(ex)
                pass

            # Log tail for each file processed
            self.log.info("#.................................................................STOP: "+file)    
         
        # /END FOR        

        print ("END Main ")

        self.log.info("collector synchronization completed in %s." % (datetime.datetime.now() - timeInitialized))
    
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

    
    main = main(parsedargs, config)
    
    ## pass policy here
    main.mainProcess() 
    
    
