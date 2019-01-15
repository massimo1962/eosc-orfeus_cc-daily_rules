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
      config = json.load(cfg)

    if config['MONGO']['ENABLED']:
      from pymongo import MongoClient
"""
class main():

    def __init__(self, parsedargs, config):
        """
        ****
        """
        self.config = config
        self._setupLogger(parsedargs['logfile'])
        self.parsedargs = parsedargs        

        if self.config['MONGO']['ENABLED']:
            import mongomanager
            self.mongo = mongomanager.MongoDatabase(self.config, self.log)


    def mainProcess(self ):
        """
        *****
        """
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
        print("datastations")
        datastations = dublinCore.getDataStations()

        #for data in datastations:
        #    print(data)
        
        # get file list 
        # filtered
        # 
        #print("files")
        files = WFcollector.getFileList()

        #
        # applay rules on each file
        #
        print ("start loop")
        for file in files:

            start_time = WFcollector._getDateFromFile(file)
            collname = self.irodsPath ( file, self.config['IRODS']['BASE_PATH'])
            dirname, filename = os.path.split(file)

            
            self.log.info("#.................................................................")
            self.log.info( "file: "+file)
            self.log.info( "collname: " + collname)
            self.log.info( "dirname: "+ dirname)
            self.log.info( "filename: "+ filename)
            

            #................................................................. iREG_INGESTION - ok
            #
            # Exec Proc: Register Digital objects into iRODS
            #
            #print("files")

            
            self.log.info("iREG on iRODS of : "+file)
            try:
                self.irods.doRegister( dirname, collname, filename)
            except Exception as ex:
                self.log.error("Could not execute a icommand iReg ")
                self.log.error(ex)
                pass
            

            #self._filterFiles()
            #print (self.files)   RegRule.r   eudatGetV.r
            #................................................................. TEST_RULE - ok

            rule_path = '/usr/src/collector/source_final/eudatGetV.r'
            self.log.info("exec rule "+ rule_path+" on file : "+file)

            try:
                myvalue = irods._ruleExec(rule_path)
            except Exception as ex:
                self.log.error("Could not execute a rule")
                self.log.error(ex)
                pass

            #................................................................. PID
            #
            # Exec Rule:  Make a PID and register into EPIC
            #

            #rule_path = '/usr/src/collector/source_final/eudatPidSingleCheck2.r'
            rule_path = '/var/lib/irods/myrules/source_final/eudatPidSingleCheck2.r'
            self.log.info("exec rule "+ rule_path+" on file : "+file)
    
            object_path = collname+"/"+filename
            # test metadata
            attr_name = "test_attr"
            attr_value = "test_value"

            # rule parameters
            input_params = {  # extra quotes for string literals
                '*object': '"{object_path}"'.format(**locals()),
                '*name': '"{attr_name}"'.format(**locals()),
                '*value': '"{attr_value}"'.format(**locals())
            }
            output = 'ruleExecOut'

            # run test rule
            myrule = Rule(session, rule_file=rule_path,
                          params=input_params, output=output)
            

            try:
                returnedArray = myrule.execute()
            except Exception as ex:
                self.log.error("Could not execute a rule")
                self.log.error(ex)
                pass
            

            """
            rule_path = '/var/lib/irods/myrules/source_final/eudatPidSingle.r'
            self.log.info("exec rule "+ rule_path+" on file : "+file)
            try:
                myvalue = irods._ruleExec(rule_path)
            except Exception as ex:
                self.log.error("Could not execute a rule")
                self.log.error(ex)
                continue

            """

            #................................................................. REPLICATION
            #
            # Exec Rule: DO a Remote Replica 
            #
            """
            rule_path = '/var/lib/irods/myrules/source_final/eudatReplication.r'
            self.log.info("exec rule "+ rule_path+" on file : "+file)
            try:
                myvalue = irods._ruleExec(rule_path)
            except Exception as ex:
                self.log.error("Could not execute a rule")
                self.log.error(ex)
                continue
            """


            #................................................................. REGISTRATION_REPLPID
            #
            # Exec Rule: Registration of Rmote PID into local ICAT
            #
            """
            rule_path = '/var/lib/irods/myrules/source_final/RegRule.r'
            self.log.info("exec rule "+ rule_path+" on file : "+file)
            try:
                myvalue = irods._ruleExec(rule_path)
            except Exception as ex:
                self.log.error("Could not execute a rule")
                self.log.error(ex)
                continue
            """

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
            
            

        # END FOR        


        print ("END Main ")

        # Delete or process files
        #if self.args['delete']:
        #  self._deleteFiles()
        #else:
        #  self._processFiles()

        self.log.info("collector synchronization completed in %s." % (datetime.datetime.now() - timeInitialized))

     # irods path maker
    def irodsPath (self, file, irodsPathBase):
        
       
        #irodsPathBase = "/INGV/home/rods/san/archive/"
        #filename =  'NI.ACOM..HHE.D.2015.006'

        fileSplit = os.path.basename(file).split('.')
        if irodsPathBase[:-1] != '/' : irodsPathBase = irodsPathBase+"/"

        #irodsPath = irodsPathBase+fileSplit[5]+"/"+fileSplit[0]+"/"+fileSplit[1]+"/"+fileSplit[3]+".D"+"/"+os.path.basename(file)
        irodsPath = irodsPathBase+fileSplit[5]+"/"+fileSplit[0]+"/"+fileSplit[1]+"/"+fileSplit[3]+".D"

        return irodsPath


     # bild logger   
    def _setupLogger(self, logfile):
        """
        WFCatalogCollector._setupLogger
        > logging setup for the WFCatalog Collector
        """

        # Set up WFCatalogger
        self.log = logging.getLogger('WFCatalog Collector')

        log_file = logfile or config['DEFAULT_LOG_FILE']

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
    
    
