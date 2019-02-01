#! /usr/bin/env python
"""
#
#
#
  
"""

import os
import json
import textwrap

import irods
from irods.session import iRODSSession
import irods.exception as ex
from irods.column import Criterion
from irods.data_object import chunks
import irods.test.helpers as helpers
import irods.keywords as kw
from irods.meta import iRODSMeta
from irods.models import (DataObject, Collection, Resource, User, DataObjectMeta,CollectionMeta, ResourceMeta, UserMeta)
from irods.results import ResultSet
from irods.rule import Rule
from irods.meta import iRODSMetaCollection
from irods.exception import CollectionDoesNotExist


#
#  data access object class for irods
#
class irodsDAO():


    def __init__(self, config, log):

        print("irods ")
        self.session = None
        self.log = log
        self.config = config

    #
    # irods Connection
    #
    def _irodsConnect(self):

        if self.session:
            return

        self.log.info("try connection to irods")
        # Make iRODS connection
        self.session = iRODSSession(host=str(self.config['IRODS']['HOST']), port=str(self.config['IRODS']['PORT']), user=str(self.config['IRODS']['USER']), password=str(self.config['IRODS']['PWD']), zone=str(self.config['IRODS']['ZONE']))
        self.log.info("done")


    #
    # irods ingestion iREG
    #
    def doRegister(self, dirname, collname, filename):   

        self._irodsConnect()

        obj_file = os.path.join(dirname, filename)
        obj_path = '{collname}/{filename}'.format(**locals())
        self._checkCollExsist(collname)
        
        # register file in test collection
        self.log.info("check obj_file : "+obj_file)
        self.log.info("check obj_path : "+obj_path)

        self.log.info("check or create a collection recursively : "+collname)
        try:
            self.session.data_objects.register(obj_file, obj_path)
            self.log.info("file registered! : "+obj_path)
        except Exception as ex:
            self.log.error("Could not register a file_obj  ")         
            self.log.error(ex)
            pass

        # confirm object presence
        #obj = self.session.data_objects.get(obj_path)
        
        #print("registred!")
        #print (obj)
       
    #
    # Execute a paramless rule
    #
    def _ruleExec(self, rule_file_path):

        self._irodsConnect()
        #print("path rule: "+rule_file_path)

        # run  rule
        myrule = Rule(self.session, rule_file_path)
        ruleout = myrule.execute()

        return ruleout

    #
    # Check if a collection exsist, if not create recursively
    #
    def _checkCollExsist(self, collname):

        self.log.info("check or create a collection recursively : "+collname)
        try:
            self.session.collections.create(collname, recurse=True)
            self.log.info("collection created! : "+collname)
        except Exception as ex:
            self.log.error("Could not create a collection recursively ")
            self.log.error(ex)
            pass


    #
    # get Digital Object
    #
    def getObject(self, obj_path):

        self._irodsConnect()

        return self.session.data_objects.get(obj_path)


    #
    # irods Rule Execution: PID creation  (PID)
    #
    def rulePIDsingle(self, object_path, rule_path):
           
        # check connection
        self._irodsConnect()

        self.log.info("exec PID SINGLE rule inside irods ")

        # load rule from file
        rule_total = self.load_rule(rule_path, path='"{object_path}"'.format(**locals()) )
        
        # prep  rule
        myrule = Rule(self.session, body=rule_total['body'], params=rule_total['params'], output=rule_total['output'] )
        
        # exec rule
        try:
            myrule.execute()
            # check that metadata is there
            #returnedMeta = self.session.metadata.get(DataObject, object_path)
            self.log.info(" PID for digitalObject: "+object_path+" is: OK")
        except Exception as ex:
            self.log.info("Could not execute a rule for PID ")
            self.log.info(ex)
            pass        

        return 1 #returnedMeta 


    #
    # irods Rule Execution: REPLICATION   (REP)
    #
    def ruleReplication(self, object_path, target_path, rule_path):

        # check connection
        self._irodsConnect()  
        self.log.info("exec Replication rule inside irods ")
        returnedMeta = {}

        # load rule from file
        rule_total = self.load_rule(rule_path, source='"{object_path}"'.format(**locals()), destination='"{target_path}"'.format(**locals()) )

        # prep  rule
        myrule = Rule(self.session, body=rule_total['body'], params=rule_total['params'], output=rule_total['output'] )
        
        # exec rule
        try:
            myrule.execute()
            # check that metadata is there
            #returnedMeta = self.session.metadata.get(DataObject, object_path)
            self.log.info(" REPLICA for digitalObject: "+object_path+" is: OK")
        except Exception as ex:
            self.log.info("Could not execute a rule for REPLICATION ")
            self.log.info(ex)
            pass

        return 1 #returnedMeta  


    #
    # irods Rule Execution: REMOTE REPLICA REGISTRATION  (RRR)
    #
    def ruleRegistration(self, object_path, target_path, rule_path):
        
        # check connection
        self._irodsConnect()       
        self.log.info("exec  Registration Remote replicated object inside irods ")

        # load rule from file
        rule_total = self.load_rule(rule_path, source='"{object_path}"'.format(**locals()), destination='"{target_path}"'.format(**locals()) )

        # prep  rule
        myrule = Rule(self.session, body=rule_total['body'], params=rule_total['params'], output=rule_total['output'] )
        
        # exec rule
        try:
            myrule.execute()
            # check that metadata is there
            #returnedMeta = self.session.metadata.get(DataObject, object_path)
            self.log.info(" REGISTRATION for digitalObject: "+object_path+" is: OK")
        except Exception as ex:
            self.log.info("Could not execute a rule for REGISTRATION ")
            self.log.info(ex)
            pass

        return 1 #returnedMeta  


    #
    # load irods rule from rule_file.r
    #    
    def load_rule(self, rule_file, **parameters):
        results = {}
        params = {}
        output = ''
        body = '@external\n'

        # parse rule file
        with open(rule_file) as f:
            for line in f:
                # parse input line
                if line.strip().lower().startswith('input'):
                    input_header, input_line = line.split(None, 1)

                    # sanity check
                    if input_header.lower() != 'input':
                        raise ValueError

                    # parse *param0="value0",*param1="value1",...
                    for pair in input_line.split(','):
                        label, value = pair.split('=')
                        params[label.strip()] = value.strip()


                # parse output line
                elif line.strip().lower().startswith('output'):
                    output_header, output_line = line.split(None, 1)

                    # sanity check
                    if output_header.lower() != 'output':
                        raise ValueError

                    # use line as is
                    output = output_line.strip()

                # parse rule
                else:
                    body += line

        # put passed parameters in params            
        for key, value in parameters.items():
            params['*'+key] = value

        results['params']=params
        results['body']=body
        results['output']=output

        return results          


