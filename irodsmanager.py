#! /usr/bin/env python
"""
#
#
#
  
"""

import os
import json

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

"""
#
# irodsManager
#
"""
class irodsDAO():


    def __init__(self, config, log):

        print("irods wake-up")
        self.session = None
        self.log = log
        self.config = config


    def _irodsConnect(self):

        if self.session:
            return

        self.log.info("try connection to irods")
        # Make iRODS connection
        self.session = iRODSSession(host=str(self.config['IRODS']['HOST']), port=str(self.config['IRODS']['PORT']), user=str(self.config['IRODS']['USER']), password=str(self.config['IRODS']['PWD']), zone=str(self.config['IRODS']['ZONE']))
        self.log.info("done")

    def doRegister(self, dirname, collname, filename):
       

        self._irodsConnect()

        obj_file = os.path.join(dirname, filename)
        obj_path = '{collname}/{filename}'.format(**locals())
        self._checkCollExsist(collname)
        
        # register file in test collection
        
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
       

    def _ruleExec(self, rule_file_path):

        #self._irodsConnect()

        # run  rule
        myrule = Rule(self.session, rule_file_path)
        ruleout = myrule.execute()

        return ruleout


    def _checkCollExsist(self, collname):

        #self._irodsConnect()
        #coll = self.sess.collections.get(collection)
        self.log.info("check or create a collection recursively : "+collname)
        try:
            self.session.collections.create(collname, recurse=True)
            self.log.info("collection created! : "+collname)
        except Exception as ex:
            self.log.error("Could not create a collection recursively ")
            self.log.error(ex)
            pass

    def getObject(self, obj_path):

        self._irodsConnect()

        return self.session.data_objects.get(obj_path)

