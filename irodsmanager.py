#! /usr/bin/env python

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

# Load configuration from JSON
cfg_dir = os.path.dirname(os.path.realpath(__file__))
print (cfg_dir)
with open(os.path.join(cfg_dir, 'config.json'), "r") as cfg:
  CONFIG = json.load(cfg)

class irodsUtil():

    def __init__(self, CONFIG):
        """
        irodsManager.__init__
       
        """
        print("irods wake-up")
        #self._irodsConnect = False


    def _irodsConnect(self):
        # Make iRODS connection
        self.session = iRODSSession(host=str(CONFIG['IRODS']['HOST']), port=str(CONFIG['IRODS']['PORT']), user=str(CONFIG['IRODS']['USER']), password=str(CONFIG['IRODS']['PWD']), zone=str(CONFIG['IRODS']['ZONE']))


    def _register(self, dirname, filename):
        # skip if server is remote
        #if self.sess.host not in ('localhost', socket.gethostname()):
        #    self.skipTest('Requires access to server-side file(s)')

        self._irodsConnect()
        #print (self.session.user)
        #print (self.session.usertype)
        # test vars
        #dirname = '/opt/test'
        #filename =  '5MB.zip'
        #filename =  'NI.ACOM..HHE.D.2015.006'
        test_file = os.path.join(dirname, filename)
        collection = CONFIG['IRODS']['BASE_PATH']   #'/BINGV/home/rods'
        obj_path = '{collection}/{filename}'.format(**locals())
        
        print ("path " + obj_path)
        print ("file " + test_file)
        # make random 4K binary file
        #with open(test_file, 'wb') as f:
        #    f.write(os.urandom(1024 * 4))

        # register file in test collection
        self.session.data_objects.register(test_file, obj_path)

        # confirm object presence
        obj = self.session.data_objects.get(obj_path)
        print("registred!")
        print (obj)
        # in a real use case we would likely
        # want to leave the physical file on disk
        #obj.unregister()

        # delete file
        #os.remove(test_file)

    def _ruleExec(self, rule_file_path):
        # skip if server is remote

        self._irodsConnect()
        # run  rule
        myrule = Rule(self.session, rule_file_path)
        ciccio = myrule.execute()
        return ciccio

