#! /usr/bin/env python
"""
#
#
#
  
"""
import os
import sys

from pymongo import MongoClient

#
# class for interaction with MongoDB
#
class MongoDatabase():


  def __init__(self, config, log):

    print ("mongo wake-up")

    self.log = log
    self.config = config
    self.host = self.config['MONGO']['DB_HOST']
    self._connected = False
    print (self.config['MONGO']['DB_HOST'])
    
    #
    # class for interaction with MongoDB
    #
    def _connect(self):
        
        if self._connected:
          return

        self.client = MongoClient(self.host)
        self.db = self.client[self.config['MONGO']['DB_NAME']]

        if self.config['MONGO']['AUTHENTICATE']:
          self.db.authenticate(self.config['MONGO']['USER'], self.config['MONGO']['PASS'])

        self._connected = True
        
    #
    # MongoDatabase.getFileDataObject
    #
    def getFileDataObject(self, file):

        return self.db.wf_do.find({'fileId': os.path.basename(file)})

    #
    # MongoDatabase._storeFileDataObject
    # stored data object to wf_do collection
    #
    def _storeWFDataObject(self, obj):
 
        return self.db.wf_do.save(obj)

    #
    # MongoDatabase._storeGranule
    # stores daily and hourly granules to collections
    #
    def _storeGranule(self, stream, granule):

        if granule == 'daily':
          return self.db.daily_streams.save(stream)
        elif granule == 'hourly':
          return self.db.hourly_streams.save(stream)

    #
    # MongoDatabase.removeDocumentsById
    # removes documents all related to ObjectId
    #
    def removeDocumentsById(self, id):
       
        self.db.daily_streams.remove({'_id': id})
        self.db.hourly_streams.remove({'streamId': id})
        self.db.c_segments.remove({'streamId': id})

    #
    # MongoDatabase.storeContinuousSegment
    # Saves a continuous segment to collection
    #
    def storeContinuousSegment(self, segment):
       
        self.db.c_segments.save(segment)

    #
    # MongoDatabase.getDailyFilesById
    # returns all documents that include this file in the metadata calculation
    #
    def getDailyFilesById(self, file):
        
        return self.db.daily_streams.find({'files.name': os.path.basename(file)}, {'files': 1, 'fileId': 1, '_id': 1})

    #
    # MongoDatabase.getDocumentByFilename
    #
    def getDocumentByFilename(self, file):
        
        return self.db.daily_streams.find({'fileId': os.path.basename(file)})
