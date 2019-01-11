

from pymongo import MongoClient


class MongoDatabase():
  """
  MongoDatabase
  > Main class for interaction with MongoDB
  """

  def __init__(self, CONFIG):
    """
    MongoDatabase.__init__
    > sets the configured host
    """
    print ("mongo wake-up")
    self.host = CONFIG['MONGO']['DB_HOST']
    self._connected = False


  def _connect(self):
    """
    MongoDatabase._connect
    > Sets up connection to the MongoDB
    """

    if self._connected:
      return

    self.client = MongoClient(self.host)
    self.db = self.client[CONFIG['MONGO']['DB_NAME']]

    if CONFIG['MONGO']['AUTHENTICATE']:
      self.db.authenticate(CONFIG['MONGO']['USER'], CONFIG['MONGO']['PASS'])

    self._connected = True

  def getFileDataObject(self, file):
    """
    MongoDatabase.getFileDataObject
    """
    return self.db.wf_do.find({'fileId': os.path.basename(file)})

  def _storeFileDataObject(self, obj):
    """
    MongoDatabase._storeFileDataObject
    stored data object to wf_do collection
    """  
    return self.db.wf_do.save(obj)

  def _storeGranule(self, stream, granule):
    """
    MongoDatabase._storeGranule
    > stores daily and hourly granules to collections
    """

    if granule == 'daily':
      return self.db.daily_streams.save(stream)
    elif granule == 'hourly':
      return self.db.hourly_streams.save(stream)


  def removeDocumentsById(self, id):
    """
    MongoDatabase.removeDocumentsById
    > removes documents all related to ObjectId
    """
    self.db.daily_streams.remove({'_id': id})
    self.db.hourly_streams.remove({'streamId': id})
    self.db.c_segments.remove({'streamId': id})


  def storeContinuousSegment(self, segment):
    """
    MongoDatabase.storeContinuousSegment
    > Saves a continuous segment to collection
    """
    self.db.c_segments.save(segment)


  def getDailyFilesById(self, file):
    """
    MongoDatabase.getDailyFilesById
    returns all documents that include this file in the metadata calculation
    """
    return self.db.daily_streams.find({'files.name': os.path.basename(file)}, {'files': 1, 'fileId': 1, '_id': 1})


  def getDocumentByFilename(self, file):
    """
    MongoDatabase.getDocumentByFilename
    balbal
    """
    return self.db.daily_streams.find({'fileId': os.path.basename(file)})
