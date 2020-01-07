import os
import time
import json
from datetime import datetime, timedelta

from lib.dbManager import retrieveRecords

from helpers.clientHelpers import createAWSClient
from helpers.logHelpers import createLog
from helpers.errorHelpers import OutputError

logger = createLog('indexing_manager')

class IndexingManager():
    SQS_QUEUE = os.environ['INDEX_QUEUE']
    
    def __init__(self):
        self.sqsClient = createAWSClient('sqs')
        self.messages = []

    def loadUpdates(self, session):
        """Fetches records updated in the configured period from the database
        and sends them to the cluster function for indexing.
        
        Arguments:
            session {Object} -- A SqlAlchemy database session object
        """
        for work in retrieveRecords(session):
            self.addMessage(work)
            if len(self.messages) % 10 == 0:
                self.sendMessages()
                self.messages = []
        
        if len(self.messages) > 0:
            self.sendMessages()

    def addMessage(self, work):
        """Adds a work UUID to a list of records to be sent to the clustering
        function for edition clustering and indexing in elasticsearch. Formats
        the record for sending via SQS queue (as a JSON string)
 
        Arguments:
            work {Object} -- A SqlAlchemy database work record
        """
        logger.debug('Adding work {} to output queue'.format(work.uuid.hex))

        messageData = json.dumps({
            'type': 'uuid',
            'identifier': work.uuid.hex
        })
        self.messages.append({
            'Id': work.uuid.hex,
            'MessageBody': messageData
        })

    def sendMessages(self):
        """This puts record identifiers into an SQS queue that is read for
        records to (re)index in ElasticSearch. Takes an object which is
        converted into a JSON string."""
        logger.info('Writing batch of {} records'.format(len(self.messages)))
        try:
            self.sqsClient.send_message_batch(
                QueueUrl=self.SQS_QUEUE,
                Entries=self.messages
            )
        except Exception as err:
            logger.error('Unable to write batch to SQS')
            logger.debug(err)
            raise OutputError('Failed to write batch to clustering queue')
