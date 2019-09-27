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
    SQS_CLIENT = createAWSClient('sqs')
    SQS_QUEUE = os.environ['INDEX_QUEUE']
    
    def __init__(self):
        pass
    
    def loadUpdates(self, session):
        for work in retrieveRecords(session):
            IndexingManager.putMessage({
                'type': 'uuid',
                'identifier': work.uuid.hex
            })

    @classmethod
    def putMessage(cls, data):
        """This puts record identifiers into an SQS queue that is read for
        records to (re)index in ElasticSearch. Takes an object which is
        converted into a JSON string."""

        logger.info('Writing identifier {}({}) to indexing queue'.format(
            data['identifier'],
            data['type']
        ))

        messageData = json.dumps(data)

        try:
            cls.SQS_CLIENT.send_message(
                QueueUrl=cls.SQS_QUEUE,
                MessageBody=messageData
            )
        except:
            logger.error('SQS Write error!')
            raise OutputError('Failed to write result to output stream!')