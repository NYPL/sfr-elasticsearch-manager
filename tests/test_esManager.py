import json
import os
import unittest
from unittest.mock import patch, MagicMock, call, DEFAULT

os.environ['INDEX_QUEUE'] = 'sqs_test'

from lib.esManager import IndexingManager
from helpers.errorHelpers import OutputError


class TestESManager(unittest.TestCase):
    @patch('lib.esManager.createAWSClient')
    def test_class_create(self, mockClient):
        mockClient.return_value = 'mockSQS'
        inst = IndexingManager()
        self.assertIsInstance(inst, IndexingManager)
        self.assertEqual(inst.messages, [])
        self.assertEqual(inst.sqsClient, 'mockSQS')

    @patch('lib.esManager.createAWSClient')
    @patch('lib.esManager.retrieveRecords', return_value=[i for i in range(15)])
    @patch.multiple(IndexingManager, addMessage=DEFAULT, sendMessages=DEFAULT)
    def test_loadUpdates(self, mockRetrieve, mockClient, addMessage, sendMessages):
        testManager = IndexingManager()

        def addMessageEffect(*args):
            testManager.messages.append(0)
        addMessage.side_effect = addMessageEffect

        testManager.loadUpdates('session')

        mockRetrieve.assert_called_once_with('session')
        self.assertEqual(len(addMessage.mock_calls), 15)
        sendMessages.assert_has_calls([call(), call()])
    
    @patch('lib.esManager.createAWSClient')
    def test_addMessage(self, mockClient):
        testManager = IndexingManager()
        mockWork = MagicMock()
        mockUUID = MagicMock()
        mockUUID.hex = 'xxxx-xxxxxx-xxxx-xxxxxxx'
        mockWork.uuid = mockUUID

        testManager.addMessage(mockWork)
        self.assertEqual(len(testManager.messages), 1)
        testMessage = testManager.messages[0]
        self.assertEqual(testMessage['Id'], 'xxxx-xxxxxx-xxxx-xxxxxxx')
        jsonVal = json.loads(testMessage['MessageBody'])
        self.assertEqual(jsonVal['type'], 'uuid')
        self.assertEqual(jsonVal['identifier'], 'xxxx-xxxxxx-xxxx-xxxxxxx')

    @patch('lib.esManager.createAWSClient')
    def test_sendMessages(self, mockClient):
        mockSQS = MagicMock()
        mockClient.return_value = mockSQS
        testManager = IndexingManager()
        testManager.messages = [1, 2, 3]
        testManager.sendMessages()

        mockSQS.send_message_batch.assert_called_once_with(
            QueueUrl='sqs_test',
            Entries=[1, 2, 3]
        )
    
    @patch('lib.esManager.createAWSClient')
    def test_sendMessages_err(self, mockClient):
        mockSQS = MagicMock()
        mockClient.return_value = mockSQS
        mockSQS.send_message_batch.side_effect = IndexError
        testManager = IndexingManager()
        with self.assertRaises(OutputError):
            testManager.sendMessages()
