import unittest
from unittest.mock import patch, call, MagicMock, DEFAULT
import os
from sfrCore import SessionManager

from helpers.errorHelpers import ESError

os.environ['DB_USER'] = 'test'
os.environ['DB_PSWD'] = 'test'
os.environ['DB_HOST'] = 'test'
os.environ['DB_PORT'] = '1'
os.environ['DB_NAME'] = 'test'


class TestHandler(unittest.TestCase):
    @patch.multiple(
        SessionManager, generateEngine=DEFAULT, decryptEnvVar=DEFAULT
    )
    def setUp(self, generateEngine, decryptEnvVar):
        from service import handler, indexRecords, IndexingManager
        self.handler = handler
        self.indexRecords = indexRecords
        self.indexingManager = IndexingManager

    @patch('service.indexRecords', return_value=True)
    def test_handler_clean(self, mock_index):
        testRec = {
            'source': 'CloudWatch'
        }
        resp = self.handler(testRec, None)
        mock_index.assert_called_once()
        self.assertTrue(resp)

    @patch('lib.esManager.createAWSClient')
    @patch.multiple(SessionManager, createSession=DEFAULT, closeConnection=DEFAULT)
    def test_parse_records_success(self, mockClient, createSession, closeConnection):
        with patch.object(self.indexingManager, 'loadUpdates') as mockUpdates:
            self.indexRecords()
            createSession.assert_called_once()
            mockClient.assert_called_once()
            mockUpdates.assert_called_once()
            closeConnection.assert_called_once()
