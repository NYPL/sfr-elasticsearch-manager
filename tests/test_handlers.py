import unittest
from unittest.mock import patch, call, MagicMock
import os

from helpers.errorHelpers import NoRecordsReceived, DataError, DBError

os.environ['DB_USER'] = 'test'
os.environ['DB_PASS'] = 'test'
os.environ['DB_HOST'] = 'test'
os.environ['DB_PORT'] = '1'
os.environ['DB_NAME'] = 'test'
os.environ['ES_INDEX'] = 'test'

# This method is invoked outside of the main handler method as this allows
# us to re-use db connections across Lambda invocations, but it requires a
# little testing weirdness, e.g. we need to mock it on import to prevent errors
with patch('lib.dbManager.dbGenerateConnection') as mock_db:
    from service import handler, parseRecords, parseRecord


class TestHandler(unittest.TestCase):

    @patch('service.parseRecords', return_value=True)
    def test_handler_clean(self, mock_parse):
        testRec = {
            'source': 'SQS',
            'Records': [
                {
                    'Body': '{"type": "work", "identifier": "uuid"}'
                }
            ]
        }
        resp = handler(testRec, None)
        self.assertTrue(resp)

    def test_handler_error(self):
        testRec = {
            'source': 'SQS',
            'Records': []
        }
        with self.assertRaises(NoRecordsReceived):
            handler(testRec, None)

    def test_records_none(self):
        testRec = {
            'source': 'SQQ'
        }
        with self.assertRaises(NoRecordsReceived):
            handler(testRec, None)

   
    sesh = MagicMock()
    @patch('service.ESConnection')
    @patch('service.parseRecord', return_value=True)
    @patch('service.createSession', return_value=sesh)
    def test_parse_records_success(self, mock_sesh, mock_parse, mock_es):
        es_mock = MagicMock(name='es_test')
        es_mock.processBatch.return_value = True
        mock_es.return_value = es_mock
        testRecords = ['rec1', 'rec2']
        res = parseRecords(testRecords)
        mock_parse.assert_has_calls([
            call('rec1', es_mock, TestHandler.sesh),
            call('rec2', es_mock, TestHandler.sesh)
        ])
        es_mock.processBatch.assert_called_once()
        TestHandler.sesh.close.assert_called_once()

    @patch('service.ESConnection')
    @patch('service.parseRecord', side_effect=DataError('test error'))
    def test_parse_records_err(self, mock_parse, mock_es):
        es_mock = MagicMock(name='es_test')
        es_mock.processBatch.return_value = True
        mock_es.return_value = es_mock
        testRecord = ['badRecord']
        res = parseRecords(testRecord)
        self.assertEqual(res, None)

    @patch('service.ESConnection')
    @patch('service.retrieveRecord')
    def test_parse_record_success(self, mock_index, mock_es):
        testJSON = {
            'body': '{"type": "work", "identifier": "a3800805fa64454095c459400c424271"}'
        }
        mock_es.createRecord.return_value = True
        mock_sesh = MagicMock()
        mock_sesh.rollback.return_value = True
        parseRecord(testJSON, mock_es, mock_sesh)
        mock_index.assert_called_once()
        mock_es.createRecord.assert_called_once()
        

    def test_parse_bad_json(self):
        badJSON = {
            'body': '{"type: "work", "identifier": "a3800805fa64454095c459400c424271"}'
        }
        with self.assertRaises(DataError):
            parseRecord(badJSON, 'mockES', 'session')

    def test_parse_missing_field(self):
        missingJSON = {
            'body': '{"type": "work"}'
        }
        with self.assertRaises(DataError):
            parseRecord(missingJSON, 'mockES', 'session')

    @patch('service.retrieveRecord', side_effect=DBError('work', 'Test Error'))
    @patch('service.createSession')
    def test_indexing_error(self, mock_session, mock_index):
        testJSON = {
            'body': '{"type": "work", "identifier": "a3800805fa64454095c459400c424271"}'
        }
        mock_sesh = MagicMock()
        mock_sesh.rollback.return_value = True
        with self.assertRaises(DBError):
            parseRecord(testJSON, 'mockES', mock_sesh)
            mock_session.assert_called_once()
            mock_index.assert_called_once()


if __name__ == '__main__':
    unittest.main()
