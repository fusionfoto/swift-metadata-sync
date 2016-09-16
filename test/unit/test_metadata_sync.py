from swift_metadata_sync import metadata_sync
import json
import mock
import unittest

class TestMetadataSync(unittest.TestCase):

    class FakeFile(object):
        def __init__(self, content):
            self.content = content
            self.closed = None

        def read(self, size=-1):
            return self.content

        def __enter__(self):
            if self.closed:
                raise RuntimeError
            self.closed = False
            return self

        def __exit__(self, *args):
            self.closed = True

    @mock.patch(
        'swift_metadata_sync.metadata_sync.MetadataSync._verify_mapping')
    @mock.patch('container_crawler.base_sync.InternalClient')
    def setUp(self, mock_client, mock_verify_mapping):
        self.status_dir = '/status/dir'
        self.es_hosts = 'es.example.com'
        self.test_index = 'test_index'
        self.test_account = 'test_account'
        self.test_container = 'test_container'
        self.sync_conf = {'es_hosts': self.es_hosts,
                          'index': self.test_index,
                          'account': self.test_account,
                          'container': self.test_container}
        self.sync = metadata_sync.MetadataSync(self.status_dir,
                                               self.sync_conf)

    @mock.patch('swift_metadata_sync.metadata_sync.os.path.exists')
    def test_get_last_row_nonexistent(self, exists_mock):
        exists_mock.return_value = False
        self.assertEqual(0, self.sync.get_last_row('bogus-id'))

    @mock.patch('swift_metadata_sync.metadata_sync.open')
    @mock.patch('swift_metadata_sync.metadata_sync.os.path.exists')
    def test_get_last_row_new_dbid(self, exists_mock, open_mock):
        exists_mock.return_value = True
        status = {'db_id': {'last_row': 42, 'index': self.test_index}}
        open_mock.return_value = self.FakeFile(json.dumps(status))
        self.assertEqual(0, self.sync.get_last_row('bogus-id'))

    @mock.patch('swift_metadata_sync.metadata_sync.open')
    @mock.patch('swift_metadata_sync.metadata_sync.os.path.exists')
    def test_get_last_row_correct_dbid(self, exists_mock, open_mock):
        exists_mock.return_value = True
        status = {'db_id': {'last_row': 42, 'index': self.test_index}}
        open_mock.return_value = self.FakeFile(json.dumps(status))
        self.assertEqual(42, self.sync.get_last_row('db_id'))

    @mock.patch('swift_metadata_sync.metadata_sync.open')
    @mock.patch('swift_metadata_sync.metadata_sync.os.path.exists')
    def test_get_last_row_new_index(self, exists_mock, open_mock):
        exists_mock.return_value = True
        status = {'db_id': {'last_row': 42, 'index': 'old-index'}}
        open_mock.return_value = self.FakeFile(json.dumps(status))
        self.assertEqual(0, self.sync.get_last_row('db_id'))

    @mock.patch('swift_metadata_sync.metadata_sync.open')
    @mock.patch('swift_metadata_sync.metadata_sync.os.path.exists')
    def test_get_last_row_malformed_status(self, exists_mock, open_mock):
        exists_mock.return_value = True
        open_mock.return_value = self.FakeFile('')
        self.assertEqual(0, self.sync.get_last_row('db_id'))

    @mock.patch('container_crawler.base_sync.InternalClient')
    @mock.patch(
        'swift_metadata_sync.metadata_sync.elasticsearch.client.IndicesClient')
    @mock.patch(
        'swift_metadata_sync.metadata_sync.elasticsearch.Elasticsearch')
    def test_verify_mapping(self, es_mock, index_mock, mock_ic_client):
        full_mapping = metadata_sync.MetadataSync.DOC_MAPPING
        swift_type = metadata_sync.MetadataSync.DOC_TYPE

        # List of tuples of mappings to test: the mapping returned by ES and the
        # mapping we expect to submit to the put_mapping call.
        test_mappings = [
            ({self.test_index: {"mappings": {}}}, full_mapping),
            ({self.test_index: {"mappings": {"bogus_type": full_mapping}}},
             full_mapping),
            ({self.test_index: {"mappings": {swift_type: {
                "properties": {
                    "content-length": {"type": "string"},
                    "x-timestamp": {"type": "string"},
                    "x-trans-id": {"type": "string"}
                }
            }}}}, {
                "content-type": {"type": "string"},
                "last-modified": {"type": "date"},
                "etag": {"type": "string", "index": "not_analyzed"},
                "x-object-manifest": {"type": "string"},
                "x-static-large-object": {"type": "boolean"},
                "x-swift-container": {"type": "string"},
                "x-swift-account": {"type": "string"},
                "x-swift-object": {"type": "string"},
            }),
            ({self.test_index: {"mappings": {swift_type: {
                "properties": full_mapping}
             }}}, {})
        ]

        for return_mapping, expected_put_mapping in test_mappings:
            es_conn = mock.Mock()
            index_conn = mock.Mock()
            index_conn.get_mapping.return_value = return_mapping
            es_mock.return_value = es_conn
            index_mock.return_value = index_conn
            sync = metadata_sync.MetadataSync(self.status_dir, self.sync_conf)

            if expected_put_mapping:
                index_conn.put_mapping.assert_called_once_with(
                    index=self.test_index, doc_type=swift_type,
                    body={"properties": expected_put_mapping})
            else:
                index_conn.put_mapping.assert_not_called()
