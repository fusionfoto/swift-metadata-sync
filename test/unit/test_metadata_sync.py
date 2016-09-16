from swift_metadata_sync import metadata_sync
import json
import mock
import unittest

class TestMetadataSync(unittest.TestCase):

    class FakeFile(object):
        def __init__(self, content):
            self.closed = None
            self.data = content

        def read(self, size=-1):
            return self.data

        def write(self, data):
            self.data += data

        def seek(self, pos, flags=None):
            if pos != 0:
                raise RuntimeError
            self.data = ''

        def truncate(self):
            return

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
        fake_file = self.FakeFile(json.dumps(status))
        open_mock.return_value = fake_file
        self.assertEqual(0, self.sync.get_last_row('db_id'))
        self.assertTrue(fake_file.closed)

    @mock.patch('swift_metadata_sync.metadata_sync.open')
    @mock.patch('swift_metadata_sync.metadata_sync.os.path.exists')
    def test_get_last_row_malformed_status(self, exists_mock, open_mock):
        exists_mock.return_value = True
        open_mock.return_value = self.FakeFile('')
        self.assertEqual(0, self.sync.get_last_row('db_id'))

    @mock.patch('swift_metadata_sync.metadata_sync.os.mkdir')
    @mock.patch('swift_metadata_sync.metadata_sync.open')
    @mock.patch('swift_metadata_sync.metadata_sync.os.path.exists')
    def test_save_last_row_dir_does_not_exist(self, exists_mock, open_mock,
            mkdir_mock):
        exists_mock.return_value = False
        fake_file = self.FakeFile('')
        open_mock.return_value = fake_file
        self.sync.save_last_row(42, 'db-id')

        mkdir_mock.assert_called_once_with(self.sync._status_account_dir)
        self.assertTrue(fake_file.closed)
        status = json.loads(fake_file.data)
        self.assertIn('db-id', status)
        self.assertEqual(42, status['db-id']['last_row'])
        self.assertEqual(self.test_index, status['db-id']['index'])

    @mock.patch('swift_metadata_sync.metadata_sync.os.mkdir')
    @mock.patch('swift_metadata_sync.metadata_sync.open')
    @mock.patch('swift_metadata_sync.metadata_sync.os.path.exists')
    def test_save_last_row_does_not_exist(self, exists_mock, open_mock,
                                          mkdir_mock):
        def fake_exists(path):
            if path.endswith(self.sync._account):
                return True
            return False

        fake_file = self.FakeFile('')
        exists_mock.side_effect = fake_exists
        open_mock.return_value = fake_file
        self.sync.save_last_row(42, 'db-id')

        mkdir_mock.assert_not_called()
        self.assertTrue(fake_file.closed)
        status = json.loads(fake_file.data)
        self.assertIn('db-id', status)
        self.assertEqual(42, status['db-id']['last_row'])
        self.assertEqual(self.test_index, status['db-id']['index'])

    @mock.patch('swift_metadata_sync.metadata_sync.open')
    @mock.patch('swift_metadata_sync.metadata_sync.os.path.exists')
    def test_save_last_row_new_db_id(self, exists_mock, open_mock):
        old_status = {'old_id': {'last_row': 1, 'index': self.test_index}}
        fake_file = self.FakeFile(json.dumps(old_status))
        exists_mock.return_value = True
        open_mock.return_value = fake_file

        self.sync.save_last_row(42, 'new-id')
        self.assertTrue(fake_file.closed)
        status = json.loads(fake_file.data)
        self.assertIn('new-id', status)
        self.assertIn('old_id', status)
        self.assertEqual(42, status['new-id']['last_row'])
        self.assertEqual(self.test_index, status['new-id']['index'])
        self.assertEqual(1, status['old_id']['last_row'])
        self.assertEqual(self.test_index, status['old_id']['index'])

    @mock.patch('swift_metadata_sync.metadata_sync.elasticsearch.helpers')
    def test_handle_delete(self, helpers_mock):
        rows = [{'name': 'row %d' % i, 'deleted': True} for i in range(0, 10)]
        helpers_mock.bulk.return_value = (None, [])

        self.sync.handle(rows)
        expected_delete_ops = [
            {'_op_type': 'delete',
             '_id': '/'.join([self.test_account, self.test_container,
                              row['name']]),
             '_index': self.test_index,
             '_type': metadata_sync.MetadataSync.DOC_TYPE
            } for row in rows]
        helpers_mock.bulk.assert_called_once_with(self.sync._es_conn,
                                                  expected_delete_ops,
                                                  raise_on_error=False,
                                                  raise_on_exception=False)

    @mock.patch('swift_metadata_sync.metadata_sync.elasticsearch.helpers')
    def test_handle_delete_errors(self, helpers_mock):
        rows = [{'name': 'row %d' % i, 'deleted': True} for i in range(0, 10)]
        helpers_mock.bulk.return_value = (0,
                                          [{'delete': {'exception': 'blow up!',
                                                       'status': 500}},
                                           {'delete': {'_id': 'fake doc id',
                                                       'status': 500}}])

        with self.assertRaises(RuntimeError):
            self.sync.handle(rows)
        expected_delete_ops = [
            {'_op_type': 'delete',
             '_id': '/'.join([self.test_account, self.test_container,
                              row['name']]),
             '_index': self.test_index,
             '_type': metadata_sync.MetadataSync.DOC_TYPE
            } for row in rows]
        helpers_mock.bulk.assert_called_once_with(self.sync._es_conn,
                                                  expected_delete_ops,
                                                  raise_on_error=False,
                                                  raise_on_exception=False)

    @mock.patch('swift_metadata_sync.metadata_sync.elasticsearch.helpers')
    def test_handle_delete_skip_404(self, helpers_mock):
        rows = [{'name': 'row %d' % i, 'deleted': True} for i in range(0, 10)]
        helpers_mock.bulk.return_value = (0,
                                          [{'delete': {'exception': 'not found',
                                                       'status': 404,
                                                       'found': False}}])

        self.sync.handle(rows)
        expected_delete_ops = [
            {'_op_type': 'delete',
             '_id': '/'.join([self.test_account, self.test_container,
                              row['name']]),
             '_index': self.test_index,
             '_type': metadata_sync.MetadataSync.DOC_TYPE
            } for row in rows]
        helpers_mock.bulk.assert_called_once_with(self.sync._es_conn,
                                                  expected_delete_ops,
                                                  raise_on_error=False,
                                                  raise_on_exception=False)

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
