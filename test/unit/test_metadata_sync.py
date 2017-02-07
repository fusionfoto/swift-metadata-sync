# -*- coding: utf-8 -*-

import email
import json
import mock
import unittest

from swift_metadata_sync import metadata_sync


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
        self.swift_mock = mock.Mock()
        mock_client.return_value = self.swift_mock
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

    @mock.patch('__builtin__.open')
    @mock.patch('swift_metadata_sync.metadata_sync.os.path.exists')
    def test_get_last_row_new_dbid(self, exists_mock, open_mock):
        exists_mock.return_value = True
        status = {'db_id': {'last_row': 42, 'index': self.test_index}}
        open_mock.return_value = self.FakeFile(json.dumps(status))
        self.assertEqual(0, self.sync.get_last_row('bogus-id'))

    @mock.patch('__builtin__.open')
    @mock.patch('swift_metadata_sync.metadata_sync.os.path.exists')
    def test_get_last_row_correct_dbid(self, exists_mock, open_mock):
        exists_mock.return_value = True
        status = {'db_id': {'last_row': 42, 'index': self.test_index}}
        open_mock.return_value = self.FakeFile(json.dumps(status))
        self.assertEqual(42, self.sync.get_last_row('db_id'))

    @mock.patch('__builtin__.open')
    @mock.patch('swift_metadata_sync.metadata_sync.os.path.exists')
    def test_get_last_row_new_index(self, exists_mock, open_mock):
        exists_mock.return_value = True
        status = {'db_id': {'last_row': 42, 'index': 'old-index'}}
        fake_file = self.FakeFile(json.dumps(status))
        open_mock.return_value = fake_file
        self.assertEqual(0, self.sync.get_last_row('db_id'))
        self.assertTrue(fake_file.closed)

    @mock.patch('__builtin__.open')
    @mock.patch('swift_metadata_sync.metadata_sync.os.path.exists')
    def test_get_last_row_malformed_status(self, exists_mock, open_mock):
        exists_mock.return_value = True
        open_mock.return_value = self.FakeFile('')
        self.assertEqual(0, self.sync.get_last_row('db_id'))

    @mock.patch('swift_metadata_sync.metadata_sync.os.mkdir')
    @mock.patch('__builtin__.open')
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
    @mock.patch('__builtin__.open')
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

    @mock.patch('__builtin__.open')
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
        expected_delete_ops = [{
            '_op_type': 'delete',
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
        expected_delete_ops = [{
            '_op_type': 'delete',
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
        helpers_mock.bulk.return_value = (0, [{
            'delete': {'exception': 'not found',
                       'status': 404,
                       'found': False}}])

        self.sync.handle(rows)
        expected_delete_ops = [{
            '_op_type': 'delete',
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
    def test_handle_update_and_new_docs(self, helpers_mock):
        def fake_object_meta(account, container, key, headers={}):
            object_id = int(key.split('_')[1])
            x_timestamp = 1000000 - object_id % 2
            return {'content-length': 42,
                    'content-type': 'application/x-fake',
                    'last-modified': email.utils.formatdate(x_timestamp),
                    'x-timestamp': x_timestamp,
                    'x-object-meta-foo': 'bar'}

        rows = [{'name': 'object_%d' % i,
                 'deleted': False,
                 'created_at': 1000000} for i in range(0, 10)]
        es_docs = {'docs': [{
            '_id': '%s/%s/object_%d' % (
                self.test_account, self.test_container, i),
            # Elasticsearch uses milliseconds
            '_source': {'x-timestamp': 1000000 * 1000 - i % 2},
            'found': True} for i in range(0, 10)]}
        self.sync._es_conn = mock.Mock()
        self.sync._es_conn.mget.return_value = es_docs
        self.swift_mock.get_object_metadata.side_effect = fake_object_meta
        helpers_mock.bulk.return_value = (None, [])

        self.sync.handle(rows)

        expected_ops = [{
            '_op_type': 'index',
            '_index': self.test_index,
            '_type': metadata_sync.MetadataSync.DOC_TYPE,
            '_id': '%s/%s/object_%d' % (
                self.test_account, self.test_container, i),
            '_source': {
                'content-length': 42,
                'content-type': 'application/x-fake',
                'last-modified': 999999*1000,
                'x-swift-account': self.test_account,
                'x-swift-container': self.test_container,
                'x-swift-object': 'object_%d' % i,
                'x-timestamp': 999999*1000,
                'foo': 'bar'
            }
        } for i in range(1, 10, 2)]
        helpers_mock.bulk.assert_called_once_with(
            self.sync._es_conn, expected_ops, raise_on_error=False,
            raise_on_exception=False)
        self.sync._es_conn.mget.assert_called_once_with(
            body={'ids': ['%s/%s/object_%d' % (
                    self.test_account, self.test_container, i)
                    for i in range(0, 10)]},
            index=self.test_index,
            refresh=True,
            _source=['x-timestamp'])

    @mock.patch('swift_metadata_sync.metadata_sync.elasticsearch.helpers')
    def test_handle_unicode_meta(self, helpers_mock):
        def fake_object_meta(account, container, key, headers={}):
            return {'content-length': 42,
                    'content-type': 'application/x-fake',
                    'last-modified': email.utils.formatdate(0),
                    'x-timestamp': 0,
                    'x-object-meta-\xf0\x9f\x90\xb5': '\xf0\x9f\x91\x8d'}

        rows = [{'name': 'object',
                 'deleted': False,
                 'created_at': 1000000}]
        es_docs = {'docs': [{'found': False,
                             '_id': '/'.join([self.test_account,
                                              self.test_container,
                                              'object'])}
                            ]}
        self.sync._es_conn = mock.Mock()
        self.sync._es_conn.mget.return_value = es_docs
        self.swift_mock.get_object_metadata.side_effect = fake_object_meta
        helpers_mock.bulk.return_value = (None, [])

        self.sync.handle(rows)

        expected_ops = [{
            '_op_type': 'index',
            '_index': self.test_index,
            '_type': metadata_sync.MetadataSync.DOC_TYPE,
            '_id': '%s/%s/object' % (
                self.test_account, self.test_container),
            '_source': {
                'content-length': 42,
                'content-type': 'application/x-fake',
                'last-modified': 0,
                'x-swift-account': self.test_account,
                'x-swift-container': self.test_container,
                'x-swift-object': 'object',
                'x-timestamp': 0,
                u'🐵': u'👍'
            }
        }]
        helpers_mock.bulk.assert_called_once_with(
            self.sync._es_conn, expected_ops, raise_on_error=False,
            raise_on_exception=False)
        self.sync._es_conn.mget.assert_called_once_with(
            body={'ids': ['%s/%s/object' % (
                    self.test_account, self.test_container)]},
            index=self.test_index,
            refresh=True,
            _source=['x-timestamp'])

    @mock.patch('container_crawler.base_sync.InternalClient')
    @mock.patch(
        'swift_metadata_sync.metadata_sync.elasticsearch.client.IndicesClient')
    @mock.patch(
        'swift_metadata_sync.metadata_sync.elasticsearch.Elasticsearch')
    def test_verify_mapping(self, es_mock, index_mock, mock_ic_client):
        full_mapping = metadata_sync.MetadataSync.DOC_MAPPING
        swift_type = metadata_sync.MetadataSync.DOC_TYPE

        # List of tuples of mappings to test: the mapping returned by ES and
        # the mapping we expect to submit to the put_mapping call.
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
            metadata_sync.MetadataSync(self.status_dir, self.sync_conf)

            if expected_put_mapping:
                index_conn.put_mapping.assert_called_once_with(
                    index=self.test_index, doc_type=swift_type,
                    body={"properties": expected_put_mapping})
            else:
                index_conn.put_mapping.assert_not_called()

    def test_unicode_document_id(self):
        row = {'name': 'monkey-\xf0\x9f\x90\xb5'}
        doc_id = self.sync._get_document_id(row)
        self.assertEqual(u'/'.join(
            [self.test_account, self.test_container, u'monkey-🐵']), doc_id)

    # For delete and index failures, we should extract the reason if
    # possible or return the status if not possible.
    @mock.patch('swift_metadata_sync.metadata_sync.elasticsearch.helpers')
    def test_delete_errors(self, helpers_mock):
        rows = [{'name': 'object_%d' % i,
                 'deleted': True,
                 'created_at': 1000000} for i in range(0, 10)]
        es_docs = {'docs': [{
            '_id': '%s/%s/object_%d' % (
                self.test_account, self.test_container, i),
            '_source': {'x-timestamp': 1000000 * 1000 - i % 2},
            'found': True} for i in range(0, 10)]}
        self.sync._es_conn = mock.Mock()
        self.sync._es_conn.mget.return_value = es_docs
        helpers_mock.bulk.return_value = (None, [
            {'delete': {'status': 400, '_id': 'object_0'}},
            {'delete': {'status': 400, '_id': 'object_1',
             'error': {'root_cause': 'delete failure reason'}}},
            {'delete': {'status': 400, '_id': 'object_2',
                        'error': {
                            'root_cause': 'delete failed',
                            'caused_by': {'reason': 'more details'}}
                        }}
        ])

        self.sync.logger = mock.Mock()
        with self.assertRaises(RuntimeError):
            self.sync.handle(rows)

        expected_error_calls = [
            mock.call("object_0: 400"),
            mock.call("object_1: delete failure reason"),
            mock.call("object_2: delete failed: more details")
        ]
        self.sync.logger.error.assert_has_calls(expected_error_calls)

    @mock.patch('swift_metadata_sync.metadata_sync.elasticsearch.helpers')
    def test_index_errors(self, helpers_mock):
        rows = [{'name': 'object_%d' % i,
                 'deleted': False,
                 'created_at': 1000000} for i in range(0, 10)]
        es_docs = {'docs': [{
            '_id': '%s/%s/object_%d' % (
                self.test_account, self.test_container, i),
            'found': False} for i in range(0, 10)]}
        self.sync._es_conn = mock.Mock()
        self.sync._es_conn.mget.return_value = es_docs
        helpers_mock.bulk.return_value = (None, [
            {'index': {'status': 400, '_id': 'object_0'}},
            {'index': {'status': 400, '_id': 'object_1',
             'error': {'root_cause': 'index failure reason'}}},
            {'index': {'status': 400, '_id': 'object_2',
                       'error': {
                            'root_cause': 'index failed',
                            'caused_by': {'reason': 'more details'}}
                       }}
        ])
        self.swift_mock.get_object_metadata.return_value = {
            'x-timestamp': 1000000,
            'last-modified': email.utils.formatdate(1000000)
        }

        self.sync.logger = mock.Mock()
        with self.assertRaises(RuntimeError):
            self.sync.handle(rows)

        expected_error_calls = [
            mock.call("object_0: 400"),
            mock.call("object_1: index failure reason"),
            mock.call("object_2: index failed: more details")
        ]
        self.sync.logger.error.assert_has_calls(expected_error_calls)
