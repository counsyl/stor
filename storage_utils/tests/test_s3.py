import unittest

from botocore.exceptions import ClientError
import mock

from storage_utils import exceptions
from storage_utils import Path
from storage_utils.experimental import s3
from storage_utils.experimental.s3 import S3Path
from storage_utils.test import S3TestCase


class TestBasicPathMethods(unittest.TestCase):
    def test_name(self):
        p = Path('s3://bucket/path/to/resource')
        self.assertEquals(p.name, 'resource')

    def test_parent(self):
        p = Path('s3://bucket/path/to/resource')
        self.assertEquals(p.parent, 's3://bucket/path/to')

    def test_dirname(self):
        p = Path('s3://bucket/path/to/resource')
        self.assertEquals(p.dirname(), 's3://bucket/path/to')

    def test_basename(self):
        p = Path('swift://bucket/path/to/resource')
        self.assertEquals(p.basename(), 'resource')


class TestValidation(unittest.TestCase):
    def test_new_failed(self):
        with self.assertRaises(ValueError):
            S3Path('bad/s3/path')

    def test_new_successful(self):
        s3_p = S3Path('s3://bucket/path/to/resource')
        self.assertEquals(s3_p, 's3://bucket/path/to/resource')


class TestRepr(unittest.TestCase):
    def test_repr(self):
        s3_p = S3Path('s3://a/b/c')
        self.assertEquals(eval(repr(s3_p)), s3_p)


class TestPathManipulation(unittest.TestCase):
    def test_add(self):
        s3_p = S3Path('s3://a')
        s3_p = s3_p + 'b' + Path('c')
        self.assertTrue(isinstance(s3_p, S3Path))
        self.assertEquals(s3_p, 's3://abc')

    def test_div(self):
        s3_p = S3Path('s3://b')
        s3_p = s3_p / 'p' / Path('k')
        self.assertTrue(isinstance(s3_p, S3Path))
        self.assertEquals(s3_p, 's3://b/p/k')


class TestBucket(unittest.TestCase):
    def test_bucket_none(self):
        s3_p = S3Path('s3://')
        self.assertIsNone(s3_p.bucket)

    def test_bucket_exists(self):
        s3_p = S3Path('s3://bucket')
        self.assertEquals(s3_p.bucket, 'bucket')


class TestResource(unittest.TestCase):
    def test_resource_none_no_bucket(self):
        s3_p = S3Path('s3://')
        self.assertIsNone(s3_p.resource)

    def test_resource_none_w_bucket(self):
        s3_p = S3Path('s3://bucket/')
        self.assertIsNone(s3_p.resource)

    def test_resource_object(self):
        s3_p = S3Path('s3://bucket/obj')
        self.assertEquals(s3_p.resource, 'obj')

    def test_resource_single_dir(self):
        s3_p = S3Path('s3://bucket/dir/')
        self.assertEquals(s3_p.resource, 'dir/')

    def test_resource_nested_obj(self):
        s3_p = S3Path('s3://bucket/nested/obj')
        self.assertEquals(s3_p.resource, 'nested/obj')

    def test_resource_nested_dir(self):
        s3_p = S3Path('s3://bucket/nested/dir/')
        self.assertEquals(s3_p.resource, 'nested/dir/')


@mock.patch('storage_utils.experimental.s3._thread_local', mock.Mock())
class TestGetS3Client(S3TestCase):
    def test_get_s3_client_exists(self):
        s3._thread_local.s3_client = 'client'
        self.disable_get_s3_client_mock()
        client = s3._get_s3_client()
        self.assertEquals(client, 'client')
        del s3._thread_local.s3_client

    def test_get_s3_client_none(self):
        self.disable_get_s3_client_mock()
        client = s3._get_s3_client()
        self.mock_s3_client.assert_called_once_with('s3')
        self.assertEquals(client, self.mock_s3_client.return_value)


class TestGetS3Iterator(S3TestCase):
    def test_get_s3_iterator(self):
        mock_paginator = self.mock_s3.get_paginator.return_value
        mock_paginate = mock_paginator.paginate
        self.disable_get_s3_iterator_mock()
        s3_p = S3Path('s3://test-bucket')
        s3_p._get_s3_iterator('method', key='val')
        mock_paginate.assert_called_once_with(key='val')


class TestS3ClientCall(S3TestCase):
    def test_s3_client_call(self):
        mock_method = self.mock_s3.method
        s3_p = S3Path('s3://test-bucket/path')
        s3_p._s3_client_call('method', key='val')
        mock_method.assert_called_once_with(key='val')

    def test_s3_client_call_any_error(self):
        mock_method = self.mock_s3.method
        mock_method.side_effect = ClientError({'Error': {}}, 'method')
        s3_p = S3Path('s3://test/path')
        with self.assertRaises(exceptions.RemoteError):
            s3_p._s3_client_call('method', key='val')

    def test_s3_client_call_unauthorized(self):
        mock_method = self.mock_s3.method
        mock_method.side_effect = ClientError(
            {
                'ResponseMetadata': {'HTTPStatusCode': 403},
                'Error': {'Message': 'unauthorized'}
            },
            'method')
        s3_p = S3Path('s3://test/path')
        with self.assertRaises(exceptions.UnauthorizedError):
            s3_p._s3_client_call('method', key='val')

    def test_s3_client_call_unavailable(self):
        mock_method = self.mock_s3.method
        mock_method.side_effect = ClientError(
            {
                'ResponseMetadata': {'HTTPStatusCode': 503},
                'Error': {'Message': 'unavailable'}
            },
            'method')
        s3_p = S3Path('s3://test/path')
        with self.assertRaises(exceptions.UnavailableError):
            s3_p._s3_client_call('method', key='val')


class TestList(S3TestCase):
    def test_list_no_bucket_error(self):
        mock_list = self.mock_s3_iterator
        mock_list.__iter__.side_effect = ClientError(
            {
                'ResponseMetadata': {'HTTPStatusCode': 404},
                'Error': {'Message': 'The specified bucket does not exist'}
            },
            'list_objects_v2')

        s3_p = S3Path('s3://bucket/key')
        with self.assertRaises(exceptions.NotFoundError):
            s3_p.list()

    def test_list_unknown_error(self):
        mock_list = self.mock_s3_iterator
        mock_list.__iter__.side_effect = ClientError(
            {
                'Error': {'Message': 'Unspecified error'}
            },
            'list_objects_v2')

        s3_p = S3Path('s3://bucket/key')
        with self.assertRaises(exceptions.RemoteError):
            s3_p.list()

    def test_list_bucket(self):
        mock_list = self.mock_s3_iterator
        mock_list.__iter__.return_value = [{
            'Contents': [{'Key': 'key1'}, {'Key': 'key2'}, {'Key': 'prefix/key3'}],
            'IsTruncated': False
        }]
        s3_p = S3Path('s3://test-bucket')
        results = s3_p.list()

        self.assertEquals(results, [
            's3://test-bucket/key1',
            's3://test-bucket/key2',
            's3://test-bucket/prefix/key3'
        ])

    def test_list_no_content(self):
        mock_list = self.mock_s3_iterator
        mock_list.__iter__.return_value = [{
            'IsTruncated': False,
            'KeyCount': 0
        }]
        s3_p = S3Path('s3://test-bucket/key')
        results = s3_p.list()

        self.assertEquals(results, [])

    def test_list_prefix(self):
        mock_list = self.mock_s3_iterator
        mock_list.__iter__.return_value = [{
            'Contents': [{'Key': 'prefix/key1'}, {'Key': 'prefix/key2'}, {'Key': 'prefix/key3'}],
            'IsTruncated': False
        }]
        s3_p = S3Path('s3://test-bucket/prefix')
        results = s3_p.list()

        self.assertEquals(results, [
            's3://test-bucket/prefix/key1',
            's3://test-bucket/prefix/key2',
            's3://test-bucket/prefix/key3'
        ])

    def test_list_starts_with_no_prefix(self):
        mock_list = self.mock_s3_iterator
        mock_list.__iter__.return_value = [{
            'Contents': [{'Key': 'prefix/key1'}, {'Key': 'prefix/key2'}, {'Key': 'prefix/key3'}],
            'IsTruncated': False
        }]
        s3_p = S3Path('s3://test-bucket')
        results = s3_p.list(starts_with='prefix')

        self.assertEquals(results, [
            's3://test-bucket/prefix/key1',
            's3://test-bucket/prefix/key2',
            's3://test-bucket/prefix/key3'
        ])

    def test_list_starts_with_prefix(self):
        mock_list = self.mock_s3_iterator
        mock_list.__iter__.return_value = [{
            'Contents': [
                {'Key': 'prefix1/prefix/key1'},
                {'Key': 'prefix1/prefix/key2'},
                {'Key': 'prefix1/prefix/key3'}
            ],
            'IsTruncated': False
        }]
        s3_p = S3Path('s3://test-bucket/prefix1')
        results = s3_p.list(starts_with='prefix')

        self.assertEquals(results, [
            's3://test-bucket/prefix1/prefix/key1',
            's3://test-bucket/prefix1/prefix/key2',
            's3://test-bucket/prefix1/prefix/key3'
        ])

    def test_list_over_1000_limit(self):
        mock_list = self.mock_s3_iterator
        mock_list.__iter__.return_value = [{
            'Contents': [{'Key': str(i) + '.txt'} for i in range(1000)],
            'IsTruncated': True,
            'KeyCount': 1000,
            'MaxKeys': 1234,
            'NextContinuationToken': 'token1'
        }, {
            'Contents': [{'Key': str(i + 1000) + '.txt'} for i in range(234)],
            'IsTruncated': True,
            'KeyCount': 234,
            'MaxKeys': 234
        }]
        s3_p = S3Path('s3://test-bucket')
        results = s3_p.list(limit=1234)
        self.assertEquals(len(results), 1234)

    def test_list_over_1000_no_limit(self):
        mock_list = self.mock_s3_iterator
        mock_list.__iter__.return_value = [{
            'Contents': [{'Key': str(i) + '.txt'} for i in range(1000)],
            'IsTruncated': True,
            'KeyCount': 1000,
            'NextContinuationToken': 'token1'
        }, {
            'Contents': [{'Key': str(i + 1000) + '.txt'} for i in range(1000)],
            'IsTruncated': True,
            'KeyCount': 1000,
            'NextContinuationToken': 'token2'
        }, {
            'Contents': [{'Key': str(i + 2000) + '.txt'} for i in range(234)],
            'IsTruncated': False,
            'KeyCount': 234
        }]
        s3_p = S3Path('s3://test-bucket')
        results = s3_p.list()
        self.assertEquals(len(results), 2234)

    def test_listdir(self):
        mock_list = self.mock_s3_iterator
        mock_list.__iter__.return_value = [{
            'Contents': [
                {'Key': 'prefix1/prefix/key1'},
                {'Key': 'prefix1/prefix/key2'},
                {'Key': 'prefix1/prefix/key3'}
            ],
            'CommonPrefixes': [
                {'Prefix': 'prefix1/prefix/p2/'},
                {'Prefix': 'prefix1/prefix/p3/'}
            ],
            'IsTruncated': False
        }]

        s3_p = S3Path('s3://test-bucket/prefix1/prefix')
        results = s3_p.listdir()
        self.assertEquals(results, [
            's3://test-bucket/prefix1/prefix/key1',
            's3://test-bucket/prefix1/prefix/key2',
            's3://test-bucket/prefix1/prefix/key3',
            's3://test-bucket/prefix1/prefix/p2/',
            's3://test-bucket/prefix1/prefix/p3/'
        ])

    def test_listdir_on_bucket(self):
        mock_list = self.mock_s3_iterator
        mock_list.__iter__.return_value = [{
            'Contents': [
                {'Key': 'key1'},
                {'Key': 'key2'},
                {'Key': 'key3'}
            ],
            'CommonPrefixes': [
                {'Prefix': 'prefix1/'},
                {'Prefix': 'prefix2/'}
            ],
            'IsTruncated': False
        }]

        s3_p = S3Path('s3://test-bucket/')
        results = s3_p.listdir()
        self.assertEquals(results, [
            's3://test-bucket/key1',
            's3://test-bucket/key2',
            's3://test-bucket/key3',
            's3://test-bucket/prefix1/',
            's3://test-bucket/prefix2/'
        ])

    def test_listdir_no_content(self):
        mock_list = self.mock_s3_iterator
        mock_list.__iter__.return_value = [{
            'CommonPrefixes': [
                {'Prefix': 'p/prefix1/'},
                {'Prefix': 'p/prefix2/'}
            ],
            'IsTruncated': False
        }]

        s3_p = S3Path('s3://test-bucket/p/')
        results = s3_p.listdir()
        self.assertEquals(results, [
            's3://test-bucket/p/prefix1/',
            's3://test-bucket/p/prefix2/'
        ])


class TestWalkFiles(S3TestCase):
    def test_walkfiles_no_pattern(self):
        mock_list = self.mock_s3_iterator
        mock_list.__iter__.return_value = [{
            'Contents': [
                {'Key': 'a/b/c1'},
                {'Key': 'a/b2.py'},
                {'Key': 'a/b/c2'},
                {'Key': 'a3.py'},
                {'Key': 'a/bpy/c3'}
            ],
            'IsTruncated': False
        }]

        s3_p = S3Path('s3://bucket')
        results = list(s3_p.walkfiles())
        self.assertEquals(results, [
            S3Path('s3://bucket/a/b/c1'),
            S3Path('s3://bucket/a/b2.py'),
            S3Path('s3://bucket/a/b/c2'),
            S3Path('s3://bucket/a3.py'),
            S3Path('s3://bucket/a/bpy/c3')
        ])

    def test_walkfiles_w_pattern(self):
        mock_list = self.mock_s3_iterator
        mock_list.__iter__.return_value = [{
            'Contents': [
                {'Key': 'a/b/c1'},
                {'Key': 'a/b2.py'},
                {'Key': 'a/b/c2'},
                {'Key': 'a3.py'},
                {'Key': 'a/bpy/c3'}
            ],
            'IsTruncated': False
        }]

        s3_p = S3Path('s3://bucket')
        results = list(s3_p.walkfiles('*.py'))
        self.assertEquals(results, [
            S3Path('s3://bucket/a/b2.py'),
            S3Path('s3://bucket/a3.py')
        ])


class TestIsMethods(S3TestCase):
    def test_isabs(self):
        self.assertTrue(S3Path('s3://a/b/c').isabs())

    def test_islink(self):
        self.assertFalse(S3Path('s3://a/b/c').islink())

    def test_ismount(self):
        self.assertTrue(S3Path('s3://a/b/c').ismount())
