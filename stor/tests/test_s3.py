import cStringIO
import datetime
import gzip
import ntpath
from tempfile import NamedTemporaryFile
import unittest

from boto3.exceptions import RetriesExceededError
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError
import freezegun
import mock
from testfixtures import LogCapture

import stor
from stor import exceptions
from stor import NamedTemporaryDirectory
from stor.obs import OBSUploadObject
from stor import Path
from stor import obs
from stor import settings
from stor import s3
from stor.s3 import S3Path
from stor.test import S3TestCase
from stor import utils
from stor.tests.shared import assert_same_data


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
        p = Path('s3://bucket/path/to/resource')
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


@mock.patch('stor.s3._thread_local', mock.Mock())
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
        self.mock_s3_session.assert_called_once_with()
        self.mock_s3_session.return_value.client.assert_called_once_with('s3')
        self.assertEquals(client, self.mock_s3_session.return_value.client.return_value)


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

    def test_list_condition(self):
        mock_list = self.mock_s3_iterator
        mock_list.__iter__.return_value = [{
            'Contents': [
                {'Key': 'path/to/resource1'},
                {'Key': 'path/to/resource2'}
            ],
            'IsTruncated': False
        }]

        s3_p = S3Path('s3://bucket/path')
        s3_p.list(condition=lambda results: len(results) == 2)

        mock_list.__iter__.return_value = [{
            'Contents': [
                {'Key': 'path/to/resource1'},
                {'Key': 'path/to/resource2'}
            ],
            'IsTruncated': False
        }]
        with self.assertRaises(exceptions.ConditionNotMetError):
            s3_p.list(condition=lambda results: len(results) == 3)

    @mock.patch('botocore.response.StreamingBody', autospec=True)
    def test_list_w_use_manifest(self, mock_stream):
        mock_stream.read.return_value = 'my/obj1\nmy/obj2\nmy/obj3\n'
        self.mock_s3.get_object.return_value = {'Body': mock_stream}
        mock_list = self.mock_s3_iterator
        mock_list.__iter__.return_value = [{
            'Contents': [
                {'Key': 'my/obj1'},
                {'Key': 'my/obj2'},
                {'Key': 'my/obj3'}
            ],
            'IsTruncated': False
        }]

        s3_p = S3Path('s3://bucket')
        results = s3_p.list(use_manifest=True)
        self.assertEquals(set(results), set([
            's3://bucket/my/obj1',
            's3://bucket/my/obj2',
            's3://bucket/my/obj3'
        ]))

    @mock.patch('botocore.response.StreamingBody', autospec=True)
    def test_list_w_use_manifest_validation_err(self, mock_stream):
        mock_stream.read.return_value = 'my/obj1\nmy/obj2\nmy/obj3\n'
        self.mock_s3.get_object.return_value = {'Body': mock_stream}
        mock_list = self.mock_s3_iterator
        mock_list.__iter__.return_value = [{
            'Contents': [
                {'Key': 'my/obj1'},
                {'Key': 'my/obj2'}
            ],
            'IsTruncated': False
        }]

        s3_p = S3Path('s3://bucket')
        with self.assertRaises(exceptions.ConditionNotMetError):
            s3_p.list(use_manifest=True)

    @mock.patch('botocore.response.StreamingBody', autospec=True)
    def test_list_w_condition_and_use_manifest(self, mock_stream):
        mock_stream.read.return_value = 'my/obj1\nmy/obj2\nmy/obj3\n'
        self.mock_s3.get_object.return_value = {'Body': mock_stream}
        mock_list = self.mock_s3_iterator
        mock_list.__iter__.return_value = [{
            'Contents': [
                {'Key': 'my/obj1'},
                {'Key': 'my/obj2'},
                {'Key': 'my/obj3'}
            ],
            'IsTruncated': False
        }]

        s3_p = S3Path('s3://bucket')
        results = s3_p.list(use_manifest=True, condition=lambda results: len(results) == 3)
        self.assertEquals(set(results), set([
            's3://bucket/my/obj1',
            's3://bucket/my/obj2',
            's3://bucket/my/obj3'
        ]))

    def test_list_ignore_dir_markers(self):
        mock_list = self.mock_s3_iterator
        mock_list.__iter__.return_value = [{
            'Contents': [
                {'Key': 'pre/key1'},
                {'Key': 'pre/key2'},
                {'Key': 'dir/marker/'},
                {'Key': 'pre/key3'}
            ],
            'IsTruncated': False
        }]
        s3_p = S3Path('s3://test-bucket')
        results = s3_p.list(ignore_dir_markers=True)

        self.assertEquals(results, [
            's3://test-bucket/pre/key1',
            's3://test-bucket/pre/key2',
            's3://test-bucket/pre/key3'
        ])


class TestListdir(S3TestCase):
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


@mock.patch.object(S3Path, 'stat', return_value={}, autospec=True)
class TestWalkFiles(S3TestCase):
    def test_walkfiles_no_pattern(self, mock_stat):
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

    def test_walkfiles_w_pattern(self, mock_stat):
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


class TestExists(S3TestCase):
    @mock.patch.object(S3Path, 'stat', autospec=True)
    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_exists_object_true(self, mock_list, mock_stat):
        mock_list.return_value = []
        mock_stat.return_value = {'key': 'val'}
        s3_p = S3Path('s3://test/b/c.txt')
        self.assertTrue(s3_p.exists())
        mock_stat.assert_called_once_with(S3Path('s3://test/b/c.txt'))
        self.assertEquals(mock_list.call_count, 0)

    @mock.patch.object(S3Path, 'stat', autospec=True)
    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_exists_dir_true(self, mock_list, mock_stat):
        mock_list.return_value = [S3Path('s3://test/b/c.txt')]
        mock_stat.side_effect = exceptions.NotFoundError('not found')
        s3_p = S3Path('s3://test/b')
        self.assertTrue(s3_p.exists())
        mock_stat.assert_called_once_with(S3Path('s3://test/b'))
        mock_list.assert_called_once_with(S3Path('s3://test/b/'), limit=1)

    def test_exists_bucket_true(self):
        mock_head_bucket = self.mock_s3.head_bucket
        mock_head_bucket.return_value = {
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'HostId': 'string',
                'RequestId': 'string'
            }
        }
        s3_p = S3Path('s3://bucket')
        self.assertTrue(s3_p.exists())
        mock_head_bucket.assert_called_once_with(Bucket='bucket')

    def test_exists_bucket_false(self):
        mock_head_bucket = self.mock_s3.head_bucket
        mock_head_bucket.side_effect = ClientError(
            {
                'ResponseMetadata': {'HTTPStatusCode': 404},
                'Error': {'Message': 'Not found'}
            },
            'head_bucket')
        s3_p = S3Path('s3://bucket')
        self.assertFalse(s3_p.exists())
        mock_head_bucket.assert_called_once_with(Bucket='bucket')

    @mock.patch.object(S3Path, 'stat', autospec=True)
    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_exists_false_empty_list(self, mock_list, mock_stat):
        mock_list.return_value = []
        mock_stat.side_effect = exceptions.NotFoundError('not found')
        s3_p = S3Path('s3://a/b/c/')
        self.assertFalse(s3_p.exists())
        mock_list.assert_called_once_with(S3Path('s3://a/b/c/'), limit=1)
        mock_stat.assert_called_once_with(S3Path('s3://a/b/c/'))

    @mock.patch.object(S3Path, 'stat', autospec=True)
    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_exists_false_404_error(self, mock_list, mock_stat):
        mock_list.side_effect = exceptions.NotFoundError('not found')
        mock_stat.side_effect = exceptions.NotFoundError('not found')
        s3_p = S3Path('s3://a/b/c/')
        self.assertFalse(s3_p.exists())
        mock_list.assert_called_once_with(S3Path('s3://a/b/c/'), limit=1)
        mock_stat.assert_called_once_with(S3Path('s3://a/b/c/'))

    @mock.patch.object(S3Path, 'stat', autospec=True)
    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_exists_any_error(self, mock_list, mock_stat):
        mock_list.side_effect = exceptions.RemoteError('some error')
        mock_stat.side_effect = exceptions.RemoteError('some error')
        s3_p = S3Path('s3://a/b/c')
        with self.assertRaises(exceptions.RemoteError):
            s3_p.exists()
        mock_stat.assert_called_once_with(S3Path('s3://a/b/c'))
        self.assertEquals(mock_list.call_count, 0)


class TestGetsize(S3TestCase):
    def test_getsize_bucket(self):
        mock_bucket = self.mock_s3.head_bucket
        mock_bucket.return_value = {
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'HostId': 'somestring',
                'RequestId': 'somestring'
            }
        }
        s3_p = S3Path('s3://bucket')
        self.assertEquals(s3_p.getsize(), 0)

    def test_getsize_file(self):
        mock_object = self.mock_s3.head_object
        mock_object.return_value = {
            'AcceptRanges': 'bytes',
            'ContentLength': 15,
            'ContentType': 'text/plain'
        }
        s3_p = S3Path('s3://bucket/obj')
        self.assertEquals(s3_p.getsize(), 15)

    @mock.patch.object(S3Path, 'exists', autospec=True)
    def test_getsize_dir(self, mock_exists):
        mock_exists.return_value = True
        mock_object = self.mock_s3.head_object
        mock_object.side_effect = ClientError(
            {
                'ResponseMetadata': {'HTTPStatusCode': 404},
                'Error': {'Message': 'Not found'}
            },
            'head_object')
        s3_p = S3Path('s3://bucket/dir')
        self.assertEquals(s3_p.getsize(), 0)

    @mock.patch.object(S3Path, 'exists', autospec=True)
    def test_getsize_error(self, mock_exists):
        mock_exists.return_value = False
        mock_object = self.mock_s3.head_object
        mock_object.side_effect = ClientError(
            {
                'ResponseMetadata': {'HTTPStatusCode': 404},
                'Error': {'Message': 'Not found'}
            },
            'head_object')
        s3_p = S3Path('s3://bucket/key')
        with self.assertRaises(exceptions.NotFoundError):
            s3_p.getsize()


class TestBasicIsMethods(S3TestCase):
    def test_isabs_always_true(self):
        self.assertTrue(S3Path('s3://a/b/c').isabs())

    def test_islink_always_false(self):
        self.assertFalse(S3Path('s3://a/b/c').islink())

    def test_ismount_always_true(self):
        self.assertTrue(S3Path('s3://a/b/c').ismount())


class TestIsdir(S3TestCase):
    def test_isdir_true_bucket(self):
        mock_head_bucket = self.mock_s3.head_bucket
        mock_head_bucket.return_value = {
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'HostId': 'string',
                'RequestId': 'string'
            }
        }
        s3_p = S3Path('s3://bucket/')
        self.assertTrue(s3_p.isdir())
        mock_head_bucket.assert_called_once_with(Bucket='bucket')

    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_isdir_true_prefix(self, mock_list):
        mock_list.return_value = [S3Path('s3://bucket/pre/fix/key')]
        s3_p = S3Path('s3://bucket/pre/fix')
        self.assertTrue(s3_p.isdir())
        mock_list.assert_called_once_with(S3Path('s3://bucket/pre/fix/'), limit=1)

    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_isdir_dir_marker(self, mock_list):
        mock_list.return_value = [S3Path('s3://bucket/pre/fix/')]
        s3_p = S3Path('s3://bucket/pre/fix')
        self.assertTrue(s3_p.isdir())
        mock_list.assert_called_once_with(S3Path('s3://bucket/pre/fix/'), limit=1)

    def test_isdir_false_bucket(self):
        mock_head_bucket = self.mock_s3.head_bucket
        mock_head_bucket.side_effect = ClientError(
            {
                'ResponseMetadata': {'HTTPStatusCode': 404},
                'Error': {'Message': 'Not found'}
            },
            'head_bucket')
        s3_p = S3Path('s3://bucket')
        self.assertFalse(s3_p.isdir())
        mock_head_bucket.assert_called_once_with(Bucket='bucket')

    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_isdir_false_file(self, mock_list):
        mock_list.return_value = []
        s3_p = S3Path('s3://bucket/pre/fix.txt')
        self.assertFalse(s3_p.isdir())
        mock_list.assert_called_once_with(S3Path('s3://bucket/pre/fix.txt/'), limit=1)

    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_isdir_false_not_found(self, mock_list):
        mock_list.side_effect = exceptions.NotFoundError('not found')
        s3_p = S3Path('s3://bucket/pre/fix')
        self.assertFalse(s3_p.isdir())
        mock_list.assert_called_once_with(S3Path('s3://bucket/pre/fix/'), limit=1)

    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_isdir_error(self, mock_list):
        mock_list.side_effect = exceptions.RemoteError('error')
        s3_p = S3Path('s3://bucket/pre/fix')
        with self.assertRaises(exceptions.RemoteError):
            s3_p.isdir()


@mock.patch.object(S3Path, 'stat', autospec=True)
class TestIsfile(S3TestCase):
    def test_isfile_true(self, mock_stat):
        mock_stat.return_value = {'key': 'val'}
        s3_p = S3Path('s3://bucket/a_file.txt')
        self.assertTrue(s3_p.isfile())
        mock_stat.assert_called_once_with(S3Path('s3://bucket/a_file.txt'))

    def test_isfile_false(self, mock_stat):
        mock_stat.side_effect = exceptions.NotFoundError('not found')
        s3_p = S3Path('s3://bucket/a')
        self.assertFalse(s3_p.isfile())
        mock_stat.assert_called_once_with(S3Path('s3://bucket/a'))

    def test_isfile_error(self, mock_stat):
        mock_stat.side_effect = exceptions.RemoteError('some error')
        s3_p = S3Path('s3://bucket/a')
        with self.assertRaises(exceptions.RemoteError):
            s3_p.isfile()
        mock_stat.assert_called_once_with(S3Path('s3://bucket/a'))


class TestRemove(S3TestCase):
    def test_remove_bucket(self):
        s3_p = S3Path('s3://bucket')
        with self.assertRaises(ValueError):
            s3_p.remove()

    def test_remove_dir(self):
        mock_delete_object = self.mock_s3.delete_object
        mock_delete_object.side_effect = ClientError(
            {
                'ResponseMetadata': {'HTTPStatusCode': 404},
                'Error': {'Message': 'not found'}
            },
            'delete_object')
        s3_p = S3Path('s3://a/b/c')
        with self.assertRaises(exceptions.NotFoundError):
            s3_p.remove()

    def test_remove_obj(self):
        mock_delete_object = self.mock_s3.delete_object
        s3_p = S3Path('s3://a/b/c.txt')
        s3_p.remove()
        mock_delete_object.assert_called_once_with(Bucket='a', Key='b/c.txt')


@mock.patch.object(S3Path, 'list', autospec=True)
class TestRmtree(S3TestCase):
    def test_rmtree_obj(self, mock_list):
        mock_delete_objects = self.mock_s3.delete_objects
        mock_list.return_value = []
        s3_p = S3Path('s3://a/b/c')
        s3_p.rmtree()
        mock_delete_objects.assert_not_called()

    def test_rmtree_dir(self, mock_list):
        mock_delete_objects = self.mock_s3.delete_objects
        mock_delete_objects.return_value = {
            'Deleted': [
                {'Key': 'b/c'},
                {'Key': 'b/d'},
                {'Key': 'b/e/f'},
                {'Key': 'b/e/g'}
            ]
        }
        objects = [
            S3Path('s3://a/b/c'),
            S3Path('s3://a/b/d'),
            S3Path('s3://a/b/e/f'),
            S3Path('s3://a/b/e/g')
        ]
        mock_list.return_value = objects

        s3_p = S3Path('s3://a/b')
        s3_p.rmtree()
        mock_delete_objects.assert_called_once_with(Bucket='a', Delete={
            'Objects': [
                {'Key': 'b/c'},
                {'Key': 'b/d'},
                {'Key': 'b/e/f'},
                {'Key': 'b/e/g'}
            ]
        })
        self.assertEquals([], objects)

    def test_rmtree_over_1000(self, mock_list):
        mock_delete_objects = self.mock_s3.delete_objects
        mock_delete_objects.return_value = {}
        objects = [S3Path('s3://bucket/obj' + str(i)) for i in range(1234)]
        mock_list.return_value = objects

        s3_p = S3Path('s3://bucket')
        s3_p.rmtree()
        mock_delete_objects.assert_has_calls([
            mock.call(Bucket='bucket', Delete={
                'Objects': [{'Key': 'obj' + str(i)} for i in range(1000)]
            }),
            mock.call(Bucket='bucket', Delete={
                'Objects': [{'Key': 'obj' + str(i + 1000)} for i in range(234)]
            })
        ])
        self.assertEquals([], objects)

    def test_rmtree_error(self, mock_list):
        mock_delete_objects = self.mock_s3.delete_objects
        mock_delete_objects.return_value = {
            'Deleted': [
                {'Key': 's3://a/b/c'},
                {'Key': 's3://a/b/d'}
            ],
            'Errors': [
                {'Key': 'bc', 'Code': 'AccessDenied', 'Message': "access denied"}
            ]
        }
        mock_list.return_value = [
            S3Path('s3://a/b/c'),
            S3Path('s3://a/bc'),
            S3Path('s3://a/b/d')
        ]

        s3_p = S3Path('s3://a')
        with self.assertRaises(exceptions.RemoteError):
            s3_p.rmtree()
        mock_delete_objects.assert_called_once_with(Bucket='a', Delete={
            'Objects': [
                {'Key': 'b/c'},
                {'Key': 'bc'},
                {'Key': 'b/d'}
            ]
        })


class TestStat(S3TestCase):
    def test_stat_obj(self):
        mock_head_object = self.mock_s3.head_object
        mock_head_object.return_value = {
            u'AcceptRanges': 'bytes',
            u'ContentLength': 0,
            u'ContentType': 'text/plain',
            u'ETag': '"d41d8cd98f00b204e9800998ecf8427e"',
            u'LastModified': datetime.datetime(2016, 6, 20, 21, 38, 31),
            u'Metadata': {},
            'ResponseMetadata': {}
        }
        s3_p = S3Path('s3://a/b.txt')
        response = s3_p.stat()
        self.assertEquals(response, {
            u'AcceptRanges': 'bytes',
            u'ContentLength': 0,
            u'ContentType': 'text/plain',
            u'ETag': '"d41d8cd98f00b204e9800998ecf8427e"',
            u'LastModified': datetime.datetime(2016, 6, 20, 21, 38, 31),
            u'Metadata': {}
        })
        mock_head_object.assert_called_once_with(Bucket='a', Key='b.txt')

    def test_stat_bucket(self):
        s3_p = S3Path('s3://bucket')
        with self.assertRaises(ValueError):
            s3_p.stat()

    def test_stat_dir(self):
        mock_head_object = self.mock_s3.head_object
        mock_head_object.side_effect = ClientError(
            {
                'ResponseMetadata': {'HTTPStatusCode': 404},
                'Error': {'Message': 'not found'}
            },
            'head_object')
        s3_p = S3Path('s3://bucket/dir')
        with self.assertRaises(exceptions.NotFoundError):
            s3_p.stat()
        mock_head_object.assert_called_once_with(Bucket='bucket', Key='dir')


@mock.patch('stor.utils.walk_files_and_dirs', autospec=True)
@mock.patch('os.path.getsize', autospec=True)
class TestUpload(S3TestCase):
    def test_upload_to_bucket(self, mock_getsize, mock_files):
        mock_files.return_value = {
            'file1': 10,
            'file2': 20,
            'dir/file3': 30
        }

        s3_p = S3Path('s3://bucket')
        s3_p.upload(['upload'])

        self.mock_s3_transfer.upload_file.assert_has_calls([
            mock.call(bucket='bucket', key='file1', filename='file1'),
            mock.call(bucket='bucket', key='file2', filename='file2'),
            mock.call(bucket='bucket', key='dir/file3', filename='dir/file3')
        ], any_order=True)

    def test_upload_rel_path(self, mock_getsize, mock_files):
        mock_files.return_value = {'../file1': 10, './file2': 20}

        s3_p = S3Path('s3://a/b')
        s3_p.upload(['../', './'])

        self.mock_s3_transfer.upload_file.assert_has_calls([
            mock.call(bucket='a', key='b/file1', filename='../file1'),
            mock.call(bucket='a', key='b/file2', filename='./file2')
        ], any_order=True)

    def test_upload_abs_path(self, mock_getsize, mock_files):
        mock_files.return_value = {'/path/to/file1': 10}

        s3_p = S3Path('s3://a/b')
        s3_p.upload(['/path/to/file1'])

        self.mock_s3_transfer.upload_file.assert_called_once_with(bucket='a',
                                                                  key='b/path/to/file1',
                                                                  filename='/path/to/file1')

    def test_upload_object_invalid(self, mock_getsize, mock_files):
        s3_p = S3Path('s3://a/b')
        with self.assertRaisesRegexp(ValueError, 'empty strings'):
            s3_p.upload([OBSUploadObject('', '')])
        with self.assertRaisesRegexp(ValueError, 'OBSUploadObject'):
            s3_p.upload([OBSUploadObject(1234, 'dest')])

    def test_upload_w_headers(self, mock_getsize, mock_files):
        mock_files.return_value = {'file.txt': 10, 'dir': 0}
        with mock.patch.object(type(Path('dir')), 'isdir', autospec=True) as mock_isdir:
            mock_isdir.side_effect = lambda pth: pth == 'dir'
            s3_p = S3Path('s3://a/b')
            s3_p.upload(['.'], headers={'ContentLanguage': 'en'})
            self.mock_s3_transfer.upload_file.assert_called_once_with(bucket='a',
                                                                      key='b/file.txt',
                                                                      filename='file.txt',
                                                                      extra_args={
                                                                          'ContentLanguage': 'en'
                                                                      })
            self.mock_s3.put_object.assert_called_once_with(Bucket='a',
                                                            Key='b/dir/',
                                                            ContentLanguage='en')

    def test_upload_empty_dir(self, mock_getsize, mock_files):
        with mock.patch.object(type(Path('dir')), 'isdir') as mock_isdir:
            mock_files.return_value = {'dir/'}
            mock_isdir.return_value = True
            s3_p = S3Path('s3://a/b/')
            s3_p.upload(['dir/'])
            self.assertEquals(self.mock_s3_transfer.upload_file.call_count, 0)
            self.mock_s3.put_object.assert_called_once_with(Bucket='a', Key='b/dir/')

    def test_upload_w_condition(self, mock_getsize, mock_files):
        mock_files.return_value = {'file1': 10, 'file2': 20}
        s3_p = S3Path('s3://bucket')
        s3_p.upload('test',
                    condition=lambda results: len(results) == 2)
        self.assertEquals(self.mock_s3_transfer.upload_file.call_count, 2)

        mock_files.return_value = {'file1': 10, 'file2': 20}
        with self.assertRaises(exceptions.ConditionNotMetError):
            s3_p.upload('test',
                        condition=lambda results: len(results) == 3)

    def test_upload_w_use_manifest(self, mock_getsize, mock_files):
        mock_files.side_effect = [
            {
                './file1': 20,
                './file2': 30
            },
            {
                './%s' % utils.DATA_MANIFEST_FILE_NAME: 10
            }
        ]

        with NamedTemporaryDirectory(change_dir=True):
            s3_p = S3Path('s3://bucket/path/')
            s3_p.upload(['.'], use_manifest=True)

        manifest_upload_kwargs = self.mock_s3_transfer.upload_file.call_args_list[0][1]
        self.assertEquals(len(manifest_upload_kwargs), 3)
        self.assertEquals(manifest_upload_kwargs['bucket'], 'bucket')
        self.assertEquals(manifest_upload_kwargs['filename'],
                          './%s' % utils.DATA_MANIFEST_FILE_NAME)
        self.assertEquals(manifest_upload_kwargs['key'],
                          'path/%s' % utils.DATA_MANIFEST_FILE_NAME)

    @mock.patch.object(S3Path, '_upload_object', autospec=True)
    def test_upload_w_manifest_validation_err(self, mock_upload, mock_getsize, mock_files):
        mock_files.return_value = {
            'file1': 20,
            'file2': 10
        }
        mock_upload.return_value = {
            'source': 'file1',
            'dest': S3Path('s3://bucket/path/file1'),
            'success': True
        }

        with NamedTemporaryDirectory(change_dir=True):
            s3_p = S3Path('s3://bucket/path')
            with self.assertRaises(exceptions.ConditionNotMetError):
                s3_p.upload(['.'], use_manifest=True)

    def test_upload_w_condition_and_use_manifest(self, mock_getsize, mock_files):
        mock_files.side_effect = [
            {
                './%s' % utils.DATA_MANIFEST_FILE_NAME: 20,
                './file1': 30,
                './file2': 40
            },
            {
                './%s' % utils.DATA_MANIFEST_FILE_NAME: 20
            }
        ]

        with NamedTemporaryDirectory(change_dir=True):
            s3_p = S3Path('s3://bucket/path')
            s3_p.upload(['.'],
                        use_manifest=True,
                        condition=lambda results: len(results) == 2)

        self.assertEquals(self.mock_s3_transfer.upload_file.call_count, 3)

        manifest_upload_kwargs = self.mock_s3_transfer.upload_file.call_args_list[0][1]
        self.assertEquals(len(manifest_upload_kwargs), 3)
        self.assertEquals(manifest_upload_kwargs['bucket'], 'bucket')
        self.assertEquals(manifest_upload_kwargs['filename'],
                          './%s' % utils.DATA_MANIFEST_FILE_NAME)
        self.assertEquals(manifest_upload_kwargs['key'],
                          'path/%s' % utils.DATA_MANIFEST_FILE_NAME)

    def test_upload_w_use_manifest_multiple_uploads(self, mock_getsize, mock_files):
        with self.assertRaisesRegexp(ValueError, 'can only upload one directory'):
            S3Path('s3://bucket/path').upload(['.', '.'],
                                              use_manifest=True)

    def test_upload_w_use_manifest_single_file(self, mock_getsize, mock_files):
        with self.assertRaisesRegexp(ValueError, 'can only upload one directory'):
            S3Path('s3://bucket/path').upload(['file'],
                                              use_manifest=True)

    @mock.patch('stor.s3.ThreadPool', autospec=True)
    def test_upload_object_threads(self, mock_pool, mock_getsize, mock_files):
        mock_files.return_value = {
            'file%s' % i: 20
            for i in range(20)
        }
        mock_getsize.return_value = 20
        mock_pool.return_value.imap_unordered.return_value.next.side_effect = StopIteration

        s3_p = S3Path('s3://bucket')
        with settings.use({'s3:upload': {'object_threads': 20}}):
            s3_p.upload(['test'])
        mock_pool.assert_called_once_with(20)

    def test_upload_remote_error(self, mock_getsize, mock_files):
        mock_files.return_value = {
            'file1': 20,
            'file2': 10
        }
        self.mock_s3_transfer.upload_file.side_effect = [
            None,
            S3UploadFailedError('failed')
        ]

        with self.assertRaises(exceptions.FailedUploadError):
            S3Path('s3://bucket/path').upload(['test'])

    def test_upload_other_error(self, mock_getsize, mock_files):
        mock_files.return_value = {
            'file1': 20,
            'file2': 10
        }
        self.mock_s3_transfer.upload_file.side_effect = [None, ValueError]

        with self.assertRaises(ValueError):
            S3Path('s3://bucket/path').upload(['test'])

    def test_upload_multipart_settings(self, mock_getsize, mock_files):
        mock_files.return_value = {
            'file1': 20,
            'file2': 10
        }
        s3_p = S3Path('s3://bucket')
        with settings.use({'s3:upload': {'segment_size': '5M', 'segment_threads': 20}}):
            s3_p.upload(['test'])
        self.mock_get_s3_transfer_config.assert_called_with(multipart_threshold=5242880,
                                                            max_concurrency=20,
                                                            multipart_chunksize=5242880)

    @freezegun.freeze_time('2016-4-5')
    def test_upload_progress_logging(self, mock_getsize, mock_files):
        mock_files.return_value = {
            'file%s' % i: 20
            for i in range(20)
        }
        mock_getsize.return_value = 20

        s3_p = S3Path('s3://bucket')
        with LogCapture('stor.s3.progress') as progress_log:
            s3_p.upload(['upload'])
            progress_log.check(
                ('stor.s3.progress', 'INFO', 'starting upload of 20 objects'),  # nopep8
                ('stor.s3.progress', 'INFO', '10/20\t0:00:00\t0.00 MB\t0.00 MB/s'),  # nopep8
                ('stor.s3.progress', 'INFO', '20/20\t0:00:00\t0.00 MB\t0.00 MB/s'),  # nopep8
                ('stor.s3.progress', 'INFO', 'upload complete - 20/20\t0:00:00\t0.00 MB\t0.00 MB/s'),  # nopep8
            )


@mock.patch('stor.utils.make_dest_dir', autospec=True)
@mock.patch('os.path.getsize', autospec=True)
class TestDownload(S3TestCase):
    @mock.patch.object(S3Path, 'isfile', return_value=True, autospec=True)
    def test_download_file_to_file(self, mock_isfile, mock_getsize, mock_make_dest):
        s3_p = S3Path('s3://a/b/c.txt')
        s3_p.download_object('test/d.txt')
        self.mock_s3_transfer.download_file.assert_called_once_with(bucket='a',
                                                                    key='b/c.txt',
                                                                    filename='test/d.txt')
        mock_make_dest.assert_called_once_with('test')

    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_download_dir(self, mock_list, mock_getsize, mock_make_dest):
        mock_list.return_value = [
            S3Path('s3://bucket/file1'),
            S3Path('s3://bucket/file2'),
            S3Path('s3://bucket/dir/file3')
        ]
        s3_p = S3Path('s3://bucket')
        s3_p.download('test')
        self.mock_s3_transfer.download_file.assert_has_calls([
            mock.call(bucket='bucket', key='file1', filename='test/file1'),
            mock.call(bucket='bucket', key='file2', filename='test/file2'),
            mock.call(bucket='bucket', key='dir/file3', filename='test/dir/file3')
        ], any_order=True)
        mock_make_dest.assert_has_calls([
            mock.call('test'),
            mock.call('test'),
            mock.call('test/dir')
        ], any_order=True)

    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_download_empty_dir(self, mock_list, mock_getsize, mock_make_dest):
        mock_list.return_value = [
            S3Path('s3://bucket/file1'),
            S3Path('s3://bucket/file2'),
            S3Path('s3://bucket/empty/'),
            S3Path('s3://bucket/dir/file3')
        ]
        s3_p = S3Path('s3://bucket')
        s3_p.download('test')
        self.mock_s3_transfer.download_file.assert_has_calls([
            mock.call(bucket='bucket', key='file1', filename='test/file1'),
            mock.call(bucket='bucket', key='file2', filename='test/file2'),
            mock.call(bucket='bucket', key='dir/file3', filename='test/dir/file3')
        ], any_order=True)
        mock_make_dest.assert_has_calls([
            mock.call('test'),
            mock.call('test'),
            mock.call('test/dir'),
            mock.call('test/empty/')
        ], any_order=True)

    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_download_w_condition(self, mock_list, mock_getsize, mock_make_dest):
        mock_list.return_value = [
            S3Path('s3://bucket/file1'),
            S3Path('s3://bucket/file2')
        ]
        s3_p = S3Path('s3://bucket')
        s3_p.download('test',
                      condition=lambda results: len(results) == 2)
        self.assertEquals(self.mock_s3_transfer.download_file.call_count, 2)

        mock_list.return_value = [
            S3Path('s3://bucket/file1'),
            S3Path('s3://bucket/file2')
        ]
        with self.assertRaises(exceptions.ConditionNotMetError):
            s3_p.download('test',
                          condition=lambda results: len(results) == 3)

    @mock.patch.object(S3Path, 'list', autospec=True)
    @mock.patch('botocore.response.StreamingBody', autospec=True)
    def test_download_w_use_manifest(self, mock_stream, mock_list, mock_getsize,
                                     mock_make_dest_dir):
        mock_stream.read.return_value = 'my/obj1\nmy/obj2\nmy/obj3\n'
        self.mock_s3.get_object.return_value = {'Body': mock_stream}
        mock_list.return_value = [
            S3Path('s3://bucket/my/obj1'),
            S3Path('s3://bucket/my/obj2'),
            S3Path('s3://bucket/my/obj3')
        ]

        s3_p = S3Path('s3://bucket')
        s3_p.download('test', use_manifest=True)
        self.assertEquals(self.mock_s3_transfer.download_file.call_count, 3)

    @mock.patch.object(S3Path, 'list', autospec=True)
    @mock.patch('botocore.response.StreamingBody', autospec=True)
    def test_download_w_use_manifest_validation_err(self, mock_stream, mock_list, mock_getsize,
                                                    mock_make_dest_dir):
        mock_stream.read.return_value = 'my/obj1\nmy/obj2\nmy/obj3\n'
        self.mock_s3.get_object.return_value = {'Body': mock_stream}
        mock_list.return_value = [
            S3Path('s3://bucket/my/obj1'),
            S3Path('s3://bucket/my/obj2')
        ]

        s3_p = S3Path('s3://bucket')
        with self.assertRaises(exceptions.ConditionNotMetError):
            s3_p.download('test', use_manifest=True)

    @mock.patch.object(S3Path, 'list', autospec=True)
    @mock.patch('botocore.response.StreamingBody', autospec=True)
    def test_download_w_condition_and_use_manifest(self, mock_stream, mock_list, mock_getsize,
                                                   mock_make_dest_dir):
        mock_stream.read.return_value = 'my/obj1\nmy/obj2\nmy/obj3\n'
        self.mock_s3.get_object.return_value = {'Body': mock_stream}
        mock_list.return_value = [
            S3Path('s3://bucket/my/obj1'),
            S3Path('s3://bucket/my/obj2'),
            S3Path('s3://bucket/my/obj3')
        ]

        s3_p = S3Path('s3://bucket')
        s3_p.download('test',
                      use_manifest=True,
                      condition=lambda results: len(results) == 3)
        self.assertEquals(self.mock_s3_transfer.download_file.call_count, 3)

    @mock.patch.object(S3Path, 'list', autospec=True)
    @mock.patch('stor.s3.ThreadPool', autospec=True)
    def test_download_object_threads(self, mock_pool, mock_list, mock_getsize,
                                     mock_make_dest_dir):
        mock_list.return_value = [
            S3Path('s3://bucket/file%s' % i)
            for i in range(20)
        ]
        mock_pool.return_value.imap_unordered.return_value.next.side_effect = StopIteration
        s3_p = S3Path('s3://bucket')
        with settings.use({'s3:download': {'object_threads': 20}}):
            s3_p.download(['test'])
        mock_pool.assert_called_once_with(20)

    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_download_remote_error(self, mock_list, mock_getsize, mock_make_dest_dir):
        mock_list.return_value = [
            S3Path('s3://bucket/my/obj1'),
            S3Path('s3://bucket/my/obj2'),
            S3Path('s3://bucket/my/obj3')
        ]
        self.mock_s3_transfer.download_file.side_effect = RetriesExceededError('failed')

        with self.assertRaises(exceptions.FailedDownloadError):
            S3Path('s3://bucket/path').download('test')

    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_download_other_error(self, mock_list, mock_getsize, mock_make_dest_dir):
        mock_list.return_value = [
            S3Path('s3://bucket/my/obj1'),
            S3Path('s3://bucket/my/obj2'),
            S3Path('s3://bucket/my/obj3')
        ]
        self.mock_s3_transfer.download_file.side_effect = [None, ValueError]

        with self.assertRaises(ValueError):
            S3Path('s3://bucket/path').download('test')

    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_download_multipart_settings(self, mock_list, mock_getsize, mock_make_dest_dir):
        mock_list.return_value = [
            S3Path('s3://bucket/my/obj1'),
            S3Path('s3://bucket/my/obj2'),
            S3Path('s3://bucket/my/obj3')
        ]
        s3_p = S3Path('s3://bucket')
        with settings.use({'s3:download': {'segment_size': '5M', 'segment_threads': 20}}):
            s3_p.download('test')
        self.mock_get_s3_transfer_config.assert_called_with(multipart_threshold=5242880,
                                                            max_concurrency=20,
                                                            multipart_chunksize=5242880)

    @freezegun.freeze_time('2016-4-5')
    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_download_progress_logging(self, mock_list, mock_getsize, mock_make_dest_dir):
        mock_list.return_value = [
            S3Path('s3://bucket/file%s' % i)
            for i in range(19)
        ] + [S3Path('s3://bucket/dir')]
        mock_getsize.return_value = 100

        s3_p = S3Path('s3://bucket')
        with LogCapture('stor.s3.progress') as progress_log:
            s3_p.download('output_dir')
            progress_log.check(
                ('stor.s3.progress', 'INFO', 'starting download of 20 objects'),  # nopep8
                ('stor.s3.progress', 'INFO', '10/20\t0:00:00\t0.00 MB\t0.00 MB/s'),  # nopep8
                ('stor.s3.progress', 'INFO', '20/20\t0:00:00\t0.00 MB\t0.00 MB/s'),  # nopep8
                ('stor.s3.progress', 'INFO', 'download complete - 20/20\t0:00:00\t0.00 MB\t0.00 MB/s'),  # nopep8
            )


class TestCopy(S3TestCase):
    @mock.patch.object(S3Path, 'download_object', autospec=True)
    def test_copy_posix_file_destination(self, mockdownload_object):
        p = S3Path('s3://bucket/key/file_source.txt')
        p.copy('file_dest.txt')
        mockdownload_object.assert_called_once_with(p, Path(u'file_dest.txt'))

    @mock.patch.object(S3Path, 'download_object', autospec=True)
    def test_copy_posix_dir_destination(self, mockdownload_object):
        p = S3Path('s3://bucket/key/file_source.txt')
        with NamedTemporaryDirectory() as tmp_d:
            p.copy(tmp_d)
            mockdownload_object.assert_called_once_with(p, Path(tmp_d) / 'file_source.txt')

    def test_copy_swift_destination(self):
        p = S3Path('s3://bucket/key/file_source')
        with self.assertRaisesRegexp(ValueError, 'OBS path'):
            p.copy('swift://tenant/container/file_dest')

    def test_copy_s3_destination(self):
        p = S3Path('s3://bucket/key/file_source')
        with self.assertRaisesRegexp(ValueError, 'OBS path'):
            p.copy('s3://bucket/key/file_dest')


class TestCopytree(S3TestCase):
    @mock.patch.object(S3Path, 'download', autospec=True)
    def test_copytree_posix_destination(self, mock_download):
        p = S3Path('s3://bucket/key')
        p.copytree('path')
        mock_download.assert_called_once_with(
            p,
            Path(u'path'),
            condition=None,
            use_manifest=False)

    def test_copytree_swift_destination(self):
        p = S3Path('s3://bucket/key')
        with self.assertRaises(ValueError):
            p.copytree('s3://s3/path')

    @mock.patch('os.path', ntpath)
    def test_copytree_windows_destination(self):
        p = S3Path('s3://bucket/key')
        with self.assertRaisesRegexp(ValueError, 'not supported'):
            p.copytree(r'windows\path')


class TestS3File(S3TestCase):
    def test_invalid_buffer_mode(self):
        s3_f = S3Path('s3://bucket/key/obj').open()
        s3_f.mode = 'invalid'
        with self.assertRaisesRegexp(ValueError, 'buffer'):
            s3_f._buffer

    @mock.patch('botocore.response.StreamingBody', autospec=True)
    def test_invalid_flush_mode(self, mock_stream):
        mock_stream.read.return_value = 'data'
        self.mock_s3.get_object.return_value = {'Body': mock_stream}
        s3_p = S3Path('s3://bucket/key/obj')
        obj = s3_p.open()
        with self.assertRaisesRegexp(TypeError, 'flush'):
            obj.flush()

    def test_name(self):
        s3_p = S3Path('s3://bucket/key/obj')
        obj = s3_p.open()
        self.assertEquals(obj.name, s3_p)

    @mock.patch('botocore.response.StreamingBody', autospec=True)
    def test_context_manager_on_closed_file(self, mock_stream):
        mock_stream.read.return_value = 'data'
        self.mock_s3.get_object.return_value = {'Body': mock_stream}
        s3_p = S3Path('s3://bucket/key/obj')
        obj = s3_p.open()
        obj.close()

        with self.assertRaisesRegexp(ValueError, 'closed file'):
            with obj:
                pass  # pragma: no cover

    def test_invalid_mode(self):
        s3_p = S3Path('s3://bucket/key/obj')
        with self.assertRaisesRegexp(ValueError, 'invalid mode'):
            s3_p.open(mode='invalid')

    def test_invalid_io_op(self):
        # now invalid delegates are considered invalid on instantiation
        with self.assertRaisesRegexp(AttributeError, 'no attribute'):
            class MyFile(object):
                closed = False
                _buffer = cStringIO.StringIO()
                invalid = obs._delegate_to_buffer('invalid')

    @mock.patch('botocore.response.StreamingBody', autospec=True)
    def test_read_on_closed_file(self, mock_stream):
        mock_stream.read.return_value = 'data'
        self.mock_s3.get_object.return_value = {'Body': mock_stream}
        s3_p = S3Path('s3://bucket/key/obj')
        obj = s3_p.open()
        obj.close()

        with self.assertRaisesRegexp(ValueError, 'closed file'):
            obj.read()

    def test_read_invalid_mode(self):
        s3_p = S3Path('s3://bucket/key/obj')
        with self.assertRaisesRegexp(TypeError, 'mode.*read'):
            s3_p.open(mode='wb').read()

    @mock.patch('botocore.response.StreamingBody', autospec=True)
    def test_read_success(self, mock_stream):
        mock_stream.read.return_value = 'data'
        self.mock_s3.get_object.return_value = {'Body': mock_stream}

        s3_p = S3Path('s3://bucket/key/obj')
        self.assertEquals(s3_p.open().read(), 'data')

    @mock.patch('botocore.response.StreamingBody', autospec=True)
    def test_iterating_over_files(self, mock_stream):
        data = '''\
line1
line2
line3
line4
'''
        mock_stream.read.return_value = data
        self.mock_s3.get_object.return_value = {'Body': mock_stream}

        s3_p = S3Path('s3://bucket/key/obj')
        self.assertEquals(s3_p.open().read(), data)
        self.assertEquals(s3_p.open().readlines(),
                          [l + '\n' for l in data.split('\n')][:-1])
        for i, line in enumerate(s3_p.open(), 1):
            self.assertEqual(line, 'line%d\n' % i)

        self.assertEqual(next(s3_p.open()), 'line1\n')
        self.assertEqual(s3_p.open().next(), 'line1\n')
        self.assertEqual(iter(s3_p.open()).next(), 'line1\n')

    def test_write_invalid_args(self):
        s3_p = S3Path('s3://bucket/key/obj')
        obj = s3_p.open(mode='r')
        with self.assertRaisesRegexp(TypeError, 'mode.*write'):
            obj.write('hello')

    @mock.patch('time.sleep', autospec=True)
    def test_write_multiple_w_context_manager(self, mock_sleep):
        mock_upload = self.mock_s3_transfer.upload_file
        s3_p = S3Path('s3://bucket/key/obj')
        with s3_p.open(mode='wb') as obj:
            obj.write('hello')
            obj.write(' world')
        upload_call, = mock_upload.call_args_list
        self.assertTrue(upload_call[1]['bucket'] == s3_p.bucket)
        self.assertTrue(upload_call[1]['key'] == s3_p.resource)

    @mock.patch('time.sleep', autospec=True)
    def test_write_multiple_flush_multiple_upload(self, mock_sleep):
        mock_upload = self.mock_s3_transfer.upload_file
        s3_p = S3Path('s3://bucket/key/obj')
        with NamedTemporaryFile(delete=False) as ntf1,\
                NamedTemporaryFile(delete=False) as ntf2,\
                NamedTemporaryFile(delete=False) as ntf3:
            with mock.patch('tempfile.NamedTemporaryFile', autospec=True) as ntf:
                ntf.side_effect = [ntf1, ntf2, ntf3]
                with s3_p.open(mode='wb') as obj:
                    obj.write('hello')
                    obj.flush()
                    obj.write(' world')
                    obj.flush()
                u1, u2, u3 = mock_upload.call_args_list
                self.assertTrue(u1[1]['bucket'] == s3_p.bucket)
                self.assertTrue(u2[1]['bucket'] == s3_p.bucket)
                self.assertTrue(u3[1]['bucket'] == s3_p.bucket)
                self.assertTrue(u1[1]['filename'] == ntf1.name)
                self.assertTrue(u2[1]['filename'] == ntf2.name)
                self.assertTrue(u3[1]['filename'] == ntf3.name)
                self.assertTrue(u1[1]['key'] == s3_p.resource)
                self.assertTrue(u2[1]['key'] == s3_p.resource)
                self.assertTrue(u3[1]['key'] == s3_p.resource)
                self.assertEqual(open(ntf1.name).read(), 'hello')
                self.assertEqual(open(ntf2.name).read(), 'hello world')
                # third call happens because we don't care about checking for
                # additional file change
                self.assertEqual(open(ntf3.name).read(), 'hello world')

    @mock.patch('time.sleep', autospec=True)
    def test_close_no_writes(self, mock_sleep):
        mock_upload = self.mock_s3_transfer.upload_file
        s3_p = S3Path('s3://bucket/key/obj')
        obj = s3_p.open(mode='wb')
        obj.close()

        self.assertFalse(mock_upload.called)

    def test_works_with_gzip(self):
        gzip_path = stor.join(stor.dirname(__file__),
                              'file_data', 's_3_2126.bcl.gz')
        text = stor.open(gzip_path).read()
        with mock.patch.object(S3Path, 'read_object', autospec=True) as read_mock:
            read_mock.return_value = text
            s3_file = stor.open('s3://A/C/s_3_2126.bcl.gz')

            with gzip.GzipFile(fileobj=s3_file) as s3_file_fp:
                with gzip.open(gzip_path) as gzip_fp:
                    assert_same_data(s3_file_fp, gzip_fp)
            s3_file = stor.open('s3://A/C/s_3_2126.bcl.gz')
            with gzip.GzipFile(fileobj=s3_file) as s3_file_fp:
                with gzip.open(gzip_path) as gzip_fp:
                    # after seeking should still be same
                    s3_file_fp.seek(3)
                    gzip_fp.seek(3)
                    assert_same_data(s3_file_fp, gzip_fp)
