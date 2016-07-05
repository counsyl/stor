import datetime
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


@mock.patch.object(S3Path, 'list', autospec=True)
class TestExists(S3TestCase):
    def test_exists_true(self, mock_list):
        mock_list.return_value = [S3Path('s3://test/b/val')]
        s3_p = S3Path('s3://test/b/')
        self.assertTrue(s3_p.exists())

    def test_exists_bucket_false(self, mock_list):
        mock_list.side_effect = exceptions.NotFoundError('not found')
        s3_p = S3Path('s3://bucket')
        self.assertFalse(s3_p.exists())

    def test_exists_dir_or_obj_false(self, mock_list):
        mock_list.return_value = []
        s3_p = S3Path('s3://a/b/c/')
        self.assertFalse(s3_p.exists())

    def test_exists_any_error(self, mock_list):
        mock_list.side_effect = exceptions.RemoteError('some error')
        s3_p = S3Path('s3://a/b/c')
        with self.assertRaises(exceptions.RemoteError):
            s3_p.exists()


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


@mock.patch.object(S3Path, 'list', autospec=True)
class TestIsdir(S3TestCase):
    def test_isdir_true_bucket(self, mock_list):
        mock_list.return_value = [S3Path('s3://bucket/key')]
        s3_p = S3Path('s3://bucket/')
        self.assertTrue(s3_p.isdir())

    def test_isdir_true_prefix(self, mock_list):
        mock_list.return_value = [S3Path('s3://bucket/pre/fix/key')]
        s3_p = S3Path('s3://bucket/pre/fix')
        self.assertTrue(s3_p.isdir())

    def test_isdir_false_bucket(self, mock_list):
        mock_list.side_effect = exceptions.NotFoundError('not found')
        s3_p = S3Path('s3://bucket')
        self.assertFalse(s3_p.isdir())

    def test_isdir_false_file(self, mock_list):
        mock_list.return_value = [S3Path('s3://bucket/pre/fix.txt')]
        s3_p = S3Path('s3://bucket/pre/fix.txt')
        self.assertFalse(s3_p.isdir())


@mock.patch.object(S3Path, 'list', autospec=True)
class TestIsfile(S3TestCase):
    def test_isfile_true(self, mock_list):
        mock_list.return_value = [S3Path('s3://bucket/a_file.txt')]
        s3_p = S3Path('s3://bucket/a_file.txt')
        self.assertTrue(s3_p.isfile())

    def test_isfile_false_directory(self, mock_list):
        mock_list.return_value = [
            S3Path('s3://bucket/a/b.txt'),
            S3Path('s3://bucket/a/c.txt')
        ]
        s3_p = S3Path('s3://bucket/a')
        self.assertFalse(s3_p.isfile())

    def test_isfile_false_emptylist(self, mock_list):
        mock_list.return_value = []
        s3_p = S3Path('s3://bucket/a')
        self.assertFalse(s3_p.isfile())

    def test_isfile_false_error(self, mock_list):
        mock_list.side_effect = exceptions.NotFoundError('not found')
        s3_p = S3Path('s3://bucket/a')
        self.assertFalse(s3_p.isfile())

    def test_isfile_any_error(self, mock_list):
        mock_list.side_effect = exceptions.RemoteError('some error')
        s3_p = S3Path('s3://bucket/a')
        with self.assertRaises(exceptions.RemoteError):
            s3_p.isfile()


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


@mock.patch('storage_utils.utils.walk_files_and_dirs', autospec=True)
class TestUpload(S3TestCase):
    def test_upload_to_bucket(self, mock_files):
        mock_files.return_value = {
            'file1': 10,
            'file2': 20,
            'dir/file3': 30
        }

        s3_p = S3Path('s3://bucket')
        s3_p.upload(['upload'])

        self.mock_s3.upload_file.assert_has_calls([
            mock.call(Bucket='bucket', Key='file1', Filename='file1'),
            mock.call(Bucket='bucket', Key='file2', Filename='file2'),
            mock.call(Bucket='bucket', Key='dir/file3', Filename='dir/file3')
        ], any_order=True)

    def test_upload_rel_path(self, mock_files):
        mock_files.return_value = {'../file1': 10, './file2': 20}

        s3_p = S3Path('s3://a/b')
        s3_p.upload(['../', './'])

        self.mock_s3.upload_file.assert_has_calls([
            mock.call(Bucket='a', Key='b/file1', Filename='../file1'),
            mock.call(Bucket='a', Key='b/file2', Filename='./file2')
        ], any_order=True)

    def test_upload_abs_path(self, mock_files):
        mock_files.return_value = {'/path/to/file1': 10}

        s3_p = S3Path('s3://a/b')
        s3_p.upload(['/path/to/file1'])

        self.mock_s3.upload_file.assert_called_once_with(Bucket='a',
                                                         Key='b/path/to/file1',
                                                         Filename='/path/to/file1')


@mock.patch('storage_utils.utils.make_dest_dir', autospec=True)
class TestDownload(S3TestCase):
    @mock.patch.object(S3Path, 'isfile', return_value=True, autospec=True)
    def test_download_file_to_file(self, mock_isfile, mock_make_dest):
        s3_p = S3Path('s3://a/b/c.txt')
        s3_p.download_object('test/d.txt')
        self.mock_s3.download_file.assert_called_once_with(Bucket='a',
                                                           Key='b/c.txt',
                                                           Filename='test/d.txt')
        mock_make_dest.assert_called_once_with('test')

    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_download_dir(self, mock_list, mock_make_dest):
        mock_list.return_value = [
            S3Path('s3://bucket/file1'),
            S3Path('s3://bucket/file2'),
            S3Path('s3://bucket/dir/file3')
        ]
        s3_p = S3Path('s3://bucket')
        s3_p.download('test')
        self.mock_s3.download_file.assert_has_calls([
            mock.call(Bucket='bucket', Key='file1', Filename='test/file1'),
            mock.call(Bucket='bucket', Key='file2', Filename='test/file2'),
            mock.call(Bucket='bucket', Key='dir/file3', Filename='test/dir/file3')
        ])
        mock_make_dest.assert_has_calls([
            mock.call('test'),
            mock.call('test'),
            mock.call('test/dir')
        ])
