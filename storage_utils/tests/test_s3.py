import mock
import unittest

from storage_utils import Path
from storage_utils.s3 import S3Path
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


class TestNew(unittest.TestCase):
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


class TestGetS3Client(S3TestCase):
    def test_get_s3_client(self):
        self.disable_get_s3_client_mock()
        s3_p = S3Path('s3://test-bucket')
        s3_p._get_s3_client()
        self.mock_s3_client.assert_called_once_with('s3')


class TestList(S3TestCase):
    def test_list_over_1000_no_limit(self):
        mock_list = self.mock_s3.list_objects_v2
        mock_list.side_effect = [{
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
        mock_list.assert_has_calls([
            mock.call(Bucket='test-bucket', Prefix=''),
            mock.call(Bucket='test-bucket', Prefix='', ContinuationToken='token1'),
            mock.call(Bucket='test-bucket', Prefix='', ContinuationToken='token2')
        ])

    def test_list_over_1000_limit(self):
        mock_list = self.mock_s3.list_objects_v2
        mock_list.side_effect = [{
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
        mock_list.assert_has_calls([
            mock.call(Bucket='test-bucket', MaxKeys=1234, Prefix=''),
            mock.call(Bucket='test-bucket', ContinuationToken='token1', MaxKeys=234, Prefix='')
        ])

    def test_list_bucket(self):
        mock_list = self.mock_s3.list_objects_v2
        mock_list.return_value = {
            'Contents': [{'Key': 'key1'}, {'Key': 'key2'}, {'Key': 'prefix/key3'}],
            'IsTruncated': False
        }
        s3_p = S3Path('s3://test-bucket')
        results = s3_p.list()

        self.assertEquals(results, [
            's3://test-bucket/key1',
            's3://test-bucket/key2',
            's3://test-bucket/prefix/key3'
        ])
        mock_list.assert_called_once_with(Bucket='test-bucket', Prefix='')

    def test_list_prefix(self):
        mock_list = self.mock_s3.list_objects_v2
        mock_list.return_value = {
            'Contents': [{'Key': 'prefix/key1'}, {'Key': 'prefix/key2'}, {'Key': 'prefix/key3'}],
            'IsTruncated': False
        }
        s3_p = S3Path('s3://test-bucket/prefix')
        results = s3_p.list()

        self.assertEquals(results, [
            's3://test-bucket/prefix/key1',
            's3://test-bucket/prefix/key2',
            's3://test-bucket/prefix/key3'
        ])
        mock_list.assert_called_once_with(Bucket='test-bucket', Prefix='prefix')

    def test_list_starts_with_no_prefix(self):
        mock_list = self.mock_s3.list_objects_v2
        mock_list.return_value = {
            'Contents': [{'Key': 'prefix/key1'}, {'Key': 'prefix/key2'}, {'Key': 'prefix/key3'}],
            'IsTruncated': False
        }
        s3_p = S3Path('s3://test-bucket')
        results = s3_p.list(starts_with='prefix')

        self.assertEquals(results, [
            's3://test-bucket/prefix/key1',
            's3://test-bucket/prefix/key2',
            's3://test-bucket/prefix/key3'
        ])
        mock_list.assert_called_once_with(Bucket='test-bucket', Prefix='prefix')

    def test_list_starts_with_prefix(self):
        mock_list = self.mock_s3.list_objects_v2
        mock_list.return_value = {
            'Contents': [
                {'Key': 'prefix1/prefix/key1'},
                {'Key': 'prefix1/prefix/key2'},
                {'Key': 'prefix1/prefix/key3'}
            ],
            'IsTruncated': False
        }
        s3_p = S3Path('s3://test-bucket/prefix1')
        results = s3_p.list(starts_with='prefix')

        self.assertEquals(results, [
            's3://test-bucket/prefix1/prefix/key1',
            's3://test-bucket/prefix1/prefix/key2',
            's3://test-bucket/prefix1/prefix/key3'
        ])
        mock_list.assert_called_once_with(Bucket='test-bucket', Prefix='prefix1/prefix')
