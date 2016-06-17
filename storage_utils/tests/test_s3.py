import unittest

from storage_utils import Path
from storage_utils.s3 import S3Path


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
