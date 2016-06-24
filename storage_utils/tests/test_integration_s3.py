import logging
import os
import unittest

from storage_utils import exceptions
from storage_utils import Path


class S3IntegrationTest(unittest.TestCase):
    """
    Integration tests for S3. Note that for now, while upload/download/remove
    methods are not implemented, tests will use the existing stor-test-bucket
    bucket on S3.
    """
    def setUp(self):
        super(S3IntegrationTest, self).setUp()

        if not os.environ.get('AWS_TEST_ACCESS_KEY_ID'):
            raise unittest.SkipTest(
                'AWS_TEST_ACCESS_KEY_ID env var not set. Skipping integration test')

        # Disable loggers so nose output is clean
        logging.getLogger('botocore').setLevel(logging.CRITICAL)

        self.test_bucket = Path('s3://stor-test-bucket')

    def tearDown(self):
        super(S3IntegrationTest, self).tearDown()

    def test_list_methods(self):
        over_1000_files = self.test_bucket / 'lots_of_files'
        self.assertEquals(1234, len(over_1000_files.list()))
        self.assertEquals(10, len(over_1000_files.list(limit=10)))
        self.assertEquals(1200, len(over_1000_files.list(limit=1200)))

        fake_bucket = Path('s3://stor-test-bucket2')
        with self.assertRaises(exceptions.NotFoundError):
            fake_bucket.list()
        fake_folder = self.test_bucket / 'not_a_dir'
        self.assertEquals([], fake_folder.list())

        small_file_set = self.test_bucket / 'small_test'
        small_file_set_list = small_file_set.list()
        starts_with_list = self.test_bucket.list(starts_with='small_test')
        self.assertEquals(set(small_file_set_list), set(starts_with_list))
        self.assertEquals(set(small_file_set_list), set([
            small_file_set / 'aaabbbab',
            small_file_set / 'aba/babba.txt',
            small_file_set / 'aba/bc',
            small_file_set / 'abaac.txt',
            small_file_set / 'nested_dir/abababa',
            small_file_set / 'nested_dir/file01.txt',
            small_file_set / 'nested_dir/file02.txt',
            small_file_set / 'nested_dir/file03.txt'
        ]))

        bucket_dirlist = self.test_bucket.listdir()
        self.assertEquals(set(bucket_dirlist), set([
            self.test_bucket / 'counsyl-storage-utils/',
            self.test_bucket / 'lots_of_files/',
            self.test_bucket / 'small_test/',
            self.test_bucket / 'a.txt',
            self.test_bucket / 'b.txt',
            self.test_bucket / 'counsyl-storage-utils'
        ]))

        test_dir = self.test_bucket / 'counsyl-storage-utils'
        self.assertTrue(test_dir.listdir() == (test_dir + '/').listdir())

    def test_walkfiles(self):
        test_dir = self.test_bucket / 'small_test'
        all_files = list(test_dir.walkfiles())
        self.assertEquals(set(all_files), set([
            test_dir / 'aaabbbab',
            test_dir / 'aba/babba.txt',
            test_dir / 'aba/bc',
            test_dir / 'abaac.txt',
            test_dir / 'nested_dir/abababa',
            test_dir / 'nested_dir/file01.txt',
            test_dir / 'nested_dir/file02.txt',
            test_dir / 'nested_dir/file03.txt'
        ]))
        prefix_files = list(test_dir.walkfiles('*.txt'))
        self.assertEquals(set(prefix_files), set([
            test_dir / 'aba/babba.txt',
            test_dir / 'abaac.txt',
            test_dir / 'nested_dir/file01.txt',
            test_dir / 'nested_dir/file02.txt',
            test_dir / 'nested_dir/file03.txt'
        ]))
        infix_files = list(test_dir.walkfiles('a*b'))
        self.assertEquals(set(infix_files), set([
            test_dir / 'aaabbbab'
        ]))
        suffix_files = list(test_dir.walkfiles('a*'))
        self.assertEquals(set(suffix_files), set([
            test_dir / 'aaabbbab',
            test_dir / 'abaac.txt',
            test_dir / 'nested_dir/abababa'
        ]))
        more_files = list(test_dir.walkfiles('*ab*'))
        self.assertEquals(set(more_files), set([
            test_dir / 'aaabbbab',
            test_dir / 'aba/babba.txt',
            test_dir / 'abaac.txt',
            test_dir / 'nested_dir/abababa'
        ]))
