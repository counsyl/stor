import logging
import os
import unittest

from storage_utils import exceptions
from storage_utils import NamedTemporaryDirectory
from storage_utils import Path
from storage_utils.tests.test_integration import BaseIntegrationTest


class S3IntegrationTest(BaseIntegrationTest.BaseTestCases):
    """
    Integration tests for S3. Note that for now, while upload/download/remove
    methods are not implemented, tests will use the existing stor-test-bucket
    bucket on S3.

    In order to run the tests, you must have valid AWS S3 credentials set in the
    following environment variables: AWS_DEFAULT_REGION, AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY. In order to have the tests actually run, you also need
    to additionally set the AWS_TEST_ACCESS_KEY_ID environment variable. For now,
    it can simply be set to any value.
    """
    def setUp(self):
        super(S3IntegrationTest, self).setUp()

        if not (os.environ.get('AWS_TEST_ACCESS_KEY_ID') and
                os.environ.get('AWS_ACCESS_KEY_ID')):
            raise unittest.SkipTest(
                'AWS_TEST_ACCESS_KEY_ID env var not set. Skipping integration test')

        # Disable loggers so nose output is clean
        logging.getLogger('botocore').setLevel(logging.CRITICAL)

        self.test_bucket = Path('s3://stor-test-bucket')
        self.test_dir = self.test_bucket / 'test'

    def tearDown(self):
        super(S3IntegrationTest, self).tearDown()
        self.test_dir.rmtree()

    # @unittest.skip("test takes a long time. run manually if you want to test.")
    def test_over_1000_files(self):
        num_test_objs = 1234
        min_obj_size = 0

        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.create_dataset(tmp_d, num_test_objs, min_obj_size)
            self.test_dir.upload(['.'])

        self.assertEquals(1234, len(self.test_dir.list()))
        self.assertEquals(1200, len(self.test_dir.list(limit=1200)))
        self.assertTrue(self.test_dir.isdir())

        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.test_dir.download('./')
            self.assertEquals(1234, len(os.listdir(tmp_d)))

    # @unittest.skip('skipping')
    def test_list_methods(self):
        fake_bucket = Path('s3://stor-test-bucket2')
        with self.assertRaises(exceptions.NotFoundError):
            fake_bucket.list()
        fake_folder = self.test_bucket / 'not_a_dir'
        self.assertEquals([], fake_folder.list())

        with NamedTemporaryDirectory(change_dir=True):
            open('file1.txt', 'w').close()
            open('file2.txt', 'w').close()
            os.mkdir('nested_dir')
            os.mkdir('nested_dir/dir')
            open('nested_dir/dir/file3.txt', 'w').close()
            self.test_dir.upload(['.'])

        file_list = self.test_dir.list()
        starts_with_list = self.test_bucket.list(starts_with='test')
        self.assertEquals(set(file_list), set(starts_with_list))
        self.assertEquals(set(file_list), set([
            self.test_dir / 'file1.txt',
            self.test_dir / 'file2.txt',
            self.test_dir / 'nested_dir/dir/file3.txt'
        ]))

        dir_list = self.test_dir.listdir()
        self.assertEquals(set(dir_list), set([
            self.test_dir / 'file1.txt',
            self.test_dir / 'file2.txt',
            self.test_dir / 'nested_dir/'
        ]))

        self.assertTrue(self.test_dir.listdir() == (self.test_dir + '/').listdir())

    # @unittest.skip('skipping')
    def test_is_methods(self):
        """
        Tests is methods, exists(), and getsize().
        getsize() integration test may be moved to a different test
        depending on whether other metadata methods (such as stat())
        are implemented.
        """
        self.assertTrue(self.test_bucket.exists())
        self.assertTrue(self.test_bucket.isdir())
        self.assertFalse(self.test_bucket.isfile())
        self.assertEquals(self.test_bucket.getsize(), 0)

        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.create_dataset(tmp_d, 1, 10)
            self.test_dir.upload(['.'])
            correct_size = os.path.getsize('0')

        self.assertTrue(self.test_dir.exists())
        self.assertTrue(self.test_dir.isdir())
        self.assertFalse(self.test_dir.isfile())
        self.assertEquals(self.test_dir.getsize(), 0)

        test_file = self.test_dir / '0'
        self.assertTrue(test_file.exists())
        self.assertFalse(test_file.isdir())
        self.assertTrue(test_file.isfile())
        self.assertEquals(test_file.getsize(), correct_size)

        test_file.remove()
        self.assertFalse(test_file.exists())
        self.assertFalse(test_file.isdir())
        self.assertFalse(test_file.isfile())
        with self.assertRaises(exceptions.NotFoundError):
            test_file.getsize()

        fake_bucket = self.test_bucket + '2'
        self.assertFalse(fake_bucket.exists())
        self.assertFalse(fake_bucket.isdir())
        self.assertFalse(fake_bucket.isfile())
        with self.assertRaises(exceptions.NotFoundError):
            fake_bucket.getsize()

    # @unittest.skip('skipping')
    def test_upload_download_remove(self):
        num_test_objs = 10
        min_obj_size = 50
        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.create_dataset(tmp_d, num_test_objs, min_obj_size)
            self.test_dir.upload(['.'])

        for which_obj in self.get_dataset_obj_names(num_test_objs):
            self.assertTrue((self.test_dir / which_obj).exists())

        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.test_dir.download(tmp_d)
            for which_obj in self.get_dataset_obj_names(num_test_objs):
                self.assertCorrectObjectContents(which_obj, which_obj, min_obj_size)
                (self.test_dir / which_obj).remove()
                self.assertFalse((self.test_dir / which_obj).exists())

    # @unittest.skip('skipping')
    def test_upload_w_headers(self):
        test_file = self.test_dir / 'a.txt'
        with NamedTemporaryDirectory(change_dir=True):
            open('a.txt', 'w').close()
            self.test_dir.upload(['.'], headers={'ContentLanguage': 'en'})

        self.assertTrue(test_file.exists())
        self.assertEquals(test_file.stat()['ContentLanguage'], 'en')

    # @unittest.skip('skipping')
    def test_download(self):
        with NamedTemporaryDirectory(change_dir=True):
            os.mkdir('dir')
            os.mkdir('dir/a')
            open('dir/a/a.txt', 'w').close()
            self.test_dir.upload(['.'])

        with NamedTemporaryDirectory(change_dir=True):
            open('dir', 'w').close()
            open('a', 'w').close()
            with self.assertRaises(OSError):
                self.test_dir.download('.')
            with self.assertRaises(OSError):
                (self.test_dir / 'dir').download('.')

    def test_condition(self):
        num_test_objs = 20
        test_obj_size = 100
        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.create_dataset(tmp_d, num_test_objs, test_obj_size)
            Path('.').copytree(self.test_dir)

        # Verify a ConditionNotMet exception is thrown when attempting to list
        # a file that hasn't been uploaded
        expected_objs = {
            self.test_dir / which_obj
            for which_obj in self.get_dataset_obj_names(num_test_objs + 1)
        }

        with self.assertRaises(exceptions.ConditionNotMetError):
            self.test_dir.list(condition=lambda results: expected_objs == set(results))

        # Verify that the condition passes when excluding the non-extant file
        expected_objs = {
            self.test_dir / which_obj
            for which_obj in self.get_dataset_obj_names(num_test_objs)
        }
        objs = self.test_dir.list(condition=lambda results: expected_objs == set(results))
        self.assertEquals(expected_objs, set(objs))

    def test_dir_markers(self):
        with NamedTemporaryDirectory(change_dir=True):
            os.mkdir('empty')
            os.mkdir('dir')
            open('a.txt', 'w').close()
            open('dir/b.txt', 'w').close()
            self.test_dir.upload(['.'])

        self.assertEquals(set(self.test_dir.list()), {
            self.test_dir / 'a.txt',
            self.test_dir / 'dir/b.txt',
            self.test_dir / 'empty'
        })
        self.assertEquals(set(self.test_dir.list(ignore_dir_markers=True)), {
            self.test_dir / 'a.txt',
            self.test_dir / 'dir/b.txt'
        })
        self.assertTrue((self.test_dir / 'empty').isdir())

        with NamedTemporaryDirectory(change_dir=True):
            self.test_dir.download('.')
            self.assertTrue(os.path.isdir('empty'))
            self.assertTrue(os.path.exists('dir/b.txt'))
            self.assertTrue(os.path.exists('a.txt'))
