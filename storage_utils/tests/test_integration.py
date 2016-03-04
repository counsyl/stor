import logging
import os
import unittest
import uuid

import mock

from storage_utils import NamedTemporaryDirectory
from storage_utils import path
from storage_utils import swift


class BaseIntegrationTest(unittest.TestCase):
    def setUp(self):
        super(BaseIntegrationTest, self).setUp()

        if not os.environ.get('SWIFT_TEST_USERNAME'):
            raise unittest.SkipTest(
                'SWIFT_TEST_USERNAME env var not set. Skipping integration test')

        # Disable loggers so nose output wont be trashed
        logging.getLogger('requests').setLevel(logging.CRITICAL)
        logging.getLogger('swiftclient').setLevel(logging.CRITICAL)
        logging.getLogger('keystoneclient').setLevel(logging.CRITICAL)

        swift.update_settings(username=os.environ.get('SWIFT_TEST_USERNAME'),
                              password=os.environ.get('SWIFT_TEST_PASSWORD'),
                              num_retries=5)

        self.test_container = path('swift://%s/%s' % ('AUTH_swft_test', uuid.uuid4()))
        if self.test_container.exists():
            raise ValueError('test container %s already exists.' % self.test_container)

        try:
            self.test_container.post()
        except:
            self.test_container.rmtree()
            raise

    def tearDown(self):
        super(BaseIntegrationTest, self).tearDown()
        self.test_container.rmtree()

    def get_dataset_obj_names(self, num_test_files):
        """Returns the name of objects in a test dataset generated with create_dataset"""
        return ['%s' % name for name in range(num_test_files)]

    def get_dataset_obj_contents(self, which_test_file, min_object_size):
        """Returns the object contents from a test file generated with create_dataset"""
        return '%s' % str(which_test_file) * min_object_size

    def create_dataset(self, directory, num_objects, min_object_size):
        """Creates a test dataset with predicatable names and contents

        Files are named from 0 to num_objects (exclusive), and their contents
        is file_name * min_object_size. Note that the actual object size is
        dependent on the object name and should be taken into consideration
        when testing.
        """
        with path(directory):
            for name in self.get_dataset_obj_names(num_objects):
                with open(name, 'w') as f:
                    f.write(self.get_dataset_obj_contents(name, min_object_size))

    def assertCorrectObjectContents(self, test_obj_path, which_test_obj, min_obj_size):
        """
        Given a test object and the minimum object size used with create_dataset, assert
        that a file exists with the correct contents
        """
        with open(test_obj_path, 'r') as test_obj:
            contents = test_obj.read()
            expected = self.get_dataset_obj_contents(which_test_obj, min_obj_size)
            self.assertEquals(contents, expected)


class SwiftIntegrationTest(BaseIntegrationTest):
    def test_copy_to_from_container(self):
        num_test_objs = 5
        min_obj_size = 100
        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.create_dataset(tmp_d, num_test_objs, min_obj_size)
            for which_obj in self.get_dataset_obj_names(num_test_objs):
                obj_path = path(self.test_container) / ('%s.txt' % which_obj)
                path(which_obj).copy(obj_path)
                obj_path.copy('copied_file')
                self.assertCorrectObjectContents('copied_file', which_obj, min_obj_size)

    def test_static_large_obj_copy(self):
        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            segment_size = 1048576
            obj_size = segment_size * 4 + 100
            self.create_dataset(tmp_d, 1, obj_size)
            obj_path = path(tmp_d) / self.get_dataset_obj_names(1)[0]
            obj_path.copy(self.test_container / 'large_object.txt', swift_retry_options={
                'segment_size': segment_size
            })

            # Verify there are five segments
            segment_container = path(self.test_container.parent) / ('.segments_%s' % self.test_container.name)  # nopep8
            objs = set(segment_container.list(condition=lambda results: len(results) == 5))
            self.assertEquals(len(objs), 5)

            # Copy back the large object and verify its contents
            obj_path = path(tmp_d) / 'large_object.txt'
            path(self.test_container / 'large_object.txt').copy(obj_path)
            self.assertCorrectObjectContents(obj_path, self.get_dataset_obj_names(1)[0], obj_size)

    def test_hidden_file_nested_dir_copytree(self):
        test_swift_dir = path(self.test_container) / 'test'
        with NamedTemporaryDirectory(change_dir=True):
            open('.hidden_file', 'w').close()
            os.symlink('.hidden_file', 'symlink')
            os.mkdir('.hidden_dir')
            os.mkdir('.hidden_dir/nested')
            open('.hidden_dir/nested/file1', 'w').close()
            open('.hidden_dir/nested/file2', 'w').close()
            path('.').copytree(test_swift_dir)

        with NamedTemporaryDirectory(change_dir=True):
            test_swift_dir.copytree('test', swift_download_options={
                'condition': lambda results: len(results) == 4
            })
            self.assertTrue(path('test/.hidden_file').isfile())
            self.assertTrue(path('test/symlink').isfile())
            self.assertTrue(path('test/.hidden_dir').isdir())
            self.assertTrue(path('test/.hidden_dir/nested').isdir())
            self.assertTrue(path('test/.hidden_dir/nested/file1').isfile())
            self.assertTrue(path('test/.hidden_dir/nested/file2').isfile())

    def test_condition_failures(self):
        num_test_objs = 20
        test_obj_size = 100
        test_dir = self.test_container / 'test'
        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.create_dataset(tmp_d, num_test_objs, test_obj_size)
            path('.').copytree(test_dir)

        # Verify a ConditionNotMet exception is thrown when attempting to list
        # a file that hasn't been uploaded
        expected_objs = {
            test_dir / which_obj
            for which_obj in self.get_dataset_obj_names(num_test_objs + 1)
        }

        with mock.patch('time.sleep') as mock_sleep:
            with self.assertRaises(swift.ConditionNotMetError):
                test_dir.list(condition=lambda results: expected_objs == set(results))
            self.assertTrue(swift.num_retries > 0)
            self.assertEquals(len(mock_sleep.call_args_list), swift.num_retries)

        # Verify that the condition passes when excluding the non-extant file
        expected_objs = {
            test_dir / which_obj
            for which_obj in self.get_dataset_obj_names(num_test_objs)
        }
        objs = test_dir.list(condition=lambda results: expected_objs == set(results))
        self.assertEquals(expected_objs, set(objs))

    def test_list_glob(self):
        num_test_objs = 20
        test_obj_size = 100
        test_dir = self.test_container / 'test'
        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.create_dataset(tmp_d, num_test_objs, test_obj_size)
            path('.').copytree(test_dir)

        objs = set(test_dir.list(condition=lambda results: len(results) == num_test_objs))
        expected_objs = {
            test_dir / obj_name
            for obj_name in self.get_dataset_obj_names(num_test_objs)
        }
        self.assertEquals(len(objs), num_test_objs)
        self.assertEquals(objs, expected_objs)

        expected_glob = {
            test_dir / obj_name
            for obj_name in self.get_dataset_obj_names(num_test_objs) if obj_name.startswith('1')
        }
        self.assertTrue(len(expected_glob) > 1)
        globbed_objs = set(
            test_dir.glob('1*', condition=lambda results: len(results) == len(expected_glob)))
        self.assertEquals(globbed_objs, expected_glob)

    def test_copytree_to_from_container(self):
        num_test_objs = 10
        test_obj_size = 100
        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.create_dataset(tmp_d, num_test_objs, test_obj_size)
            path('.').copytree(self.test_container / 'test')

        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            path(self.test_container / 'test').copytree('test', swift_download_options={
                'condition': lambda results: len(results) == num_test_objs
            })

            # Verify contents of all downloaded test objects
            for which_obj in self.get_dataset_obj_names(num_test_objs):
                obj_path = path('test') / which_obj
                self.assertCorrectObjectContents(obj_path, which_obj, test_obj_size)
