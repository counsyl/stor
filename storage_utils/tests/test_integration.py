import logging
import os
import unittest
import uuid

import mock

from six.moves import builtins

import storage_utils
from storage_utils import NamedTemporaryDirectory
from storage_utils import Path
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

        self.test_container = Path('swift://%s/%s' % ('AUTH_swft_test', uuid.uuid4()))
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
        with Path(directory):
            for name in self.get_dataset_obj_names(num_objects):
                with builtins.open(name, 'w') as f:
                    f.write(self.get_dataset_obj_contents(name, min_object_size))

    def assertCorrectObjectContents(self, test_obj_path, which_test_obj, min_obj_size):
        """
        Given a test object and the minimum object size used with create_dataset, assert
        that a file exists with the correct contents
        """
        with builtins.open(test_obj_path, 'r') as test_obj:
            contents = test_obj.read()
            expected = self.get_dataset_obj_contents(which_test_obj, min_obj_size)
            self.assertEquals(contents, expected)


class SwiftIntegrationTest(BaseIntegrationTest):
    def test_cached_auth_and_auth_invalidation(self):
        from swiftclient.client import get_auth_keystone as real_get_keystone
        swift._clear_cached_auth_credentials()
        with mock.patch('swiftclient.client.get_auth_keystone', autospec=True) as mock_get_ks:
            mock_get_ks.side_effect = real_get_keystone
            s = Path(self.test_container).stat()
            self.assertEquals(s['Account'], 'AUTH_swft_test')
            self.assertEquals(len(mock_get_ks.call_args_list), 1)

            # The keystone auth should not be called on another stat
            mock_get_ks.reset_mock()
            s = Path(self.test_container).stat()
            self.assertEquals(s['Account'], 'AUTH_swft_test')
            self.assertEquals(len(mock_get_ks.call_args_list), 0)

            # Set the auth cache to something bad. The auth keystone should
            # be called twice on another stat. It's first called by the swiftclient
            # when retrying auth (with the bad token) and then called by us without
            # a token after the swiftclient raises an authorization error.
            mock_get_ks.reset_mock()
            swift._cached_auth_token_map['AUTH_swft_test']['os_auth_token'] = 'bad_auth'
            s = Path(self.test_container).stat()
            self.assertEquals(s['Account'], 'AUTH_swft_test')
            self.assertEquals(len(mock_get_ks.call_args_list), 2)
            self.assertEquals(mock_get_ks.call_args_list[0][0][3]['auth_token'], 'bad_auth')
            self.assertTrue('auth_token' not in mock_get_ks.call_args_list[1][0][3])

            # Now make the auth always be invalid and verify that an auth error is thrown
            # This also tests that keystone auth errors are propagated as swift
            # AuthenticationErrors
            mock_get_ks.reset_mock()
            swift._clear_cached_auth_credentials()
            with mock.patch('keystoneclient.v2_0.client.Client') as mock_ks_client:
                from keystoneclient.exceptions import Unauthorized
                mock_ks_client.side_effect = Unauthorized
                with self.assertRaises(swift.AuthenticationError):
                    Path(self.test_container).stat()

                # Verify that getting the auth was called two more times because of retry
                # logic
                self.assertEquals(len(mock_get_ks.call_args_list), 2)

    def test_copy_to_from_container(self):
        num_test_objs = 5
        min_obj_size = 100
        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.create_dataset(tmp_d, num_test_objs, min_obj_size)
            for which_obj in self.get_dataset_obj_names(num_test_objs):
                obj_path = storage_utils.join(self.test_container, '%s.txt' % which_obj)
                storage_utils.copy(which_obj, obj_path)
                storage_utils.copy(obj_path, 'copied_file')
                self.assertCorrectObjectContents('copied_file', which_obj, min_obj_size)

    def test_static_large_obj_copy(self):
        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            segment_size = 1048576
            obj_size = segment_size * 4 + 100
            self.create_dataset(tmp_d, 1, obj_size)
            obj_path = storage_utils.join(tmp_d,
                                          self.get_dataset_obj_names(1)[0])
            obj_path.copy(self.test_container / 'large_object.txt', swift_retry_options={
                'segment_size': segment_size
            })

            # Verify there are five segments
            segment_container = Path(self.test_container.parent) / ('.segments_%s' % self.test_container.name)  # nopep8
            objs = set(segment_container.list(condition=lambda results: len(results) == 5))
            self.assertEquals(len(objs), 5)

            # Copy back the large object and verify its contents
            obj_path = Path(tmp_d) / 'large_object.txt'
            Path(self.test_container / 'large_object.txt').copy(obj_path)
            self.assertCorrectObjectContents(obj_path, self.get_dataset_obj_names(1)[0], obj_size)

    def test_hidden_file_nested_dir_copytree(self):
        test_swift_dir = Path(self.test_container) / 'test'
        with NamedTemporaryDirectory(change_dir=True):
            builtins.open('.hidden_file', 'w').close()
            os.symlink('.hidden_file', 'symlink')
            os.mkdir('.hidden_dir')
            os.mkdir('.hidden_dir/nested')
            builtins.open('.hidden_dir/nested/file1', 'w').close()
            builtins.open('.hidden_dir/nested/file2', 'w').close()
            Path('.').copytree(test_swift_dir)

        with NamedTemporaryDirectory(change_dir=True):
            test_swift_dir.copytree('test', swift_download_options={
                'condition': lambda results: len(results) == 4
            })
            self.assertTrue(Path('test/.hidden_file').isfile())
            self.assertTrue(Path('test/symlink').isfile())
            self.assertTrue(Path('test/.hidden_dir').isdir())
            self.assertTrue(Path('test/.hidden_dir/nested').isdir())
            self.assertTrue(Path('test/.hidden_dir/nested/file1').isfile())
            self.assertTrue(Path('test/.hidden_dir/nested/file2').isfile())

    def test_condition_failures(self):
        num_test_objs = 20
        test_obj_size = 100
        test_dir = self.test_container / 'test'
        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.create_dataset(tmp_d, num_test_objs, test_obj_size)
            Path('.').copytree(test_dir)

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
            storage_utils.copytree(
                '.',
                storage_utils.join(self.test_container, 'test'))

        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            path(self.test_container / 'test').copytree('test', swift_download_options={
                'condition': lambda results: len(results) == num_test_objs
            })

            # Verify contents of all downloaded test objects
            for which_obj in self.get_dataset_obj_names(num_test_objs):
                obj_path = path('test') / which_obj
                self.assertCorrectObjectContents(obj_path, which_obj, test_obj_size)

    def test_is_methods(self):
        container = self.test_container
        container = self.test_container
        file_with_prefix = storage_utils.join(container, 'analysis.txt')

        # ensure container is crated but empty
        sentinel = storage_utils.join(container, 'sentinel')
        with storage_utils.open(sentinel, 'w') as fp:
            fp.write('blah')
        storage_utils.remove(sentinel)
        self.assertTrue(storage_utils.isdir(container))
        self.assertFalse(storage_utils.isfile(container))
        self.assertTrue(storage_utils.exists(container))
        self.assertFalse(storage_utils.listdir(container))

        folder = storage_utils.join(container, 'analysis')
        subfolder = storage_utils.join(container, 'analysis', 'alignments')
        file_in_folder = storage_utils.join(container, 'analysis', 'alignments',
                                            'bam.bam')
        self.assertFalse(storage_utils.exists(file_in_folder))
        self.assertFalse(storage_utils.isdir(folder))
        self.assertFalse(storage_utils.isdir(folder + '/'))
        with storage_utils.open(file_with_prefix, 'w') as fp:
            fp.write('data\n')
        self.assertFalse(storage_utils.isdir(folder))
        self.assertTrue(storage_utils.isfile(file_with_prefix))

        with storage_utils.open(file_in_folder, 'w') as fp:
            fp.write('blah.txt\n')

        self.assertTrue(storage_utils.isdir(folder))
        self.assertFalse(storage_utils.isfile(folder))
        self.assertTrue(storage_utils.isdir(subfolder))
