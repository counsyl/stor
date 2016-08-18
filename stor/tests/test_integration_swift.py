import logging
import os
import time
import unittest
import uuid

import mock
import requests

import stor
from stor import exceptions
from stor import NamedTemporaryDirectory
from stor import Path
from stor import settings
from stor import swift
from stor import utils
from stor.tests.test_integration import BaseIntegrationTest


class SwiftIntegrationTest(BaseIntegrationTest.BaseTestCases):
    def setUp(self):
        super(SwiftIntegrationTest, self).setUp()

        if not os.environ.get('SWIFT_TEST_USERNAME'):
            raise unittest.SkipTest(
                'SWIFT_TEST_USERNAME env var not set. Skipping integration test')

        # Disable loggers so nose output wont be trashed
        logging.getLogger('requests').setLevel(logging.CRITICAL)
        logging.getLogger('swiftclient').setLevel(logging.CRITICAL)
        logging.getLogger('keystoneclient').setLevel(logging.CRITICAL)

        settings.update({
            'swift': {
                'username': os.environ.get('SWIFT_TEST_USERNAME'),
                'password': os.environ.get('SWIFT_TEST_PASSWORD'),
                'num_retries': 5
            }})

        self.test_container = Path('swift://%s/%s' % ('AUTH_swft_test', uuid.uuid4()))
        if self.test_container.exists():
            raise ValueError('test container %s already exists.' % self.test_container)

        try:
            self.test_container.post()
        except:
            self.test_container.rmtree()
            raise

        self.test_dir = self.test_container / 'test'

    def tearDown(self):
        super(SwiftIntegrationTest, self).tearDown()
        self.test_container.rmtree()

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
            swift._cached_auth_token_map['AUTH_swft_test']['creds']['os_auth_token'] = 'bad_auth'
            s = Path(self.test_container).stat()
            self.assertEquals(s['Account'], 'AUTH_swft_test')
            self.assertEquals(len(mock_get_ks.call_args_list), 2)
            # Note that the auth_token is passed into the keystone client but then popped
            # from the kwargs. Assert that an auth token is no longer part of the retry calls
            self.assertTrue('auth_token' not in mock_get_ks.call_args_list[0][0][3])
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

    def test_static_large_obj_copy_and_segment_container(self):
        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            segment_size = 1048576
            obj_size = segment_size * 4 + 100
            self.create_dataset(tmp_d, 1, obj_size)
            obj_path = stor.join(tmp_d,
                                 self.get_dataset_obj_names(1)[0])
            options = {'swift:upload': {'segment_size': segment_size}}
            with settings.use(options):
                obj_path.copy(self.test_container / 'large_object.txt')

            # Verify there is a segment container and that it can be ignored when listing a dir
            segment_container = Path(self.test_container.parent) / ('.segments_%s' % self.test_container.name)  # nopep8
            containers = Path(self.test_container.parent).listdir(ignore_segment_containers=False)
            self.assertTrue(segment_container in containers)
            self.assertTrue(self.test_container in containers)
            containers = Path(self.test_container.parent).listdir(ignore_segment_containers=True)
            self.assertFalse(segment_container in containers)
            self.assertTrue(self.test_container in containers)

            # Verify there are five segments
            objs = set(segment_container.list(condition=lambda results: len(results) == 5))
            self.assertEquals(len(objs), 5)

            # Copy back the large object and verify its contents
            obj_path = Path(tmp_d) / 'large_object.txt'
            Path(self.test_container / 'large_object.txt').copy(obj_path)
            self.assertCorrectObjectContents(obj_path, self.get_dataset_obj_names(1)[0], obj_size)

    def test_temp_url(self):
        basic_file = 'test.txt'
        complex_file = 'my test?file=special_chars.txt'
        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            nested_tmp_dir = stor.join(tmp_d, 'tmp')
            os.mkdir(nested_tmp_dir)
            basic_file_p = stor.join(nested_tmp_dir, basic_file)
            complex_file_p = stor.join(nested_tmp_dir, 'my test?file=special_chars.txt')

            with stor.open(basic_file_p, 'w') as f:
                f.write('basic test')
            with stor.open(complex_file_p, 'w') as f:
                f.write('complex test')

            self.test_container.upload(['.'])

        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            basic_obj = stor.Path(
                stor.join(self.test_container, 'tmp', basic_file))
            basic_temp_url = basic_obj.temp_url(inline=False, filename=basic_file)
            r = requests.get(basic_temp_url)
            self.assertEquals(r.content, 'basic test')
            self.assertEquals(r.headers['Content-Disposition'],
                              'attachment; filename="test.txt"; filename*=UTF-8\'\'test.txt')

            complex_obj = stor.Path(
                stor.join(self.test_container, 'tmp', complex_file))
            complex_temp_url = complex_obj.temp_url(inline=False, filename=complex_file)
            r = requests.get(complex_temp_url)
            self.assertEquals(r.content, 'complex test')
            self.assertEquals(r.headers['Content-Disposition'],
                              'attachment; filename="my test%3Ffile%3Dspecial_chars.txt"; filename*=UTF-8\'\'my%20test%3Ffile%3Dspecial_chars.txt')  # nopep8

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

        num_retries = settings.get()['swift']['num_retries']
        with mock.patch('time.sleep') as mock_sleep:
            with self.assertRaises(swift.ConditionNotMetError):
                test_dir.list(condition=lambda results: expected_objs == set(results))
            self.assertTrue(num_retries > 0)
            self.assertEquals(len(mock_sleep.call_args_list), num_retries)

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
            Path('.').copytree(test_dir)

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

    def test_copytree_w_headers(self):
        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            open(tmp_d / 'test_obj', 'w').close()
            stor.copytree(
                '.',
                self.test_container,
                headers=['X-Delete-After:1000'])

        obj = stor.join(self.test_container, 'test_obj')
        stat_results = obj.stat()
        self.assertTrue('x-delete-at' in stat_results['headers'])

    def test_rmtree(self):
        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            # Make a couple empty test files and nested files
            tmp_d = Path(tmp_d)
            os.mkdir(tmp_d / 'my_dir')
            open(tmp_d / 'my_dir' / 'dir_file1', 'w').close()
            open(tmp_d / 'my_dir' / 'dir_file2', 'w').close()
            open(tmp_d / 'base_file1', 'w').close()
            open(tmp_d / 'base_file2', 'w').close()

            stor.copytree(
                '.',
                self.test_container,
                use_manifest=True)

            swift_dir = self.test_container / 'my_dir'
            self.assertEquals(len(swift_dir.list()), 2)
            swift_dir.rmtree()
            self.assertEquals(len(swift_dir.list()), 0)

            base_contents = self.test_container.list()
            self.assertTrue((self.test_container / 'base_file1') in base_contents)
            self.assertTrue((self.test_container / 'base_file1') in base_contents)

            self.test_container.rmtree()

            # TODO figure out a better way to test that the container no longer exists.
            with self.assertRaises(swift.NotFoundError):
                # Replication may have not happened yet for container deletion. Keep
                # listing in intervals until a NotFoundError is thrown
                for i in (0, 1, 3):
                    time.sleep(i)
                    self.test_container.list()

    def test_is_methods(self):
        container = self.test_container
        container = self.test_container
        file_with_prefix = stor.join(container, 'analysis.txt')

        # ensure container is created but empty
        container.post()
        self.assertTrue(stor.isdir(container))
        self.assertFalse(stor.isfile(container))
        self.assertTrue(stor.exists(container))
        self.assertFalse(stor.listdir(container))

        folder = stor.join(container, 'analysis')
        subfolder = stor.join(container, 'analysis', 'alignments')
        file_in_folder = stor.join(container, 'analysis', 'alignments',
                                   'bam.bam')
        self.assertFalse(stor.exists(file_in_folder))
        self.assertFalse(stor.isdir(folder))
        self.assertFalse(stor.isdir(folder + '/'))
        with stor.open(file_with_prefix, 'w') as fp:
            fp.write('data\n')
        self.assertFalse(stor.isdir(folder))
        self.assertTrue(stor.isfile(file_with_prefix))

        with stor.open(file_in_folder, 'w') as fp:
            fp.write('blah.txt\n')

        self.assertTrue(stor.isdir(folder))
        self.assertFalse(stor.isfile(folder))
        self.assertTrue(stor.isdir(subfolder))

    def test_metadata_pulling(self):
        file_in_folder = stor.join(self.test_container,
                                   'somefile.svg')
        with stor.open(file_in_folder, 'w') as fp:
            fp.write('12345\n')

        self.assertEqual(stor.getsize(file_in_folder), 6)
        stat_data = stor.Path(file_in_folder).stat()
        self.assertIn('Content-Type', stat_data)
        self.assertEqual(stat_data['Content-Type'], 'image/svg+xml')

    def test_push_metadata(self):
        obj = self.test_container / 'object.txt'
        with obj.open('w') as fp:
            fp.write('a\n')
        obj.post({'header': ['X-Object-Meta-Custom:text']})
        stat_data = obj.stat()
        # TODO(jtratner): consider validating x-object-meta vs.
        # x-container-meta (otherwise headers won't take)
        self.assertIn('x-object-meta-custom', stat_data['headers'])
        self.assertEqual(stat_data['headers']['x-object-meta-custom'], 'text')
        self.test_container.post({'header': ['X-Container-Meta-Exciting:value'],
                                  'read_acl': '.r:*'})
        stat_data = self.test_container.stat()
        self.assertEqual(stat_data['Read-ACL'], '.r:*')
        self.assertIn('x-container-meta-exciting', stat_data['headers'])
        self.assertEqual(stat_data['headers']['x-container-meta-exciting'], 'value')
        self.test_container.post({'read_acl': '.r:example.com'})
        self.assertEqual(self.test_container.stat()['Read-ACL'],
                         '.r:example.com')

    def test_copytree_to_from_dir_w_manifest(self):
            num_test_objs = 10
            test_obj_size = 100
            with NamedTemporaryDirectory(change_dir=True) as tmp_d:
                self.create_dataset(tmp_d, num_test_objs, test_obj_size)
                # Make a nested file and an empty directory for testing purposes
                tmp_d = Path(tmp_d)
                os.mkdir(tmp_d / 'my_dir')
                open(tmp_d / 'my_dir' / 'empty_file', 'w').close()
                os.mkdir(tmp_d / 'my_dir' / 'empty_dir')

                stor.copytree(
                    '.',
                    self.test_dir,
                    use_manifest=True)

                # Validate the contents of the manifest file
                manifest_contents = utils.get_data_manifest_contents(self.test_dir)
                expected_contents = self.get_dataset_obj_names(num_test_objs)
                expected_contents.extend(['my_dir/empty_file',
                                          'my_dir/empty_dir'])
                expected_contents = [Path('test') / c for c in expected_contents]
                self.assertEquals(set(manifest_contents), set(expected_contents))

            with NamedTemporaryDirectory(change_dir=True) as tmp_d:
                # Download the results successfully
                Path(self.test_dir).copytree(
                    'test',
                    use_manifest=True)

                # Now delete one of the objects from swift. A second download
                # will fail with a condition error
                Path(self.test_dir / 'my_dir' / 'empty_dir').remove()
                with self.assertRaises(exceptions.ConditionNotMetError):
                    Path(self.test_dir).copytree(
                        'test',
                        use_manifest=True,
                        num_retries=0)
