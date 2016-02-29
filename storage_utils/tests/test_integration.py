import logging
import os
import unittest
import uuid

from storage_utils import NamedTemporaryDirectory
from storage_utils import path
from storage_utils import swift


class BaseIntegrationTest(unittest.TestCase):
    def setUp(self):
        super(BaseIntegrationTest, self).setUp()

        if not os.environ.get('SWIFT_TEST_USERNAME'):
            raise unittest.SkipTest('SWIFT_TEST_USERNAME env var not set. Skipping integration test')

        logging.getLogger('requests').setLevel(logging.CRITICAL)
        logging.getLogger('swiftclient').setLevel(logging.CRITICAL)
        logging.getLogger('keystoneclient').setLevel(logging.CRITICAL)

        swift.update_settings(username=os.environ.get('SWIFT_TEST_USERNAME'),
                              password=os.environ.get('SWIFT_TEST_PASSWORD'),
                              num_retries=0)

        self.test_container = path('swift://%s/%s' % ('AUTH_swft_test', uuid.uuid4()))
        if self.test_container.exists():
            raise ValueError('test container %s already exists.' % self.test_container)

        self.test_container.post()

    def tearDown(self):
        super(BaseIntegrationTest, self).tearDown()
        self.test_container.rmtree()

    def get_dataset_obj_names(self, num_test_files):
        return ['%s' % name for name in range(num_test_files)]

    def generate_dataset_obj_contents(self, which_test_file, object_size):
        return '%s' % str(which_test_file) * object_size

    def create_dataset(self, directory, num_objects, object_size):
        with path(directory):
            for name in self.get_dataset_obj_names(num_objects):
                with open(name, 'w') as f:
                    f.write(self.generate_dataset_obj_contents(name, object_size))

    def assertCorrectObjectContents(self, test_obj_name, which_test_obj, test_obj_size):
        with open(test_obj_name, 'r') as test_obj:
            contents = test_obj.read()
            expected = self.generate_dataset_obj_contents(which_test_obj, test_obj_size)
            self.assertEquals(contents, expected)


class SwiftIntegrationTest(BaseIntegrationTest):
    def test_copy_to_from_container(self):
        num_test_objs = 5
        test_obj_size = 100
        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.create_dataset(tmp_d, num_test_objs, test_obj_size)
            for which_obj in self.get_dataset_obj_names(num_test_objs):
                obj_path = path(self.test_container) / ('%s.txt' % which_obj)
                path(which_obj).copy(obj_path)
                obj_path.copy('copied_file')
                self.assertCorrectObjectContents('copied_file', which_obj, test_obj_size)

    def test_hidden_file_dir_copytree(self):
        test_swift_dir = path(self.test_container) / 'test'
        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            open('.hidden_file', 'w').close()
            os.symlink('.hidden_file', 'symlink')
            os.mkdir('.hidden_dir')
            open('.hidden_dir/file1', 'w').close()
            open('.hidden_dir/file2', 'w').close()
            path('.').copytree(test_swift_dir)

        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            test_swift_dir.copytree('test', swift_download_options={
                'condition': lambda results: len(results) == 4
            })
            self.assertTrue(path('test/.hidden_file').isfile())
            self.assertTrue(path('test/symlink').isfile())
            self.assertTrue(path('test/.hidden_dir').isdir())
            self.assertTrue(path('test/.hidden_dir/file1').isfile())
            self.assertTrue(path('test/.hidden_dir/file2').isfile())

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
        globbed_objs = set(test_dir.glob('1*', condition=lambda results: len(results) == len(expected_glob)))
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
