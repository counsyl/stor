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

        swift.update_settings(username=os.environ.get('SWIFT_TEST_USERNAME'),
                              password=os.environ.get('SWIFT_TEST_PASSWORD'),
                              num_retries=5)

        self.test_container = path('swift://%s/%s' % ('AUTH_swft_test', uuid.uuid4()))
        if self.test_container.exists():
            raise ValueError('test container %s already exists.' % self.test_container)

        self.test_container.post()

    def tearDown(self):
        super(BaseIntegrationTest, self).tearDown()
        print 'removing container '
        self.test_container.rmtree()
        print 'done removing container'

    def get_dataset_obj_names(self, num_test_files):
        return ['%s' % name for name in range(num_test_files)]

    def generate_dataset_obj_contents(self, which_test_file, object_size):
        return '%s' % str(which_test_file) * object_size

    def create_dataset(self, directory, num_objects, object_size):
        with path(directory):
            for name in self.get_dataset_obj_names(num_objects):
                with open(name, 'w') as f:
                    f.write(self.generate_dataset_obj_contents(name, object_size))


class SwiftIntegrationTest(BaseIntegrationTest):
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
            for obj_name in self.get_dataset_obj_names(num_test_objs):
                full_obj_name = path('test') / obj_name
                self.assertTrue(full_obj_name.exists())
                with open(full_obj_name, 'r') as test_obj:
                    contents = test_obj.read()
                    expected = self.generate_dataset_obj_contents(obj_name, test_obj_size)
                    self.assertEquals(contents, expected)
