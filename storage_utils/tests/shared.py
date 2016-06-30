import unittest
from six.moves import builtins
from storage_utils import Path


def assert_same_data(fp1, fp2):
    actual_data = fp1.read(100)
    expected_data = fp2.read(100)
    while (expected_data or actual_data):
        assert actual_data == expected_data
        actual_data = fp1.read(100)
        expected_data = fp2.read(100)


class BaseIntegrationTest(unittest.TestCase):
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
