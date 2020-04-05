# -*- coding: utf-8 -*-
import gzip
import os
import unittest

import pytest

import stor
from stor import NamedTemporaryDirectory
from stor import Path
from stor.tests.shared import assert_same_data

# UTF-8 encoding
# TODO (jtratner): decide how to handle this on non-utf8 systems
BYTE_STRING = b"\xf0\x9f\x98\xb8"
STRING_STRING = u"hey😸"


class BaseIntegrationTest(object):
    """A wrapper class for common test cases so they aren't executed on their
    own as part of the base test class.
    """
    class BaseTestCases(unittest.TestCase):
        def get_dataset_obj_names(self, num_test_files):
            """Returns the name of objects in a test dataset generated with create_dataset"""
            return [str(name) for name in range(num_test_files)]

        def get_dataset_obj_contents(self, which_test_file, min_object_size):
            """Returns the object contents from a test file generated with create_dataset"""
            return str(which_test_file) * min_object_size

        def create_dataset(self, directory, num_objects, min_object_size):
            """Creates a test dataset with predicatable names and contents

            Files are named from 0 to num_objects (exclusive), and their contents
            is file_name * min_object_size. Note that the actual object size is
            dependent on the object name and should be taken into consideration
            when testing.
            """
            Path(directory).makedirs_p()
            with Path(directory):
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

        def _skip_if_filesystem(self, dirname, explanation='needs bytes/str fix'):
            if stor.is_filesystem_path(dirname):
                raise unittest.SkipTest('Skipping filesystem test: %s' % explanation)

        def test_copy_to_from_dir(self):
            num_test_objs = 5
            min_obj_size = 100
            with NamedTemporaryDirectory(change_dir=True) as tmp_d:
                self.create_dataset(tmp_d, num_test_objs, min_obj_size)
                for which_obj in self.get_dataset_obj_names(num_test_objs):
                    obj_path = stor.join(self.test_dir, '%s.txt' % which_obj)
                    stor.copy(which_obj, obj_path)
                    stor.copy(obj_path, 'copied_file')
                    self.assertCorrectObjectContents('copied_file', which_obj, min_obj_size)

        def test_copytree_to_from_dir(self):
            num_test_objs = 10
            test_obj_size = 100
            with NamedTemporaryDirectory(change_dir=True) as tmp_d:
                self.create_dataset(tmp_d, num_test_objs, test_obj_size)
                stor.copytree(
                    '.',
                    self.test_dir)

            with NamedTemporaryDirectory(change_dir=True) as tmp_d:
                self.test_dir.copytree(
                    'test',
                    condition=lambda results: len(results) == num_test_objs)

                # Verify contents of all downloaded test objects
                for which_obj in self.get_dataset_obj_names(num_test_objs):
                    obj_path = Path('test') / which_obj
                    self.assertCorrectObjectContents(obj_path, which_obj, test_obj_size)

        def test_hidden_file_nested_dir_copytree(self):
            with NamedTemporaryDirectory(change_dir=True):
                open('.hidden_file', 'w').close()
                os.symlink('.hidden_file', 'symlink')
                os.mkdir('.hidden_dir')
                os.mkdir('.hidden_dir/nested')
                open('.hidden_dir/nested/file1', 'w').close()
                open('.hidden_dir/nested/file2', 'w').close()
                Path('.').copytree(self.test_dir)

            with NamedTemporaryDirectory(change_dir=True):
                self.test_dir.copytree('test', condition=lambda results: len(results) == 4)
                self.assertTrue(Path('test/.hidden_file').isfile())
                self.assertTrue(Path('test/symlink').isfile())
                self.assertTrue(Path('test/.hidden_dir').isdir())
                self.assertTrue(Path('test/.hidden_dir/nested').isdir())
                self.assertTrue(Path('test/.hidden_dir/nested/file1').isfile())
                self.assertTrue(Path('test/.hidden_dir/nested/file2').isfile())

        def test_walkfiles(self):
            with NamedTemporaryDirectory(change_dir=True):
                # Make a dataset with files that will match a particular pattern (*.sh)
                # and also empty directories that should be ignored when calling walkfiles
                open('aabc.sh', 'w').close()
                open('aabc', 'w').close()
                os.mkdir('b')
                open('b/c.sh', 'w').close()
                os.mkdir('empty')
                open('b/d', 'w').close()
                open('b/abbbc', 'w').close()
                Path('.').copytree(self.test_dir)

            unfiltered_files = list(self.test_dir.walkfiles())
            self.assertEquals(set(unfiltered_files), set([
                stor.join(self.test_dir, 'aabc.sh'),
                stor.join(self.test_dir, 'aabc'),
                stor.join(self.test_dir, 'b/c.sh'),
                stor.join(self.test_dir, 'b/d'),
                stor.join(self.test_dir, 'b/abbbc'),
            ]))
            prefix_files = list(self.test_dir.walkfiles('*.sh'))
            self.assertEquals(set(prefix_files), set([
                stor.join(self.test_dir, 'aabc.sh'),
                stor.join(self.test_dir, 'b/c.sh'),
            ]))
            double_infix_files = list(self.test_dir.walkfiles('a*b*c'))
            self.assertEquals(set(double_infix_files), set([
                stor.join(self.test_dir, 'aabc'),
                stor.join(self.test_dir, 'b/abbbc'),
            ]))
            suffix_files = list(self.test_dir.walkfiles('a*'))
            self.assertEquals(set(suffix_files), set([
                stor.join(self.test_dir, 'aabc.sh'),
                stor.join(self.test_dir, 'aabc'),
                stor.join(self.test_dir, 'b/abbbc'),
            ]))
            # should still *make* an empty directory
            assert stor.exists(stor.join(self.test_dir, 'empty'))

        def test_gzip_on_remote(self):
            self._skip_if_filesystem(self.test_dir)
            local_gzip = os.path.join(os.path.dirname(__file__),
                                      'file_data/s_3_2126.bcl.gz')
            remote_gzip = stor.join(self.test_dir,
                                    stor.basename(local_gzip))
            stor.copy(local_gzip, remote_gzip)
            with stor.open(remote_gzip, mode='rb') as fp:
                with gzip.GzipFile(fileobj=fp) as remote_gzip_fp:
                    with gzip.open(local_gzip) as local_gzip_fp:
                        assert_same_data(remote_gzip_fp, local_gzip_fp)

        def test_file_read_write(self):
            self._skip_if_filesystem(self.test_dir)
            non_with_file = self.test_dir / 'nonwithfile.txt'
            test_file = self.test_dir / 'test_file.txt'
            copy_file = self.test_dir / 'copy_file.txt'

            fp = stor.open(non_with_file, mode='wb')
            # File opened in wb mode requires: bytes on py3k, str on py27
            fp.write('blah'.encode())
            del fp

            self.assertTrue(non_with_file.exists())
            self.assertTrue(non_with_file.isfile())
            self.assertFalse(non_with_file.isdir())

            with test_file.open(mode='wb') as obj:
                obj.write('this is a test\n'.encode())
                obj.write('this is another line.\n'.encode())

            self.assertTrue(test_file.exists())
            self.assertTrue(test_file.isfile())
            self.assertFalse(test_file.isdir())

            with test_file.open(mode='rb') as obj:
                with copy_file.open(mode='wb') as copy_obj:
                    copy_obj.write(obj.read())

            self.assertTrue(copy_file.exists())
            self.assertTrue(copy_file.isfile())
            self.assertFalse(copy_file.isdir())

            test_contents = test_file.open(mode='rb').read()
            copy_contents = copy_file.open(mode='rb').read()
            self.assertEquals(test_contents, 'this is a test\nthis is another line.\n'.encode())
            self.assertEquals(test_contents, copy_contents)

        def test_write_bytes_to_binary(self):
            test_file = self.test_dir / 'test_file.txt'
            with stor.open(test_file, mode='wb') as fp:
                fp.write(BYTE_STRING)

        # Python 3 is quite strict on string handling

        def test_write_string_to_binary(self):
            with pytest.raises(TypeError):
                test_file = self.test_dir / 'test_file.txt'
                with stor.open(test_file, mode='wb') as fp:
                    fp.write(STRING_STRING)

        def test_write_bytes_to_text(self):   # pragma: no cover
            with pytest.raises(TypeError):
                test_file = self.test_dir / 'test_file.txt'
                with stor.open(test_file, mode='w') as fp:
                    fp.write(BYTE_STRING)

        def test_write_string_to_text(self):
            test_file = self.test_dir / 'test_file.txt'
            with stor.open(test_file, mode='w') as fp:
                fp.write(STRING_STRING)

        def test_read_bytes_from_binary(self):
            test_file = self.test_dir / 'test_file.txt'
            with stor.open(test_file, mode='wb') as fp:
                fp.write(BYTE_STRING)

            with stor.open(test_file, mode='rb') as fp:
                result = fp.read()
            assert result == BYTE_STRING

        def test_read_string_from_text(self):
            test_file = self.test_dir / 'test_file.txt'
            with stor.open(test_file, mode='w') as fp:
                fp.write(STRING_STRING)

            with stor.open(test_file, mode='r') as fp:
                result = fp.read()
            assert result == STRING_STRING

        def test_custom_encoding_text(self):
            test_file = self.test_dir / 'test_file.txt'
            with stor.open(test_file, mode='w', encoding='utf-16') as fp:
                fp.write(STRING_STRING)

            with stor.open(test_file, mode='r', encoding='utf-16') as fp:
                result = fp.read()
            assert result == STRING_STRING

            with pytest.raises(UnicodeDecodeError):
                with stor.open(test_file, mode='r', encoding='utf-8') as fp:
                    result = fp.read()
