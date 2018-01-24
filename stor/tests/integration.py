# -*- coding: utf-8 -*-
"""
Integration Tests Against Object Storage
========================================

Purpose
-------

A primary motivation for the stor tool is to allow the same code to work across multiple storage
backends without changes. (POSIX -> POSIX, POSIX -> OBS, and OBS -> POSIX)

This module both tests that code works as expected against the real object storage services, it
also serves to check that the same code really does behave in the same way against multiple
backends.

Test Structure
--------------

Module Overview
^^^^^^^^^^^^^^^

* ``integration.py`` - generic test cases
* ``test_integration_s3.py`` - runs only against S3 (when AWS_TEST_* env vars are set)
* ``test_integration_swift.py`` - runs only against Swift (when SWIFT_TEST_* env vars are set)
* ``test_integration_cross_obs.py`` - runs against Swift & S3 (with both sets of TEST env vars)

Base Integration Test Classes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* `BaseIntegrationTest` - implements helper methods to generate and check object data
* `SourceDestTestCase` - tests that upload and download between services
* `FromFilesystemTestCase` - tests that only make sense with a POSIX source

How it Works
------------

This file (``integration.py``) holds generic test cases that should work between any storage
system. The test cases are wrapped in a ``integration`` class so that py.test does not pick them
up.

Each service "implements" the integration test by setting ``self.src_dir`` and ``self.test_dir``,
which the individual tests use to target different combinations of src and dest. ``self.src_dir``
and ``self.test_dir`` automatically clean themselves up.

Per-service test files may implement additional tests to test functionality that's specific to
its service.


Guidelines for Adding or Updating Tests
---------------------------------------

* **Use helper methods on BaseIntegrationTest**
* **stick to the functional API** for cross-compatible tests (i.e., everything
  available as ``from stor import *``) and avoid using object methods (which may not be available
  cross-service).
* Always **use ``self.src_dir`` and ``self.test_dir``** as your targets.
* **Be judicious** - tests are slow! Every test you write will be run many times (8 times for
  ``SourceDestTestCase`` and 3 times for ``FromFilesystemTestCase``) Think about whether it's
  necessary to run with as many objects or do as many checks.
"""
import gzip
import os
import unittest
import six
from nose.tools import raises
from unittest import skipIf

import pytest

import stor
from stor import NamedTemporaryDirectory
from stor import Path
from stor.tests.shared import assert_same_data

# UTF-8 encoding
# TODO (jtratner): decide how to handle this on non-utf8 systems
BYTE_STRING = b"\xf0\x9f\x98\xb8"
STRING_STRING = u"hey😸"


class BaseIntegrationTest(unittest.TestCase):
    """Helper methods for writing integration tests (split out for ease of the reader)"""
    def tearDown(self):
        super(BaseIntegrationTest, self).tearDown()
        try:
            stor.rmtree(self.test_dir)
        except stor.exceptions.NotFoundError:
            pass

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
        for name in self.get_dataset_obj_names(num_objects):
            with stor.open(stor.join(directory, name), 'w') as f:
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


class SourceDestTestCase(BaseIntegrationTest):
    """TestCases that attempt to copy to/from a target directory"""
    def test_copy_to_from_dir(self):
        num_test_objs = 5
        min_obj_size = 100
        self.create_dataset(self.src_dir, num_test_objs, min_obj_size)
        for which_obj in self.get_dataset_obj_names(num_test_objs):
            obj_path = stor.join(self.test_dir, '%s.txt' % which_obj)
            stor.copy(stor.join(self.src_dir, which_obj), obj_path)
            stor.copy(obj_path, 'copied_file')
            self.assertCorrectObjectContents('copied_file', which_obj, min_obj_size)

    def test_copytree_to_from_dir(self):
        num_test_objs = 10
        test_obj_size = 100
        upload_from = stor.join(self.src_dir, 'upload')
        download_to = stor.join(self.src_dir, 'download')
        self.create_dataset(upload_from, num_test_objs, test_obj_size)
        stor.copytree(upload_from, self.test_dir)
        self.test_dir.copytree(
            download_to,
            condition=lambda results: len(results) == num_test_objs)  # pragma: no cover

        # Verify contents of all downloaded test objects
        for which_obj in self.get_dataset_obj_names(num_test_objs):
            obj_path = download_to / which_obj
            self.assertCorrectObjectContents(obj_path, which_obj, test_obj_size)


class FromFilesystemTestCase(SourceDestTestCase):
    """TestCases with src dir as file system"""
    def setUp(self):
        ntd = NamedTemporaryDirectory()
        self.src_dir = ntd.__enter__()
        self.addCleanup(ntd.__exit__, None, None, None)

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
            self.test_dir.copytree('test',
                                   condition=lambda results: len(results) == 4)  # pragma: no cover
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

    # python 2 is quite lenient about string types on I/O

    @skipIf(not six.PY2, "Only tested on py2")
    def test_write_string_to_binary_py2(self):  # pragma: no cover
        test_file = self.test_dir / 'test_file.txt'
        with stor.open(test_file, mode='wb') as fp:
            fp.write(u'myasciistring')
        with stor.open(test_file, mode='wb') as fp:
            fp.write(b'myasciistring')

    @skipIf(not six.PY2, "Only tested on py2")
    def test_write_bytes_to_text_py2(self):  # pragma: no cover
        test_file = self.test_dir / 'test_file.txt'
        with stor.open(test_file, mode='w') as fp:
            fp.write(b'myasciistring')
        with stor.open(test_file, mode='w') as fp:
            fp.write(u'myasciistring')

    # whereas Python 3 is quite strict

    @skipIf(six.PY2, "Only tested on py3")
    @raises(TypeError)
    def test_write_string_to_binary(self):   # pragma: no cover
        test_file = self.test_dir / 'test_file.txt'
        with stor.open(test_file, mode='wb') as fp:
            fp.write(STRING_STRING)

    @skipIf(six.PY2, "Only tested on py3")
    @raises(TypeError)
    def test_write_bytes_to_text(self):   # pragma: no cover
        test_file = self.test_dir / 'test_file.txt'
        with stor.open(test_file, mode='w') as fp:
            fp.write(BYTE_STRING)

    @skipIf(six.PY2, "Only tested on py3")
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

    @skipIf(six.PY2, "Only tested on py3")
    def test_read_string_from_text(self):
        test_file = self.test_dir / 'test_file.txt'
        with stor.open(test_file, mode='w') as fp:
            fp.write(STRING_STRING)

        with stor.open(test_file, mode='r') as fp:
            result = fp.read()
        assert result == STRING_STRING

    @skipIf(six.PY2, 'explicit encoding currently only supported on Python 3')
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

    @skipIf(not six.PY2, 'only check for encoding typeerrors on python 2')
    def test_encoding_typeerror_py2(self):  # pragma: no cover
        test_file = self.test_dir / 'test_file.txt'
        with pytest.raises(TypeError, regex='encoding'):
            stor.open(test_file, mode='r', encoding='utf-8')
        with pytest.raises(TypeError, regex='encoding'):
            stor.Path(test_file).open(mode='r', encoding='utf-8')
