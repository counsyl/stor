import os
from path import Path
import mock
import storage_utils
from storage_utils.swift import SwiftPath
from storage_utils import utils
from storage_utils.test import SwiftTestCase
import subprocess
import unittest


class TestCopy(SwiftTestCase):
    def test_two_swift_paths(self):
        with self.assertRaises(ValueError):
            utils.copy(storage_utils.path('swift://tenant/container1'),
                       storage_utils.path('swift://tenant/container2'))

    def test_two_posix_paths(self):
        with storage_utils.NamedTemporaryDirectory() as tmp_d:
            source = tmp_d / 'source'
            os.mkdir(source)
            with open(source / '1', 'w') as tmp_file:
                tmp_file.write('1')

            dest = tmp_d / 'my' / 'dest'
            utils.copy(source, dest)
            self.assertTrue((dest / '1').exists())

    def test_two_posix_paths_failed_command(self):
        with storage_utils.NamedTemporaryDirectory() as tmp_d:
            invalid_source = tmp_d / 'source'
            dest = tmp_d / 'my' / 'dest'

            with self.assertRaises(subprocess.CalledProcessError):
                utils.copy(invalid_source, dest)

    @mock.patch.object(SwiftPath, 'upload', autospec=True)
    def test_posix_to_swift(self, mock_upload):
        source = '.'
        dest = storage_utils.path('swift://tenant/container')
        utils.copy(source, dest, object_threads=30, segment_threads=40)
        mock_upload.assert_called_once_with(
            dest,
            ['.'],
            segment_size=1073741824,
            use_slo=True,
            object_threads=30,
            segment_threads=40)

    @mock.patch.object(SwiftPath, 'download', autospec=True)
    def test_swift_to_posix(self, mock_download):
        source = storage_utils.path('swift://tenant/container')
        dest = '.'
        utils.copy(source, dest, object_threads=30)
        mock_download.assert_called_once_with(
            source,
            output_dir=dest,
            remove_prefix=True,
            object_threads=30)


class TestPath(unittest.TestCase):
    def test_swift_returned(self):
        p = storage_utils.path('swift://my/swift/path')
        self.assertTrue(isinstance(p, SwiftPath))

    def test_posix_path_returned(self):
        p = storage_utils.path('my/posix/path')
        self.assertTrue(isinstance(p, Path))


class TestIsSwiftPath(unittest.TestCase):
    def test_true(self):
        self.assertTrue(storage_utils.is_swift_path('swift://my/swift/path'))

    def test_false(self):
        self.assertFalse(storage_utils.is_swift_path('my/posix/path'))


class TestIsPosixPath(unittest.TestCase):
    def test_true(self):
        self.assertTrue(storage_utils.is_posix_path('my/posix/path'))

    def test_false(self):
        self.assertFalse(storage_utils.is_posix_path('swift://my/swift/path'))


class TestWalkFilesAndDirs(unittest.TestCase):
    def test_w_dir(self):
        # Create an empty directory for this test in ./swift_upload. This
        # is because git doesnt allow a truly empty directory to be checked
        # in
        swift_dir = (
            storage_utils.path(__file__).expand().abspath().parent /
            'swift_upload'
        )
        with utils.NamedTemporaryDirectory(dir=swift_dir) as tmp_dir:
            uploads = utils.walk_files_and_dirs([swift_dir])
            self.assertEquals(set(uploads), set([
                swift_dir / 'file1',
                tmp_dir,
                swift_dir / 'data_dir' / 'file2',
            ]))

    def test_w_file(self):
        name = (
            storage_utils.path(__file__).expand().abspath().parent /
            'swift_upload' / 'file1'
        )

        uploads = utils.walk_files_and_dirs([name])
        self.assertEquals(set(uploads), set([name]))

    def test_w_invalid_file(self):
        name = (
            storage_utils.path(__file__).expand().abspath().parent /
            'swift_upload' / 'invalid'
        )

        with self.assertRaises(ValueError):
            utils.walk_files_and_dirs([name])


class TestNamedTemporaryDirectory(unittest.TestCase):
    def test_w_chdir(self):
        tmp_d = None
        with utils.NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.assertTrue(tmp_d.exists())
            p = storage_utils.path('.').expand().abspath()
            self.assertTrue(tmp_d in p)

        self.assertFalse(tmp_d.exists())

    def test_wo_chdir(self):
        tmp_d = None
        with utils.NamedTemporaryDirectory() as tmp_d:
            self.assertTrue(tmp_d.exists())

        self.assertFalse(tmp_d.exists())
