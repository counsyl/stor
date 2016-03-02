import mock
import ntpath
import storage_utils
from storage_utils.posix import PosixPath
from storage_utils.swift import SwiftPath
from storage_utils.windows import WindowsPath
from storage_utils import utils
import unittest


class TestPath(unittest.TestCase):
    def test_swift_returned(self):
        p = storage_utils.path('swift://my/swift/path')
        self.assertTrue(isinstance(p, SwiftPath))

    def test_posix_path_returned(self):
        p = storage_utils.path('my/posix/path')
        self.assertTrue(isinstance(p, PosixPath))

    @mock.patch('os.path', ntpath)
    def test_abs_windows_path_returned(self):
        p = storage_utils.path('C:\\my\\windows\\path')
        self.assertTrue(isinstance(p, WindowsPath))


class TestIsSwiftPath(unittest.TestCase):
    def test_true(self):
        self.assertTrue(storage_utils.is_swift_path('swift://my/swift/path'))

    def test_false(self):
        self.assertFalse(storage_utils.is_swift_path('my/posix/path'))


class TestIsFilesystemPath(unittest.TestCase):
    def test_is_posix_path(self):
        self.assertEquals(storage_utils.is_posix_path, storage_utils.is_filesystem_path)

    def test_true(self):
        self.assertTrue(storage_utils.is_filesystem_path('my/posix/path'))

    def test_false(self):
        self.assertFalse(storage_utils.is_filesystem_path('swift://my/swift/path'))


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
