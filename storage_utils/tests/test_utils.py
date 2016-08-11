import errno
import logging
import mock
import ntpath
import os
import storage_utils
from storage_utils.posix import PosixPath
from storage_utils.experimental.s3 import S3Path
from storage_utils.swift import SwiftPath
from storage_utils.windows import WindowsPath
from storage_utils import utils
from testfixtures import LogCapture
import unittest


class TestBaseProgressLogger(unittest.TestCase):
    def test_empty_logger(self):
        class EmptyLogger(utils.BaseProgressLogger):
            def get_progress_message(self):
                return ''

        with LogCapture('') as progress_log:
            with EmptyLogger(logging.getLogger('')) as l:
                l.add_result({})
            progress_log.check()


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

    def test_s3_returned(self):
        p = storage_utils.Path('s3://my/s3/path')
        self.assertTrue(isinstance(p, S3Path))


class TestIsSwiftPath(unittest.TestCase):
    def test_true(self):
        self.assertTrue(storage_utils.is_swift_path('swift://my/swift/path'))

    def test_false(self):
        self.assertFalse(storage_utils.is_swift_path('my/posix/path'))
        self.assertFalse(storage_utils.is_swift_path('s3://my/s3/path'))


class TestIsFilesystemPath(unittest.TestCase):
    def test_true(self):
        self.assertTrue(storage_utils.is_filesystem_path('my/posix/path'))

    def test_false(self):
        self.assertFalse(storage_utils.is_filesystem_path('swift://my/swift/path'))
        self.assertFalse(storage_utils.is_filesystem_path('s3://my/s3/path'))


class TestIsS3Path(unittest.TestCase):
    def test_true(self):
        self.assertTrue(storage_utils.utils.is_s3_path('s3://my/s3/path'))

    def test_false(self):
        self.assertFalse(storage_utils.utils.is_s3_path('swift://my/swift/path'))
        self.assertFalse(storage_utils.utils.is_s3_path('my/posix/path'))


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

    def test_w_error(self):
        tmp_d = None
        with self.assertRaises(ValueError):
            with utils.NamedTemporaryDirectory() as tmp_d:
                self.assertTrue(tmp_d.exists())
                raise ValueError()

        self.assertFalse(tmp_d.exists())

    def test_w_error_chdir(self):
        tmp_d = None
        with self.assertRaises(ValueError):
            with utils.NamedTemporaryDirectory(change_dir=True) as tmp_d:
                self.assertTrue(tmp_d.exists())
                raise ValueError()

        self.assertFalse(tmp_d.exists())


class TestPathFunction(unittest.TestCase):
    def test_path_function_back_compat(self):
        pth = storage_utils.path('/blah')
        self.assertIsInstance(pth, storage_utils.Path)


class TestMakeDestDir(unittest.TestCase):
    def test_make_dest_dir_w_oserror(self):
        with utils.NamedTemporaryDirectory() as tmp_d:
            test_file = os.path.join(tmp_d, 'test_file')
            open(test_file, 'w').close()

            with self.assertRaises(OSError):
                utils.make_dest_dir(test_file)
            self.assertFalse(os.path.isdir(test_file))

    def test_make_dest_dir_w_enotdir_error(self):
        with utils.NamedTemporaryDirectory() as tmp_d:
            test_file = os.path.join(tmp_d, 'test_file')
            open(test_file, 'w').close()
            with self.assertRaises(OSError) as exc:
                new_dir = os.path.join(test_file, 'test')
                utils.make_dest_dir(new_dir)
            self.assertEquals(exc.exception.errno, errno.ENOTDIR)
            self.assertFalse(os.path.isdir(new_dir))

    def test_make_dest_dir_success(self):
        with utils.NamedTemporaryDirectory() as tmp_d:
            dest_dir = os.path.join(tmp_d, 'test')
            utils.make_dest_dir(dest_dir)
            self.assertTrue(os.path.isdir(dest_dir))

    def test_make_dest_dir_existing(self):
        with utils.NamedTemporaryDirectory() as tmp_d:
            dest_dir = os.path.join(tmp_d, 'test')
            utils.make_dest_dir(dest_dir)
            self.assertTrue(os.path.isdir(dest_dir))


class TestCondition(unittest.TestCase):
    def test_invalid_condition_type(self):
        with self.assertRaisesRegexp(ValueError, 'must be callable'):
            utils.validate_condition('bad_cond')

    def test_invalid_condition_args(self):
        with self.assertRaisesRegexp(ValueError, 'exactly one argument'):
            utils.validate_condition(lambda: True)  # pragma: no cover


class TestMisc(unittest.TestCase):
    def test_has_trailing_slash(self):
        self.assertFalse(utils.has_trailing_slash(''))


class TestFileNameToObjectName(unittest.TestCase):
    @mock.patch('os.path', ntpath)
    def test_abs_windows_path(self):
        self.assertEquals(utils.file_name_to_object_name(r'C:\windows\path\\'),
                          'windows/path')

    @mock.patch('os.path', ntpath)
    def test_rel_windows_path(self):
        self.assertEquals(utils.file_name_to_object_name(r'.\windows\path\\'),
                          'windows/path')

    def test_abs_path(self):
        self.assertEquals(utils.file_name_to_object_name('/abs/path/'),
                          'abs/path')

    def test_hidden_file(self):
        self.assertEquals(utils.file_name_to_object_name('.hidden'),
                          '.hidden')

    def test_hidden_dir(self):
        self.assertEquals(utils.file_name_to_object_name('.git/file'),
                          '.git/file')

    def test_no_obj_name(self):
        self.assertEquals(utils.file_name_to_object_name('.'),
                          '')

    def test_poorly_formatted_path(self):
        self.assertEquals(utils.file_name_to_object_name('.//poor//path//file'),
                          'poor/path/file')

    @mock.patch.dict(os.environ, {'HOME': '/home/wes/'})
    def test_path_w_env_var(self):
        self.assertEquals(utils.file_name_to_object_name('$HOME/path//file'),
                          'home/wes/path/file')
