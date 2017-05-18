import errno
import logging
import mock
import ntpath
import os
import stat
import unittest

from testfixtures import LogCapture

import stor
from stor import Path
from stor.posix import PosixPath
from stor.s3 import S3Path
from stor.swift import SwiftPath
from stor.windows import WindowsPath
from stor import utils


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
        p = Path('swift://my/swift/path')
        self.assertTrue(isinstance(p, SwiftPath))

    def test_posix_path_returned(self):
        p = Path('my/posix/path')
        self.assertTrue(isinstance(p, PosixPath))

    @mock.patch('os.path', ntpath)
    def test_abs_windows_path_returned(self):
        p = Path('C:\\my\\windows\\path')
        self.assertTrue(isinstance(p, WindowsPath))

    def test_s3_returned(self):
        p = stor.Path('s3://my/s3/path')
        self.assertTrue(isinstance(p, S3Path))


class TestIsSwiftPath(unittest.TestCase):
    def test_true(self):
        self.assertTrue(stor.is_swift_path('swift://my/swift/path'))

    def test_false(self):
        self.assertFalse(stor.is_swift_path('my/posix/path'))
        self.assertFalse(stor.is_swift_path('s3://my/s3/path'))


class TestIsFilesystemPath(unittest.TestCase):
    def test_true(self):
        self.assertTrue(stor.is_filesystem_path('my/posix/path'))

    def test_false(self):
        self.assertFalse(stor.is_filesystem_path('swift://my/swift/path'))
        self.assertFalse(stor.is_filesystem_path('s3://my/s3/path'))


class TestIsS3Path(unittest.TestCase):
    def test_true(self):
        self.assertTrue(stor.utils.is_s3_path('s3://my/s3/path'))

    def test_false(self):
        self.assertFalse(stor.utils.is_s3_path('swift://my/swift/path'))
        self.assertFalse(stor.utils.is_s3_path('my/posix/path'))


class TestWalkFilesAndDirs(unittest.TestCase):
    swift_dir = (
        Path(__file__).expand().abspath().parent /
        'swift_upload'
    )

    def test_w_dir(self):
        # Create an empty directory for this test in ./swift_upload. This
        # is because git doesnt allow a truly empty directory to be checked
        # in
        with utils.NamedTemporaryDirectory(dir=self.swift_dir) as tmp_dir:
            uploads = utils.walk_files_and_dirs([self.swift_dir])
            self.assertEquals(set(uploads), set([
                self.swift_dir / 'file1',
                tmp_dir,
                self.swift_dir / 'data_dir' / 'file2',
            ]))

    def test_w_file(self):
        name = self.swift_dir / 'file1'

        uploads = utils.walk_files_and_dirs([name])
        self.assertEquals(set(uploads), set([name]))

    def test_w_missing_file(self):
        name = self.swift_dir / 'invalid'

        with self.assertRaises(ValueError):
            utils.walk_files_and_dirs([name])

    def test_w_broken_symlink(self):
        swift_dir = (
            Path(__file__).expand().abspath().parent /
            'swift_upload'
        )
        with utils.NamedTemporaryDirectory(dir=swift_dir) as tmp_dir:
            symlink = tmp_dir / 'broken.symlink'
            symlink_source = tmp_dir / 'nonexistent'
            # put something in symlink source so that Python doesn't complain
            with stor.open(symlink_source, 'w') as fp:
                fp.write('blah')
            os.symlink(symlink_source, symlink)
            uploads = utils.walk_files_and_dirs([swift_dir])
            self.assertEquals(set(uploads), set([
                swift_dir / 'file1',
                swift_dir / 'data_dir' / 'file2',
                symlink,
                symlink_source,
            ]))
            # NOW: destroy it with fire and we have empty directory
            os.remove(symlink_source)
            uploads = utils.walk_files_and_dirs([swift_dir])
            self.assertEquals(set(uploads), set([
                swift_dir / 'file1',
                tmp_dir,
                swift_dir / 'data_dir' / 'file2',
            ]))
            # but put a sub directory, now tmp_dir doesn't show up
            subdir = tmp_dir / 'subdir'
            subdir.makedirs_p()
            uploads = utils.walk_files_and_dirs([swift_dir])
            self.assertEquals(set(uploads), set([
                swift_dir / 'file1',
                subdir,
                swift_dir / 'data_dir' / 'file2',
            ]))

    def test_broken_symlink_file_name_truncation(self):
        with stor.NamedTemporaryDirectory(dir=self.swift_dir) as tmp_dir:
            # finally make enough symlinks to trigger log trimming (currently
            # untested but we want to make sure we don't error) + hits some
            # issues if path to the test file is so long that even 1 file is >
            # 50 characters, so we skip coverage check because it's not
            # worth the hassle to get full branch coverage for big edge case
            super_long_name = tmp_dir / 'abcdefghijklmnopqrstuvwxyzrsjexcasdfawefawefawefawefaef'
            for x in range(5):
                os.symlink(super_long_name, super_long_name + str(x))
            # have no errors! yay!
            self.assert_(utils.walk_files_and_dirs([self.swift_dir]))
            for x in range(5, 20):
                os.symlink(super_long_name, super_long_name + str(x))
            # have no errors! yay!
            self.assert_(utils.walk_files_and_dirs([self.swift_dir]))


class TestNamedTemporaryDirectory(unittest.TestCase):
    def test_w_chdir(self):
        tmp_d = None
        with utils.NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.assertTrue(tmp_d.exists())
            p = Path('.').expand().abspath()
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
        pth = Path('/blah')
        self.assertIsInstance(pth, stor.Path)


class TestMakeDestDir(unittest.TestCase):
    def test_make_dest_dir_w_oserror(self):
        with utils.NamedTemporaryDirectory() as tmp_d:
            test_file = os.path.join(tmp_d, 'test_file')
            open(test_file, 'w').close()

            with self.assertRaisesRegexp(OSError, 'File exists'):
                utils.make_dest_dir(test_file)
            self.assertFalse(os.path.isdir(test_file))

    def test_make_dest_dir_w_enotdir_error(self):
        with utils.NamedTemporaryDirectory() as tmp_d:
            test_file = os.path.join(tmp_d, 'test_file')
            open(test_file, 'w').close()
            with self.assertRaisesRegexp(OSError, 'already exists as a file') as exc:
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
            os.mkdir(dest_dir)
            utils.make_dest_dir(dest_dir)
            self.assertTrue(os.path.isdir(dest_dir))


class TestCondition(unittest.TestCase):
    def test_invalid_condition_type(self):
        with self.assertRaisesRegexp(ValueError, 'must be callable'):
            utils.validate_condition('bad_cond')

    def test_invalid_condition_args(self):
        with self.assertRaisesRegexp(ValueError, 'exactly one argument'):
            utils.validate_condition(lambda: True)  # pragma: no cover


class TestSizeConversion(unittest.TestCase):
    def test_str_to_bytes_int(self):
        self.assertEquals(5, utils.str_to_bytes(5))

    def test_str_to_bytes_invalid_str_short(self):
        with self.assertRaises(ValueError):
            utils.str_to_bytes('M')

    def test_str_to_bytes_invalid_str_long(self):
        with self.assertRaises(ValueError):
            utils.str_to_bytes('wrongM')

    def test_str_to_bytes_invalid_units(self):
        with self.assertRaises(ValueError):
            utils.str_to_bytes('10L')


class TestMisc(unittest.TestCase):
    def test_has_trailing_slash(self):
        self.assertFalse(utils.has_trailing_slash(''))

    def test_has_trailing_slash_none(self):
        self.assertFalse(utils.has_trailing_slash(None))

    def test_has_trailing_slash_true(self):
        self.assertTrue(utils.has_trailing_slash('has/slash/'))

    def test_has_trailing_slash_false(self):
        self.assertFalse(utils.has_trailing_slash('no/slash'))

    def test_remove_trailing_slash_none(self):
        self.assertIsNone(utils.remove_trailing_slash(None))

    def test_remove_trailing_slash_wo_slash(self):
        self.assertEquals(utils.remove_trailing_slash('no/slash'), 'no/slash')

    def test_remove_trailing_slash_multiple(self):
        self.assertEquals(utils.remove_trailing_slash('many/slashes//'), 'many/slashes')


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


class TestIsWriteablePOSIX(unittest.TestCase):
    def test_existing_path(self):
        with utils.NamedTemporaryDirectory() as tmp_d:
            self.assertTrue(utils.is_writeable(tmp_d))

    def test_non_existing_path(self):
        with utils.NamedTemporaryDirectory() as tmp_d:
            non_existing_path = tmp_d / 'does' / 'not' / 'exist'
            self.assertFalse(utils.is_writeable(non_existing_path))

    def test_path_unchanged(self):
        with utils.NamedTemporaryDirectory() as tmp_d:
            non_existing_path = tmp_d / 'does' / 'not' / 'exist'
            self.assertFalse(non_existing_path.exists())
            utils.is_writeable(non_existing_path)
            self.assertFalse(non_existing_path.exists())
            self.assertFalse((tmp_d / 'does').exists())

    def test_existing_path_not_removed(self):
        with utils.NamedTemporaryDirectory() as tmp_d:
            self.assertTrue(tmp_d.exists())
            utils.is_writeable(tmp_d)
            self.assertTrue(tmp_d.exists())

    def test_path_no_perms(self):
        with utils.NamedTemporaryDirectory() as tmp_d:
            os.chmod(tmp_d, stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH)
            self.assertFalse(utils.is_writeable(tmp_d))


@mock.patch('stor.swift.SwiftPath.exists')
@mock.patch('stor.utils.copy')
@mock.patch('stor.rmtree')
@mock.patch('stor.remove')
@mock.patch('stor.swift.SwiftPath.listdir', mock.Mock(return_value=[]))
@mock.patch('stor.utils.tempfile.NamedTemporaryFile')
class TestIsWriteableSwift(unittest.TestCase):
    def test_existing_path(self, tmpfile, mock_remove, _rm, _cp, mock_exists):
        filename = 'test-file'
        tmpfile.return_value.__enter__.return_value.name = filename
        mock_exists.return_value = True
        path = SwiftPath('swift://AUTH_stor_test/container/test/')
        self.assertTrue(utils.is_writeable(path))
        mock_remove.assert_called_with(path / filename)

    def test_non_existing_path(self, _tmp, _remove, _rm, _cp, mock_exists):
        mock_exists.return_value = False
        self.assertTrue(utils.is_writeable('swift://AUTH_stor_test/container/test/'))

    def test_path_unchanged(self, _tmp, _remove, mock_rmtree, _cp, mock_exists):
        # Make the first call to exists() return False and the second return True.
        mock_exists.side_effect = [False, True]
        utils.is_writeable('swift://AUTH_stor_test/container/test/')
        mock_rmtree.assert_called_once_with(SwiftPath('swift://AUTH_stor_test/container/'))

    def test_existing_path_not_removed(self, _tmp, _remove, mock_rmtree, _, mock_exists):
        mock_exists.return_value = True
        utils.is_writeable('swift://AUTH_stor_test/container/test/')
        mock_rmtree.assert_not_called()

    def test_path_no_perms(self, _tmp, _remove, _rm, mock_copy, _ex):
        mock_copy.side_effect = stor.swift.UnauthorizedError('foo')
        self.assertFalse(utils.is_writeable('swift://AUTH_stor_test/container/test/'))

    def test_disable_backoff(self, tmpfile, _remove, _rm, mock_copy, _ex):
        filename = 'test_file'
        path = Path('swift://AUTH_stor_test/container/test/')
        swift_opts = {'num_retries': 0}
        tmpfile.return_value.__enter__.return_value.name = filename
        utils.is_writeable(path, swift_opts)
        mock_copy.assert_called_with(filename, path, swift_retry_options=swift_opts)

    def test_no_trailing_slash(self, tmpfile, _remove, _rm, mock_copy, _ex):
        filename = 'test_file'
        path = SwiftPath('swift://AUTH_stor_test/container')
        tmpfile.return_value.__enter__.return_value.name = filename
        utils.is_writeable(path)  # no trailing slash
        mock_copy.assert_called_with(
            filename,
            utils.with_trailing_slash(path),
            swift_retry_options=None
        )


@mock.patch('stor.utils.copy')
@mock.patch('stor.rmtree')
@mock.patch('stor.remove')
@mock.patch('stor.utils.tempfile.NamedTemporaryFile')
class TestIsWriteableS3(unittest.TestCase):
    def test_success(self, tmpfile, mock_remove, _rmtree, mock_copy):
        filename = 'test-file'
        tmpfile.return_value.__enter__.return_value.name = filename
        path = 's3://stor-test/foo/bar'
        self.assertTrue(utils.is_writeable(path))
        mock_remove.assert_called_with(S3Path('{}/{}'.format(path, filename)))

    def test_existing_path_not_removed(self, _tmp, _remove, mock_rmtree, mock_copy):
        utils.is_writeable('s3://stor-test/foo/bar')
        mock_rmtree.assert_not_called()

    def test_path_no_perms(self, _tmp, mock_remove, mock_rmtree, mock_copy):
        mock_copy.side_effect = stor.exceptions.FailedUploadError('foo')
        self.assertFalse(utils.is_writeable('s3://stor-test/foo/bar'))
        mock_remove.assert_not_called()
