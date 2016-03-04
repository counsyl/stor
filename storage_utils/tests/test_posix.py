import mock
import os
import storage_utils
from storage_utils import NamedTemporaryDirectory
from storage_utils import path
from storage_utils import Path
from storage_utils import posix
from storage_utils import swift
from storage_utils import utils
from storage_utils import windows
import tempfile
import unittest


class TestDiv(unittest.TestCase):
    def test_success(self):
        p = posix.PosixPath('my/path') / 'other/path'
        self.assertEquals(p, posix.PosixPath('my/path/other/path'))
        self.assertEquals(p, storage_utils.join('my/path', 'other/path'))

    def test_rdiv(self):
        p = 'my/path' / posix.PosixPath('other/path')
        self.assertEquals(p, posix.PosixPath('my/path/other/path'))

    def test_w_windows_path(self):
        with self.assertRaisesRegexp(TypeError, 'unsupported operand'):
            posix.PosixPath('my/path') / windows.WindowsPath(r'other\path')

    def test_w_swift_component(self):
        p = posix.PosixPath('my/path') / swift.SwiftPath('swift://t/c/name').name
        self.assertEquals(p, posix.PosixPath('my/path/name'))
        self.assertEquals(storage_utils.join('my/path',
                                             swift.SwiftPath('swift://t/c/name').name),
                          p)


class TestAdd(unittest.TestCase):
    def test_success(self):
        p = posix.PosixPath('my/path') + 'other/path'
        self.assertEquals(p, posix.PosixPath('my/pathother/path'))

    def test_w_windows_path(self):
        with self.assertRaisesRegexp(TypeError, 'unsupported operand'):
            posix.PosixPath('my/path') + windows.WindowsPath(r'other\path')

    def test_w_swift_component(self):
        p = posix.PosixPath('my/path') + swift.SwiftPath('swift://t/c/name').name
        self.assertEquals(p, posix.PosixPath('my/pathname'))

    def test_invalid_radd(self):
        with self.assertRaisesRegexp(TypeError, 'unsupported operand'):
            1 + posix.PosixPath('my/path')


class TestCopy(unittest.TestCase):
    def test_posix_dir_destination(self):
        with storage_utils.NamedTemporaryDirectory() as tmp_d:
            source = tmp_d / 'source'
            os.mkdir(source)
            with open(source / '1', 'w') as tmp_file:
                tmp_file.write('1')

            dest = tmp_d / 'my' / 'dest'
            dest.makedirs_p()
            utils.copy(source / '1', dest)
            self.assertTrue((dest / '1').exists())
            self.assertEquals((dest / '1').open().read(), '1')

    def test_posix_file_destination(self):
        with storage_utils.NamedTemporaryDirectory() as tmp_d:
            source = tmp_d / 'source'
            os.mkdir(source)
            with open(source / '1', 'w') as tmp_file:
                tmp_file.write('1')

            dest = tmp_d / 'my' / 'dest' / '1'
            dest.parent.makedirs_p()
            utils.copy(source / '1', dest)
            self.assertTrue(dest.exists())
            self.assertEquals(dest.open().read(), '1')

    def test_ambigious_swift_resource_destination(self):
        with storage_utils.NamedTemporaryDirectory() as tmp_d:
            source = tmp_d / '1'
            with open(source, 'w') as tmp_file:
                tmp_file.write('1')

            dest = 'swift://tenant/container/ambiguous-resource'
            with self.assertRaisesRegexp(ValueError, 'swift destination'):
                utils.copy(source, dest)

    def test_ambigious_swift_container_destination(self):
        with storage_utils.NamedTemporaryDirectory() as tmp_d:
            source = tmp_d / '1'
            with open(source, 'w') as tmp_file:
                tmp_file.write('1')

            dest = 'swift://tenant/ambiguous-container'
            with self.assertRaisesRegexp(ValueError, 'swift destination'):
                utils.copy(source, dest)

    def test_tenant_swift_destination(self):
        with storage_utils.NamedTemporaryDirectory() as tmp_d:
            source = tmp_d / 'source'
            os.mkdir(source)
            with open(source / '1', 'w') as tmp_file:
                tmp_file.write('1')

            dest = 'swift://tenant/'
            with self.assertRaisesRegexp(ValueError, 'copy to tenant'):
                utils.copy(source / '1', dest)

    @mock.patch.object(swift.SwiftPath, 'upload', autospec=True)
    def test_swift_destination(self, mock_upload):
        dest = storage_utils.path('swift://tenant/container/file.txt')
        with tempfile.NamedTemporaryFile() as tmp_f:
            path(tmp_f.name).copy(dest)
            upload_args = mock_upload.call_args_list[0][0]
            self.assertEquals(upload_args[0], dest.parent)
            self.assertEquals(upload_args[1][0].source, tmp_f.name)
            self.assertEquals(upload_args[1][0].object_name, 'file.txt')


class TestCopytree(unittest.TestCase):
    def test_posix_destination(self):
        with storage_utils.NamedTemporaryDirectory() as tmp_d:
            source = tmp_d / 'source'
            os.mkdir(source)
            with open(source / '1', 'w') as tmp_file:
                tmp_file.write('1')

            dest = tmp_d / 'my' / 'dest'
            utils.copytree(source, dest)
            self.assertTrue((dest / '1').exists())

    def test_posix_destination_w_cmd(self):
        with storage_utils.NamedTemporaryDirectory() as tmp_d:
            source = tmp_d / 'source'
            os.mkdir(source)
            with open(source / '1', 'w') as tmp_file:
                tmp_file.write('1')

            dest = tmp_d / 'my' / 'dest'
            utils.copytree(source, dest, copy_cmd='cp -r')
            self.assertTrue((dest / '1').exists())

    def test_posix_destination_already_exists(self):
        with storage_utils.NamedTemporaryDirectory() as tmp_d:
            source = tmp_d / '1'
            source.makedirs_p()

            with self.assertRaisesRegexp(OSError, 'exists'):
                utils.copytree(source, tmp_d)

    def test_posix_destination_w_error(self):
        with storage_utils.NamedTemporaryDirectory() as tmp_d:
            invalid_source = tmp_d / 'source'
            dest = tmp_d / 'my' / 'dest'

            with self.assertRaises(OSError):
                utils.copytree(invalid_source, dest)

    @mock.patch.object(swift.SwiftPath, 'upload', autospec=True)
    def test_swift_destination(self, mock_upload):
        source = '.'
        dest = storage_utils.path('swift://tenant/container')
        utils.copytree(source, dest, swift_upload_options={
            'object_threads': 30,
            'segment_threads': 40
        })
        mock_upload.assert_called_once_with(
            dest,
            ['.'],
            object_threads=30,
            segment_threads=40)


class TestOpen(unittest.TestCase):
    def test_open_works_w_swift_params(self):
        with tempfile.NamedTemporaryFile() as f:
            p = storage_utils.path(f.name).open(swift_upload_kwargs={
                'use_slo': True
            })
            p.close()

    def test_functional_open(self):
        with tempfile.NamedTemporaryFile() as f:
            with storage_utils.open(f.name, 'wb', swift_upload_kwargs={}) as f:
                f.write('blah')

    def test_open_works_wo_swift_params(self):
        with tempfile.NamedTemporaryFile() as f:
            p = storage_utils.Path(f.name).open()
            p.close()

        with tempfile.NamedTemporaryFile() as f:
            p = storage_utils.open(f.name)
            p.close()


class TestMisc(unittest.TestCase):
    def test_repr(self):
        self.assertEqual(Path('/a/b'), "PosixPath('/a/b')")

    def test_mk_rm_dir(self):
        with NamedTemporaryDirectory(change_dir=True) as tmpdir:
            dirname = tmpdir / 'blah'
            dirname.mkdir()
            with self.assertRaises(OSError):
                dirname.mkdir()
            dirname.mkdir_p()
            dirname.rmdir()
            with self.assertRaises(OSError):
                dirname.rmdir()
            dirname.rmdir_p()
            dirname.mkdir_p()
            dirname.makedirs_p()
            with self.assertRaises(OSError):
                dirname.makedirs()
            assert dirname.exists()
            dirname.rmdir_p()

    def test_is_methods(self):
        with NamedTemporaryDirectory(change_dir=True) as tmpdir:
            dirname = tmpdir / 'blah'
            self.assertFalse(dirname.isdir())
            self.assertFalse(dirname.isfile())
            dirname.mkdir()
            self.assertTrue(dirname.isdir())
            self.assertFalse(dirname.isfile())
            filename = os.path.join(dirname, 'test.txt')
            with filename.open('wb') as fp:
                fp.write('somedata')
            self.assertFalse(filename.isdir())
            self.assertTrue(filename.isfile())
            self.assertFalse(filename.islink())
            self.assertTrue(filename.abspath().isabs())
            self.assertFalse(Path('.').isabs())
            self.assertEqual(dirname.listdir(), [filename])
