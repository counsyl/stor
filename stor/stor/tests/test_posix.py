import os
import tempfile
import unittest

import stor
from stor import NamedTemporaryDirectory
from stor import Path
from stor import posix
from stor import windows


class TestDiv(unittest.TestCase):
    def test_success(self):
        p = posix.PosixPath('my/path') / 'other/path'
        self.assertEquals(p, posix.PosixPath('my/path/other/path'))
        self.assertEquals(p, stor.join('my/path', 'other/path'))

    def test_rdiv(self):
        p = 'my/path' / posix.PosixPath('other/path')
        self.assertEquals(p, posix.PosixPath('my/path/other/path'))

    def test_w_windows_path(self):
        with self.assertRaisesRegexp(TypeError, 'unsupported operand'):
            posix.PosixPath('my/path') / windows.WindowsPath(r'other\path')


class TestAdd(unittest.TestCase):
    def test_success(self):
        p = posix.PosixPath('my/path') + 'other/path'
        self.assertEquals(p, posix.PosixPath('my/pathother/path'))

    def test_w_windows_path(self):
        with self.assertRaisesRegexp(TypeError, 'unsupported operand'):
            posix.PosixPath('my/path') + windows.WindowsPath(r'other\path')

    def test_invalid_radd(self):
        with self.assertRaisesRegexp(TypeError, 'unsupported operand'):
            1 + posix.PosixPath('my/path')


class TestCopy(unittest.TestCase):
    def test_posix_dir_destination(self):
        with stor.NamedTemporaryDirectory() as tmp_d:
            source = tmp_d / 'source'
            os.mkdir(source)
            with open(source / '1', 'w') as tmp_file:
                tmp_file.write('1')

            dest = tmp_d / 'my' / 'dest'
            dest.makedirs_p()
            stor.copy(source / '1', dest)
            self.assertTrue((dest / '1').exists())
            self.assertEquals((dest / '1').open().read(), '1')

    def test_posix_file_destination(self):
        with stor.NamedTemporaryDirectory() as tmp_d:
            source = tmp_d / 'source'
            os.mkdir(source)
            with open(source / '1', 'w') as tmp_file:
                tmp_file.write('1')

            dest = tmp_d / 'my' / 'dest' / '1'
            dest.parent.makedirs_p()
            stor.copy(source / '1', dest)
            self.assertTrue(dest.exists())
            self.assertEquals(dest.open().read(), '1')


class TestCopytree(unittest.TestCase):
    def test_posix_destination(self):
        with stor.NamedTemporaryDirectory() as tmp_d:
            source = tmp_d / 'source'
            os.mkdir(source)
            with open(source / '1', 'w') as tmp_file:
                tmp_file.write('1')

            dest = tmp_d / 'my' / 'dest'
            stor.copytree(source, dest)
            self.assertTrue((dest / '1').exists())

    def test_posix_destination_w_cmd(self):
        with stor.NamedTemporaryDirectory() as tmp_d:
            source = tmp_d / 'source'
            os.mkdir(source)
            with open(source / '1', 'w') as tmp_file:
                tmp_file.write('1')

            dest = tmp_d / 'my' / 'dest'
            stor.copytree(source, dest, copy_cmd='cp -r')
            self.assertTrue((dest / '1').exists())

    def test_posix_destination_already_exists(self):
        with stor.NamedTemporaryDirectory() as tmp_d:
            source = tmp_d / '1'
            source.makedirs_p()

            with self.assertRaisesRegexp(OSError, 'exists'):
                stor.copytree(source, tmp_d)

    def test_posix_destination_w_error(self):
        with stor.NamedTemporaryDirectory() as tmp_d:
            invalid_source = tmp_d / 'source'
            dest = tmp_d / 'my' / 'dest'

            with self.assertRaises(OSError):
                stor.copytree(invalid_source, dest)


class TestOpen(unittest.TestCase):
    def test_functional_open(self):
        with tempfile.NamedTemporaryFile() as f:
            with stor.open(f.name, 'w') as f:
                f.write('blah')

    def test_open_works(self):
        with tempfile.NamedTemporaryFile() as f:
            p = stor.Path(f.name).open()
            p.close()

        with tempfile.NamedTemporaryFile() as f:
            p = stor.open(f.name)
            p.close()


class TestGlob(unittest.TestCase):
    def test_glob(self):
        with NamedTemporaryDirectory(change_dir=True):
            open('file.txt', 'w').close()
            open('file2.txt', 'w').close()
            open('file3', 'w').close()

            files = stor.glob('.', '*.txt')
            self.assertEquals(set(files), set(['./file.txt', './file2.txt']))


class TestList(unittest.TestCase):
    def test_list(self):
        with NamedTemporaryDirectory(change_dir=True):
            open('file1.txt', 'w').close()
            os.mkdir('dir')
            os.mkdir('dir/dir2')
            open('dir/file2.txt', 'w').close()
            open('dir/dir2/file3', 'w').close()
            open('dir/dir2/file4.txt', 'w').close()

            files = Path('.').list()
            self.assertEquals(set(files), set(['./file1.txt',
                                               './dir/file2.txt',
                                               './dir/dir2/file3',
                                               './dir/dir2/file4.txt']))


class TestWalkfiles(unittest.TestCase):
    def test_walkfiles(self):
        with NamedTemporaryDirectory(change_dir=True):
            open('file1.txt', 'w').close()
            os.mkdir('dir')
            os.mkdir('dir/dir2')
            open('dir/file2.txt', 'w').close()
            open('dir/dir2/file3', 'w').close()
            open('dir/dir2/file4.txt', 'w').close()

            files = Path('.').walkfiles(pattern='*.txt')
            self.assertEquals(set(files), set(['./file1.txt',
                                               './dir/file2.txt',
                                               './dir/dir2/file4.txt']))


class TestMisc(unittest.TestCase):
    def test_repr(self):
        self.assertEqual(repr(Path('/a/b')), "PosixPath('/a/b')")

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
                fp.write(b'somedata')
            self.assertFalse(filename.isdir())
            self.assertTrue(filename.isfile())
            self.assertFalse(filename.islink())
            self.assertTrue(filename.abspath().isabs())
            self.assertFalse(Path('.').isabs())
            self.assertEqual(dirname.listdir(), [filename])

    def test_getsize(self):
        with tempfile.NamedTemporaryFile('w') as tmp:
            tmp.write('blah')
            tmp.flush()
            self.assertEqual(Path(tmp.name).getsize(), 4)