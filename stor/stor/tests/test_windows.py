from stor import posix
from stor import windows
import unittest


class TestDiv(unittest.TestCase):
    def test_success(self):
        p = windows.WindowsPath(r'my\path') / r'other\path'
        self.assertEquals(p, windows.WindowsPath(r'my\path\other\path'))

    def test_w_posix_path(self):
        with self.assertRaisesRegexp(TypeError, 'unsupported operand'):
            windows.WindowsPath(r'my\path') / posix.PosixPath('other/path')


class TestAdd(unittest.TestCase):
    def test_success(self):
        p = windows.WindowsPath(r'my\path') + r'other\path'
        self.assertEquals(p, windows.WindowsPath(r'my\pathother\path'))

    def test_w_posix_path(self):
        with self.assertRaisesRegexp(TypeError, 'unsupported operand'):
            windows.WindowsPath(r'my\path') + posix.PosixPath('other/path')
