from stor import posix
from stor import s3
from stor import swift
from stor import windows
import unittest


class TestDiv(unittest.TestCase):
    def test_success(self):
        p = windows.WindowsPath(r'my\path') / r'other\path'
        self.assertEquals(p, windows.WindowsPath(r'my\path\other\path'))

    def test_w_posix_path(self):
        with self.assertRaisesRegexp(TypeError, 'unsupported operand'):
            windows.WindowsPath(r'my\path') / posix.PosixPath('other/path')

    def test_w_swift_component(self):
        with self.assertRaisesRegexp(TypeError, 'unsupported operand'):
            windows.WindowsPath(r'my\path') / swift.SwiftPath('swift://t/c/name').name

    def test_w_s3_component(self):
        with self.assertRaisesRegexp(TypeError, 'unsupported operand'):
            windows.WindowsPath(r'my\path') / s3.S3Path('s3://b/name').name


class TestAdd(unittest.TestCase):
    def test_success(self):
        p = windows.WindowsPath(r'my\path') + r'other\path'
        self.assertEquals(p, windows.WindowsPath(r'my\pathother\path'))

    def test_w_posix_path(self):
        with self.assertRaisesRegexp(TypeError, 'unsupported operand'):
            windows.WindowsPath(r'my\path') + posix.PosixPath('other/path')

    def test_w_swift_component(self):
        with self.assertRaisesRegexp(TypeError, 'unsupported operand'):
            windows.WindowsPath(r'my\path') + swift.SwiftPath('swift://t/c/name').name

    def test_w_s3_component(self):
        with self.assertRaisesRegexp(TypeError, 'unsupported operand'):
            windows.WindowsPath(r'my\path') + s3.S3Path('s3://b/name').name
