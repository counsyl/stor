from storage_utils import posix
from storage_utils import swift
from storage_utils import windows
import unittest


class TestDiv(unittest.TestCase):
    def test_success(self):
        p = windows.WindowsPath(r'my\path') / r'other\path'
        self.assertEquals(p, windows.WindowsPath(r'my\path\other\path'))

    def test_w_posix_path(self):
        with self.assertRaisesRegexp(ValueError, 'cannot join paths'):
            windows.WindowsPath(r'my\path') / posix.PosixPath('other/path')

    def test_w_swift_component(self):
        with self.assertRaisesRegexp(ValueError, 'cannot join paths'):
            windows.WindowsPath(r'my\path') / swift.SwiftPath('swift://t/c/name').name
