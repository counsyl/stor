from stor import windows
from stor_swift import swift
import unittest


class TestDiv(unittest.TestCase):
    def test_w_swift_component(self):
        with self.assertRaisesRegexp(TypeError, 'unsupported operand'):
            windows.WindowsPath(r'my\path') / swift.SwiftPath('swift://t/c/name').name


class TestAdd(unittest.TestCase):
    def test_w_swift_component(self):
        with self.assertRaisesRegexp(TypeError, 'unsupported operand'):
            windows.WindowsPath(r'my\path') + swift.SwiftPath('swift://t/c/name').name
