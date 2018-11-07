from stor import windows
from stor_s3 import s3
import unittest


class TestDiv(unittest.TestCase):
    def test_w_s3_component(self):
        with self.assertRaisesRegexp(TypeError, 'unsupported operand'):
            windows.WindowsPath(r'my\path') / s3.S3Path('s3://b/name').name


class TestAdd(unittest.TestCase):
    def test_w_s3_component(self):
        with self.assertRaisesRegexp(TypeError, 'unsupported operand'):
            windows.WindowsPath(r'my\path') + s3.S3Path('s3://b/name').name
