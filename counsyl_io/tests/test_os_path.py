from counsyl_io.os_path import OSPath
import os
import unittest


class TestNew(unittest.TestCase):
    def test_no_args(self):
        # os.curdir should be used for the path if no args are provided
        p = OSPath()
        self.assertEquals(str(p), os.curdir)

    def test_invalid_args(self):
        with self.assertRaises(ValueError):
            OSPath([])

    def test_multiple_args(self):
        p = OSPath('my', 'path')
        self.assertEquals(str(p), 'my/path')


class TestAbsexpand(unittest.TestCase):
    def test_absexpand(self):
        p = OSPath().absexpand()
        self.assertTrue(p.endswith('counsyl-io'))
