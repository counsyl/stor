from counsyl_io import os_path
from counsyl_io import utils
import unittest


class TestChdir(unittest.TestCase):
    def test_chdir(self):
        p = os_path.OSPath().absexpand()
        self.assertTrue(p.endswith('counsyl-io'))

        with utils.chdir(p / 'counsyl_io' / 'tests'):
            p = os_path.OSPath().absexpand()
            self.assertTrue(p.endswith('tests'))

        p = os_path.OSPath().absexpand()
        self.assertTrue(p.endswith('counsyl-io'))


class TestNamedTemporaryDirectory(unittest.TestCase):
    def test_wo_chdir(self):
        tmp_d = None
        with utils.NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.assertTrue(tmp_d.exists())
            p = os_path.OSPath().absexpand()
            self.assertTrue(tmp_d in p)

        self.assertFalse(tmp_d.exists())

    def test_w_chdir(self):
        tmp_d = None
        with utils.NamedTemporaryDirectory() as tmp_d:
            self.assertTrue(tmp_d.exists())

        self.assertFalse(tmp_d.exists())
