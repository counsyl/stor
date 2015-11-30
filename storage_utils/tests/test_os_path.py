from storage_utils import path
from storage_utils.os_path import OSPath
import os
import unittest


class TestFactory(unittest.TestCase):
    def test_os_path_returned(self):
        # Verify the factory behaves as expected with os paths
        p = path('my/os/path')
        self.assertTrue(isinstance(p, OSPath))


class TestAbsexpand(unittest.TestCase):
    def test_absexpand(self):
        p = OSPath().absexpand()
        project_name = os.path.basename(os.getcwd())
        self.assertTrue(p.endswith(project_name))
