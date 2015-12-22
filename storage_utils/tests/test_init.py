from path import Path
import storage_utils
from storage_utils.swift import SwiftPath
import unittest


class TestPath(unittest.TestCase):
    def test_swift_returned(self):
        p = storage_utils.path('swift://my/swift/path')
        self.assertTrue(isinstance(p, SwiftPath))

    def test_posix_path_returned(self):
        p = storage_utils.path('my/posix/path')
        self.assertTrue(isinstance(p, Path))


class TestIsSwiftPath(unittest.TestCase):
    def test_true(self):
        self.assertTrue(storage_utils.is_swift_path('swift://my/swift/path'))

    def test_false(self):
        self.assertFalse(storage_utils.is_swift_path('my/posix/path'))


class TestIsPosixPath(unittest.TestCase):
    def test_true(self):
        self.assertTrue(storage_utils.is_posix_path('my/posix/path'))

    def test_false(self):
        self.assertFalse(storage_utils.is_posix_path('swift://my/swift/path'))
