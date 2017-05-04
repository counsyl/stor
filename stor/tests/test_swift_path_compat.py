import unittest

from stor.swift import SwiftPath


class TestBasics(unittest.TestCase):
    def test_relpath(self):
        with self.assertRaises(AttributeError):
            SwiftPath('swift://tenant').relpathto()
        with self.assertRaises(AttributeError):
            SwiftPath('swift://tenant').relpath()

    def test_construction_from_none(self):
        with self.assertRaises(ValueError):
            SwiftPath(None)

    def test_string_compatibility(self):
        """ Test compatibility with ordinary strings. """
        x = SwiftPath('swift://xyzzy')
        assert x == 'swift://xyzzy'
        assert x == str('swift://xyzzy')
        assert 'xyz' in x
        assert 'analysis' not in x

        # sorting
        items = [SwiftPath('swift://fhj'),
                 SwiftPath('swift://fgh'),
                 'swift://E',
                 SwiftPath('swift://d'),
                 'swift://A',
                 SwiftPath('swift://B'),
                 'swift://c']
        items.sort()
        self.assertEqual(items,
                         ['swift://A', 'swift://B', 'swift://E', 'swift://c',
                          'swift://d', 'swift://fgh', 'swift://fhj'])

        # Test p1/p1.
        p1 = SwiftPath("swift://foo")
        p2 = "bar"
        self.assertEqual(p1 / p2, SwiftPath("swift://foo/bar"))

    def test_properties(self):
        # Create sample path object.
        f = SwiftPath('swift://tenant/container/whatever.csv')

        self.assertEqual(f.parent, SwiftPath('swift://tenant/container'))

        # .name
        self.assertEqual(f.name, 'whatever.csv')
        self.assertEqual(f.parent.name, 'container')
        self.assertEqual(f.parent.parent.name, 'tenant')

        # .ext
        self.assertEqual(f.ext, '.csv')
        self.assertEqual(f.parent.ext, '')
