import unittest

from stor.dx import DXPath


class TestBasics(unittest.TestCase):
    def test_relpath(self):
        with self.assertRaises(AttributeError):
            DXPath('dx://project').relpathto()
        with self.assertRaises(AttributeError):
            DXPath('dx://project').relpath()

    def test_construction_from_none(self):
        with self.assertRaises(ValueError):
            DXPath(None)

    def test_string_compatibility(self):
        """ Test compatibility with ordinary strings. """
        x = DXPath('dx://xyzzy')
        assert x == 'dx://xyzzy'
        assert x == str('dx://xyzzy')
        assert 'xyz' in x
        assert 'analysis' not in x

        # sorting
        items = [DXPath('dx://fhj'),
                 DXPath('dx://fgh'),
                 'dx://E',
                 DXPath('dx://d'),
                 'dx://A',
                 DXPath('dx://B'),
                 'dx://c']
        items.sort()
        self.assertEqual(items,
                         ['dx://A', 'dx://B', 'dx://E', 'dx://c',
                          'dx://d', 'dx://fgh', 'dx://fhj'])

        # Test p1/p1.
        p1 = DXPath("dx://foo")
        p2 = "bar"
        self.assertEqual(p1 / p2, DXPath("dx://foo/bar"))

    def test_properties(self):
        # Create sample path object.
        f = DXPath('dx://project/prefix/whatever.csv')

        self.assertEqual(f.parent, DXPath('dx://project/prefix'))

        # .name
        self.assertEqual(f.name, 'whatever.csv')
        self.assertEqual(f.parent.name, 'prefix')
        self.assertEqual(f.parent.parent.name, 'project')

        # .ext
        self.assertEqual(f.ext, '.csv')
        self.assertEqual(f.parent.ext, '')
