import pytest
import unittest

from stor_dx.dx import DXPath, DXCanonicalPath, DXVirtualPath


class TestBasics(unittest.TestCase):
    def test_relpath(self):
        with self.assertRaises(AttributeError):
            DXPath('dx://project:').relpathto()
        with self.assertRaises(AttributeError):
            DXPath('dx://project:').relpath()

    def test_construction_from_none(self):
        with self.assertRaises(TypeError):
            DXPath(None)

    def test_construction_from_no_project(self):
        with pytest.raises(ValueError, match='Project is required to construct a DXPath'):
            DXPath('dx://')

    def test_canonical_construct_fail(self):
        with pytest.raises(ValueError, match='ambiguous'):
            DXPath('dx://project-123456789012345678901234:/file-123456789012345678901234/a/')

    def test_canonical_construct_wo_file(self):
        for path_str in [
            'dx://project-123456789012345678901234:/',
            'dx://project-123456789012345678901234:',
            'dx://project-123456789012345678901234'
        ]:
            p = DXPath(path_str)
            self.assertIsInstance(p, DXCanonicalPath, 'Expected canonical DXPath for (%s)' % p)
            self.assertEqual(p.project, 'project-123456789012345678901234',
                             'Project parsing unsuccessful for %s' % p)

    def test_canonical_construct_w_file(self):
        for path_str in [
            'dx://project-123456789012345678901234:file-123456789012345678901234',
            'dx://project-123456789012345678901234:/file-123456789012345678901234'
        ]:
            p = DXPath(path_str)
            self.assertIsInstance(p, DXCanonicalPath, 'Expected canonical DXPath for (%s)' % p)
            self.assertEqual(p.project, 'project-123456789012345678901234',
                             'Project parsing unsuccessful for %s' % p)
            self.assertEqual(p.resource, 'file-123456789012345678901234',
                             'Resource parsing error for %s' % p)

    def test_virtual_construct_wo_resource(self):
        for path_str in [
            'dx://proj123:/',
            'dx://proj123:',
            'dx://proj123'
        ]:
            p = DXPath(path_str)
            self.assertIsInstance(p, DXVirtualPath, 'Expected virtual DXPath for (%s)' % p)
            self.assertEqual(p.project, 'proj123',
                             'Project parsing unsuccessful for %s' % p)

    def test_virtual_construct_wo_folder(self):
        for path_str in [
            'dx://proj123:/a.ext',
            'dx://proj123:a.ext'
        ]:
            p = DXPath(path_str)
            self.assertIsInstance(p, DXVirtualPath, 'Expected virtual DXPath for (%s)' % p)
            self.assertEqual(p.project, 'proj123',
                             'Project parsing unsuccessful for %s' % p)
            self.assertEqual(str(p.resource), 'a.ext',
                             'Resource parsing error for %s' % p)

        for path_str in [
            'dx://project-123456789012345678901234:/a.ext',
            'dx://project-123456789012345678901234:a.ext'
        ]:
            p = DXPath(path_str)
            self.assertIsInstance(p, DXVirtualPath, 'Expected virtual DXPath for (%s)' % p)
            self.assertEqual(p.project, 'project-123456789012345678901234',
                             'Project parsing unsuccessful for %s' % p)
            self.assertEqual(p.resource, 'a.ext',
                             'Resource parsing error for %s' % p)

    def test_virtual_construct_w_folder(self):
        for path_str in [
            'dx://proj123:/b/c/a.ext',
            'dx://proj123:b/c/a.ext'
        ]:
            p = DXPath(path_str)
            self.assertIsInstance(p, DXVirtualPath, 'Expected virtual DXPath for (%s)' % p)
            self.assertEqual(p.project, 'proj123',
                             'Project parsing unsuccessful for %s' % p)
            self.assertEqual(p.resource, 'b/c/a.ext',
                             'Resource parsing error for %s' % p)

        for path_str in [
            'dx://project-123456789012345678901234:/b/c/a.ext',
            'dx://project-123456789012345678901234:b/c/a.ext'
        ]:
            p = DXPath(path_str)
            self.assertIsInstance(p, DXVirtualPath, 'Expected virtual DXPath for (%s)' % p)
            self.assertEqual(p.project, 'project-123456789012345678901234',
                             'Project parsing unsuccessful for %s' % p)
            self.assertEqual(p.resource, 'b/c/a.ext',
                             'Resource parsing error for %s' % p)

    def test_string_compatibility(self):
        """ Test compatibility with ordinary strings. """
        x = DXPath('dx://xyzzy:')
        assert x == 'dx://xyzzy:'
        assert x == str('dx://xyzzy:')
        assert 'xyz' in x
        assert 'analysis' not in x

        # sorting
        items = [DXPath('dx://fhj:'),
                 DXPath('dx://fgh:'),
                 'dx://E:',
                 DXPath('dx://d:'),
                 'dx://A:',
                 DXPath('dx://B:'),
                 'dx://c:']
        items.sort()
        self.assertEqual(items,
                         ['dx://A:', 'dx://B:', 'dx://E:', 'dx://c:',
                          'dx://d:', 'dx://fgh:', 'dx://fhj:'])

        # Test p1/p1.
        p1 = DXPath("dx://foo:")
        p2 = "bar"
        self.assertEqual(p1 / p2, DXPath("dx://foo:/bar"))

    def test_properties(self):
        # Create sample DXPath object.
        f = DXPath('dx://project:/prefix/whatever.csv')

        self.assertEqual(f.parent, DXPath('dx://project:/prefix'))

        # .name
        self.assertEqual(f.name, 'whatever.csv')
        self.assertEqual(f.parent.name, 'prefix')
        self.assertEqual(f.parent.parent.name, 'project:')

        # .ext
        self.assertEqual(f.ext, '.csv')
        self.assertEqual(f.parent.ext, '')
