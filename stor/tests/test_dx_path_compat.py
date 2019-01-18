import pytest
import unittest

from stor.dx import DXPath, DXCanonicalPath, DXVirtualPath
import stor


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

    def test_joinpath_no_resource(self):
        f = DXPath('dx://project')
        assert f.joinpath("dir/dir2", "file") == DXPath('dx://project:/dir/dir2/file')
        assert f.joinpath("dir/file") == DXPath('dx://project:/dir/file')
        assert f.joinpath("dir") == DXPath('dx://project:/dir')
        assert f.joinpath("/dir") == stor.Path('/dir')

        f = DXPath('dx://project-123456789012345678901234')
        assert f.joinpath("dir", "file") == DXPath(
            'dx://project-123456789012345678901234:/dir/file')
        assert f.joinpath("dir/file") == DXPath('dx://project-123456789012345678901234:/dir/file')
        assert f.joinpath("file-123456789012345678901234") == DXPath(
            'dx://project-123456789012345678901234:/file-123456789012345678901234')
        with pytest.raises(ValueError, match='ambiguous'):
            f.joinpath('file-123456789012345678901234', 'asd')

    def test_joinpath_w_resource(self):
        f = DXPath('dx://project:/dir')
        assert f.joinpath("file") == DXPath('dx://project:/dir/file')
        assert f.joinpath("/dir") == stor.Path('/dir')

        f = DXPath('dx://project-123456789012345678901234:dir/dir2')
        assert f.joinpath("file") == DXPath('dx://project-123456789012345678901234:/dir/dir2/file')

        f = DXPath('dx://project-123456789012345678901234:/file-123456789012345678901234')
        with pytest.raises(ValueError, match='ambiguous'):
            f.joinpath('asd')

    def test_joinpath_to_nothing(self):
        f = DXPath('dx://project:/prefix')
        assert f.joinpath() == f
        f = DXPath('dx://project:')
        assert f.joinpath() == DXPath('dx://project:/')
        f = DXPath('dx://project')
        assert f.joinpath() == DXPath('dx://project:/')
        f = DXPath('dx://project-123456789012345678901234')
        assert f.joinpath() == DXPath('dx://project-123456789012345678901234:')
        f = DXPath('dx://project-123456789012345678901234:/file-123456789012345678901234')
        assert f.joinpath() == DXPath(
            'dx://project-123456789012345678901234:file-123456789012345678901234')

    def test_splitpath(self):
        f = DXPath('dx://project:prefix/dir/file')
        assert f.splitpath() == (DXPath("dx://project:/prefix/dir"), 'file')
        f = DXPath('dx://project:/prefix/file')
        assert f.splitpath() == (DXPath("dx://project:/prefix"), 'file')
        f = DXPath('dx://project:/prefix')
        assert f.splitpath() == (DXPath("dx://project:"), 'prefix')
        f = DXPath('dx://project:/')
        assert f.splitpath() == (DXPath("dx://project:"), '')
        f = DXPath('dx://project:')
        assert f.splitpath() == (DXPath("dx://project:"), '')
        f = DXPath('dx://project')
        assert f.splitpath() == (DXPath("dx://project:"), '')

        f = DXPath('dx://project-123456789012345678901234:/file-123456789012345678901234')
        assert f.splitpath() == (DXPath("dx://project-123456789012345678901234:"),
                                 'file-123456789012345678901234')
        f = DXPath('dx://project-123456789012345678901234:file-123456789012345678901234')
        assert f.splitpath() == (DXPath("dx://project-123456789012345678901234:"),
                                 'file-123456789012345678901234')
        f = DXPath('dx://project-123456789012345678901234:/prefix/file')
        assert f.splitpath() == (DXPath("dx://project-123456789012345678901234:/prefix"), 'file')
        f = DXPath('dx://project-123456789012345678901234:/prefix')
        assert f.splitpath() == (DXPath("dx://project-123456789012345678901234:"), 'prefix')
        # This won't resolve to a canonical project:
        f = DXPath('dx://project-123456789012345678901234:prefix')
        assert f.splitpath() == (DXPath("dx://project-123456789012345678901234:"), 'prefix')
        f = DXPath('dx://project-123456789012345678901234:/')
        assert f.splitpath() == (DXCanonicalPath("dx://project-123456789012345678901234:"), '')
        f = DXPath('dx://project-123456789012345678901234:')
        assert f.splitpath() == (DXCanonicalPath("dx://project-123456789012345678901234:"), '')
        f = DXPath('dx://project-123456789012345678901234')
        assert f.splitpath() == (DXCanonicalPath("dx://project-123456789012345678901234:"), '')
