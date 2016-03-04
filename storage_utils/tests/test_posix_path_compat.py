"""
Tests (from path.py) to ensure compatibility with path.py module.
"""
import os
import platform

import unittest

from storage_utils import NamedTemporaryDirectory
from storage_utils import Path


def p(**choices):
    """ Choose a value from several possible values, based on os.name """
    return choices[os.name]


class TestBasics(unittest.TestCase):
    def test_relpath(self):
        root = Path(p(nt='C:\\', posix='/'))
        foo = root / 'foo'
        quux = foo / 'quux'
        bar = foo / 'bar'
        boz = bar / 'Baz' / 'Boz'
        up = Path(os.pardir)

        # basics
        assert root.relpathto(boz) == Path('foo')/'bar'/'Baz'/'Boz'
        assert bar.relpathto(boz) == Path('Baz')/'Boz'
        assert quux.relpathto(boz) == up/'bar'/'Baz'/'Boz'
        assert boz.relpathto(quux) == up/up/up/'quux'
        assert boz.relpathto(bar) == up/up

        # Path is not the first element in concatenation
        assert root.relpathto(boz) == 'foo'/Path('bar')/'Baz'/'Boz'

        # x.relpathto(x) == curdir
        assert root.relpathto(root) == os.curdir
        assert boz.relpathto(boz) == os.curdir
        # Make sure case is properly noted (or ignored)
        assert boz.relpathto(boz.normcase()) == os.curdir

        # relpath()
        cwd = Path(os.getcwd())
        assert boz.relpath() == cwd.relpathto(boz)

        if os.name == 'nt':
            # Check relpath across drives.
            d = Path('D:\\')
            assert d.relpathto(boz) == boz

    def test_construction_from_none(self):
        """

        """
        try:
            Path(None)
        except TypeError:
            pass
        else:
            raise Exception("DID NOT RAISE")

    def test_construction_from_int(self):
        """
        Path class will construct a path as a string of the number
        """
        assert Path(1) == '1'

    def test_string_compatibility(self):
        """ Test compatibility with ordinary strings. """
        x = Path('xyzzy')
        assert x == 'xyzzy'
        assert x == str('xyzzy')

        # sorting
        items = [Path('fhj'),
                 Path('fgh'),
                 'E',
                 Path('d'),
                 'A',
                 Path('B'),
                 'c']
        items.sort()
        assert items == ['A', 'B', 'E', 'c', 'd', 'fgh', 'fhj']

        # Test p1/p1.
        p1 = Path("foo")
        p2 = Path("bar")
        assert p1/p2 == p(nt='foo\\bar', posix='foo/bar')

    def test_properties(self):
        # Create sample path object.
        f = p(nt='C:\\Program Files\\Python\\Lib\\xyzzy.py',
              posix='/usr/local/python/lib/xyzzy.py')
        f = Path(f)

        # .parent
        nt_lib = 'C:\\Program Files\\Python\\Lib'
        posix_lib = '/usr/local/python/lib'
        expected = p(nt=nt_lib, posix=posix_lib)
        assert f.parent == expected

        # .name
        assert f.name == 'xyzzy.py'
        assert f.parent.name == p(nt='Lib', posix='lib')

        # .ext
        assert f.ext == '.py'
        assert f.parent.ext == ''

        # .drive
        assert f.drive == p(nt='C:', posix='')

    def test_methods(self):
        # .abspath()
        assert Path(os.curdir).abspath() == os.getcwd()

        # .getcwd()
        cwd = Path.getcwd()
        assert isinstance(cwd, Path)
        assert cwd == os.getcwd()

    def test_joinpath_on_instance(self):
        res = Path('foo')
        foo_bar = res.joinpath('bar')
        assert foo_bar == p(nt='foo\\bar', posix='foo/bar')

    def test_joinpath_to_nothing(self):
        res = Path('foo')
        assert res.joinpath() == res


class TestScratchDir(unittest.TestCase):
    """
    Tests that run in a temporary directory (does not test tempdir class)
    """
    def test_context_manager(self):
        """Can be used as context manager for chdir."""
        with NamedTemporaryDirectory() as tmpdir:
            d = Path(tmpdir)
            subdir = d / 'subdir'
            subdir.makedirs()
            old_dir = os.getcwd()
            with subdir:
                assert os.getcwd() == os.path.realpath(subdir)
            assert os.getcwd() == old_dir

    def test_listing(self):
        with NamedTemporaryDirectory() as tmpdir:
            d = Path(tmpdir)
            assert d.listdir() == []

            f = 'testfile.txt'
            af = d / f
            assert af == os.path.join(d, f)
            af.touch()
            try:
                assert af.exists()

                assert d.listdir() == [af]

                # .glob()
                assert d.glob('testfile.txt') == [af]
                assert d.glob('test*.txt') == [af]
                assert d.glob('*.txt') == [af]
                assert d.glob('*txt') == [af]
                assert d.glob('*') == [af]
                assert d.glob('*.html') == []
                assert d.glob('testfile') == []
            finally:
                af.remove()

            # Try a test with 20 files
            files = [d / ('%d.txt' % i) for i in range(20)]
            for f in files:
                fobj = open(f, 'w')
                fobj.write('some text\n')
                fobj.close()
            try:
                files2 = d.listdir()
                files.sort()
                files2.sort()
                assert files == files2
            finally:
                for f in files:
                    try:
                        f.remove()
                    except:
                        pass

    def test_listdir_other_encoding(self):
        """
        Some filesystems allow non-character sequences in path names.
        ``.listdir`` should still function in this case.
        See issue #61 for details.
        """
        with NamedTemporaryDirectory() as tmpdir:
            assert Path(tmpdir).listdir() == []
            tmpdir_bytes = str(tmpdir).encode('ascii')

            filename = 'r\xe9\xf1emi'.encode('latin-1')
            pathname = os.path.join(tmpdir_bytes, filename)
            with open(pathname, 'wb'):
                pass
            # first demonstrate that os.listdir works
            assert os.listdir(tmpdir_bytes)

            # now try with path.py
            results = Path(tmpdir).listdir()
            assert len(results) == 1
            res, = results
            assert isinstance(res, Path)
            # OS X seems to encode the bytes in the filename as %XX characters.
            if platform.system() == 'Darwin':
                assert res.basename() == 'r%E9%F1emi'
                return
            assert len(res.basename()) == len(filename)

    def test_makedirs(self):
        with NamedTemporaryDirectory() as tmpdir:
            d = Path(tmpdir)

            # Placeholder file so that when removedirs() is called,
            # it doesn't remove the temporary directory itself.
            tempf = d / 'temp.txt'
            tempf.touch()
            try:
                foo = d / 'foo'
                boz = foo / 'bar' / 'baz' / 'boz'
                boz.makedirs()
                try:
                    assert boz.isdir()
                finally:
                    foo.rmtree()
                assert not foo.exists()
                assert d.exists()

                foo.mkdir(0o750)
                boz.makedirs(0o700)
                try:
                    assert boz.isdir()
                finally:
                    foo.rmtree()
                assert not foo.exists()
                assert d.exists()
            finally:
                os.remove(tempf)

    def assertSetsEqual(self, a, b):
        ad = {}

        for i in a:
            ad[i] = None

        bd = {}

        for i in b:
            bd[i] = None

        assert ad == bd

    def assertList(self, listing, expected):
        assert sorted(listing) == sorted(expected)


class TestChdir(unittest.TestCase):
    def test_chdir_or_cd(self):
        """ tests the chdir or cd method """
        with NamedTemporaryDirectory() as tmpdir:
            d = Path(str(tmpdir))
            cwd = d.getcwd()

            # ensure the cwd isn't our tempdir
            assert str(d) != str(cwd)
            # now, we're going to chdir to tempdir
            d.chdir()

            # we now ensure that our cwd is the tempdir
            assert str(d.getcwd()) == str(tmpdir)
            # we're resetting our path
            d = Path(cwd)

            # we ensure that our cwd is still set to tempdir
            assert str(d.getcwd()) == str(tmpdir)

            # we're calling the alias cd method
            d.cd()
            # now, we ensure cwd isn'r tempdir
            assert str(d.getcwd()) == str(cwd)
            assert str(d.getcwd()) != str(tmpdir)
