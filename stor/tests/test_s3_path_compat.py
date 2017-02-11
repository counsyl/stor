import unittest

from stor.s3 import S3Path


class TestBasics(unittest.TestCase):
    def test_relpath(self):
        with self.assertRaises(AttributeError):
            S3Path('s3://bucket').relpathto()
        with self.assertRaises(AttributeError):
            S3Path('s3://bucket').relpath()

    def test_construction_from_none(self):
        with self.assertRaises(ValueError):
            S3Path(None)

    def test_string_compatibility(self):
        """ Test compatibility with ordinary strings. """
        x = S3Path('s3://xyzzy')
        assert x == 's3://xyzzy'
        assert x == str('s3://xyzzy')
        assert 'xyz' in x
        assert 'analysis' not in x

        # sorting
        items = [S3Path('s3://fhj'),
                 S3Path('s3://fgh'),
                 's3://E',
                 S3Path('s3://d'),
                 's3://A',
                 S3Path('s3://B'),
                 's3://c']
        items.sort()
        self.assertEqual(items,
                         ['s3://A', 's3://B', 's3://E', 's3://c',
                          's3://d', 's3://fgh', 's3://fhj'])

        # Test p1/p1.
        p1 = S3Path("s3://foo")
        p2 = "bar"
        self.assertEqual(p1 / p2, S3Path("s3://foo/bar"))

    def test_properties(self):
        # Create sample path object.
        f = S3Path('s3://bucket/prefix/whatever.csv')

        self.assertEqual(f.parent, S3Path('s3://bucket/prefix'))

        # .name
        self.assertEqual(f.name, 'whatever.csv')
        self.assertEqual(f.parent.name, 'prefix')
        self.assertEqual(f.parent.parent.name, 'bucket')

        # .ext
        self.assertEqual(f.ext, '.csv')
        self.assertEqual(f.parent.ext, '')
