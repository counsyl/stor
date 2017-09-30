from nose.tools import raises
from unittest import skipIf
import six
import stor
import os

byte_string = b'hey'
string_string = 'hey'
base_name = "test.txt"


def generate_file(opener, test_filename, mode='b'):
    if mode == "b":
        fp = opener(test_filename, 'wb')
        fp.write(byte_string)
    else:
        fp = opener(test_filename, 'w')
        fp.write(string_string)


class BaseTest(object):
    __test__ = False

    @property
    def test_filename(self):
        return self.basepath + "/" + base_name  # Yes I know

    def test_write_bytes_to_binary(self):
        fp = self.open(self.test_filename, mode='wb')
        fp.write(byte_string)

    @skipIf(not six.PY3, "Only tested on py3")
    @raises(TypeError)
    def test_write_string_to_binary(self):
        fp = self.open(self.test_filename, mode='wb')
        fp.write(string_string)

    @skipIf(not six.PY3, "Only tested on py3")
    @raises(TypeError)
    def test_write_bytes_to_text(self):
        fp = self.open(self.test_filename, mode='w')
        fp.write(byte_string)

    def test_write_string_to_text(self):
        fp = self.open(self.test_filename, mode='w')
        fp.write(string_string)

    def test_read_bytes_from_binary(self):
        generate_file(self.open, self.test_filename, mode='b')
        fp = self.open(self.test_filename, mode='rb')
        result = fp.read()
        print(type(result), result)
        assert result == byte_string, "Strings don't match!"

    def test_read_string_from_text(self):
        generate_file(self.open, self.test_filename, mode='t')
        fp = self.open(self.test_filename, mode='r')
        result = fp.read()
        print(type(result), result)
        assert result == string_string, "Strings don't match!"


class FileTest(BaseTest):
    __test__ = True
    basepath = "./"

    def open(self, filename, mode):
        return open(filename, mode)


class StorPosixTest(BaseTest):
    __test__ = True
    basepath = "./"

    def open(self, filename, mode):
        return stor.Path(filename).open(mode)


class StorSwiftTest(StorPosixTest):
    basepath = os.environ.get("TEST_SWIFTPATH")
    __test__ = True if basepath is not None else False
