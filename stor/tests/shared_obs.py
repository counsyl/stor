"""
Shared unittests for Swift and S3
=================================

This file contains a number of shared tests that vary only based upon which concrete class
(SwiftPath or S3Path) is tested.
"""

import functools
import gc
import gzip

import mock
import six

from stor import obs
from stor.tests.shared import assert_same_data

import stor


def mock_method_on_Path(method_name):
    """Generates a decorator that will mock the appropriate method on the concrete path object
    (either S3Path or SwiftPath)"""
    def mocker(f):
        @functools.wraps(f)
        def wrapper(self, *args, **kwargs):
            with mock.patch.object(self.path_class, method_name, autospec=True) as method_mock:
                return f(self, method_mock, *args, **kwargs)
        return wrapper
    mocker.__name__ = 'mock_%s' % method_name
    return mocker


mock_write_object = mock_method_on_Path('write_object')
mock_read_object = mock_method_on_Path('read_object')


class SharedOBSFileCases(object):
    """Tests that only rely on read_object() on underlying path class to be called, so we can just
    parametrize on drive and path"""
    drive = None
    path_class = None

    @mock_read_object
    def test_works_with_gzip(self, mock_read_object):
        gzip_path = stor.join(stor.dirname(__file__),
                              'file_data', 's_3_2126.bcl.gz')
        text = stor.open(gzip_path, 'rb').read()
        mock_read_object.return_value = text
        fileobj = stor.open(stor.join(self.drive, 'A/C/s_3_2126.bcl.gz'), 'rb')

        with fileobj:
            with gzip.GzipFile(fileobj=fileobj) as fp:
                with gzip.open(gzip_path) as gzip_fp:
                    assert_same_data(fp, gzip_fp)

        fileobj = stor.open(stor.join(self.drive, 'A/C/s_3_2126.bcl.gz'), 'rb')

        with fileobj:
            with gzip.GzipFile(fileobj=fileobj) as fp:
                with gzip.open(gzip_path) as gzip_fp:
                    # after seeking should still be same
                    fp.seek(3)
                    gzip_fp.seek(3)
                    assert_same_data(fp, gzip_fp)

    def test_makedirs_p_does_nothing(self):
        # dumb test... but why not?
        self.normal_path.makedirs_p()

    @mock_read_object
    def test_context_manager_on_closed_file(self, mock_read_object):
        mock_read_object.return_value = b'data'
        pth = self.normal_path
        obj = pth.open()
        obj.close()

        with self.assertRaisesRegexp(ValueError, 'closed file'):
            with obj:
                pass  # pragma: no cover

    @mock_write_object
    @mock_read_object
    def test_empty_buffer_no_writes(self, mock_read_object, mock_write_object):
        # NOTE: this tests that our current description (only non-empty buffers are uploaded) is
        # enshrined.
        fileobj = stor.open(stor.join(self.drive, 'B/C/obj'), 'w')
        fileobj.flush()
        self.assertFalse(fileobj._buffer)
        fileobj.write('')
        fileobj.flush()
        fileobj.close()
        self.assertFalse(mock_read_object.called)
        self.assertFalse(mock_write_object.called)

    @mock_write_object
    @mock_read_object
    def test_on_del_no_writes(self, mock_read_object, mock_write_object):
        fileobj = stor.open(stor.join(self.drive, 'B/C/obj'), 'w')
        del fileobj
        gc.collect()

        self.assertFalse(mock_read_object.called)
        self.assertFalse(mock_write_object.called)

        fileobj = stor.open(stor.join(self.drive, 'B/C/obj'), 'r')
        del fileobj
        gc.collect()

        self.assertFalse(mock_read_object.called)
        self.assertFalse(mock_write_object.called)

    @mock.patch('time.sleep', autospec=True)
    @mock_write_object
    def test_close_no_writes(self, mock_write_object, mock_sleep):
        pth = self.normal_path
        obj = pth.open(mode='wb')
        obj.close()

        self.assertTrue(obj.closed)
        self.assertFalse(mock_write_object.called)
        self.assertFalse(obj._buffer)

    def test_invalid_buffer_mode(self):
        # internal error only
        fp = self.normal_path.open()
        fp.mode = 'invalid'
        with self.assertRaisesRegexp(ValueError, 'buffer'):
            fp._get_or_create_buffer()

    def test_invalid_open(self):
        pth = stor.join(self.drive, 'B/C/D')
        with self.assertRaisesRegexp(ValueError, 'mode'):
            # keep reference here
            f = stor.open(pth, 'invalid')  # nopep8
            assert False, 'should error before this'  # pragma: no cover

        with self.assertRaisesRegexp(ValueError, 'mode'):
            with stor.open(pth, 'invalid'):
                assert False, 'should error before this'  # pragma: no cover

        with self.assertRaisesRegexp(ValueError, 'mode'):
            with stor.Path(pth).open('invalid'):
                assert False, 'should error before this'  # pragma: no cover

    @mock_write_object
    @mock_read_object
    def test_invalid_flush_mode(self, mock_read_object, mock_write_object):
        mock_read_object.return_value = b'data'
        obj = self.normal_path.open()
        with self.assertRaisesRegexp(TypeError, 'flush'):
            obj.flush()
        self.assertFalse(mock_write_object.called)
        # should not read it if we didn't ever use it
        self.assertFalse(mock_read_object.called)

    def test_name(self):
        obj = self.normal_path.open()
        self.assertEquals(obj.name, self.normal_path)

    @mock_read_object
    def test_read_on_closed_file(self, mock_read_object):
        mock_read_object.return_value = b'data'
        obj = self.normal_path.open()
        obj.close()

        with self.assertRaisesRegexp(ValueError, 'closed file'):
            obj.read()

    def test_invalid_io_op(self):
        # now invalid delegates are considered invalid on instantiation
        with self.assertRaisesRegexp(AttributeError, 'no attribute'):
            class MyFile(object):
                closed = False
                _buffer = six.BytesIO()
                invalid = obs._delegate_to_buffer('invalid')

    def test_read_invalid_mode(self):
        pth = self.normal_path
        with self.assertRaisesRegexp(TypeError, 'mode.*read'):
            pth.open(mode='wb').read()

    def test_write_invalid_args(self):
        pth = self.normal_path
        obj = pth.open(mode='r')
        with self.assertRaisesRegexp(TypeError, 'mode.*write'):
            obj.write('hello')

    @mock.patch('time.sleep', autospec=True)
    @mock_write_object
    def test_write_multiple_w_context_manager(self, mock_write_object, mock_sleep):
        with self.normal_path.open(mode='wb') as obj:
            obj.write(b'hello')
            obj.write(b' world')
        mock_write_object.assert_called_with(self.normal_path, b'hello world')
        self.assertEquals(len(mock_write_object.call_args_list), 1)

    @mock_write_object
    def test_seek_behavior_and_writes(self, mock_write_object):
        with self.normal_path.open(mode='wb') as obj:
            obj.write(b'happy go lucky')
            obj.seek(5)
            obj.write(b'-')
            obj.seek(8)
            obj.write(b'_')
            obj.seek(0)
        mock_write_object.assert_called_with(self.normal_path, b'happy-go_lucky')
        self.assertEquals(len(mock_write_object.call_args_list), 1)
