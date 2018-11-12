import mock
import tempfile
import unittest

import stor
from stor import Path
from stor import posix
from stor_s3 import s3


class TestDiv(unittest.TestCase):
    def test_w_s3_component(self):
        p = posix.PosixPath('my/path') / s3.S3Path('s3://b/name').name
        self.assertEquals(p, posix.PosixPath('my/path/name'))
        self.assertEquals(stor.join('my/path',
                                    s3.S3Path('s3://b/name').name),
                          p)


class TestAdd(unittest.TestCase):
    def test_w_s3_component(self):
        p = posix.PosixPath('my/path') + s3.S3Path('s3://b/name').name
        self.assertEquals(p, posix.PosixPath('my/pathname'))


class TestCopy(unittest.TestCase):
    @mock.patch.object(s3.S3Path, 'upload', autospec=True)
    def test_s3_destination(self, mock_upload):
        dest = Path('s3://bucket/key/file.txt')
        with tempfile.NamedTemporaryFile() as tmp_f:
            Path(tmp_f.name).copy(dest)
            upload_args = mock_upload.call_args_list[0][0]
            self.assertEquals(upload_args[0], dest.parent)
            self.assertEquals(upload_args[1][0].source, tmp_f.name)
            self.assertEquals(upload_args[1][0].object_name, 'key/file.txt')
