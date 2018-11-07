import mock
import unittest

from stor import Path
from stor_s3 import utils
from stor_s3.s3 import S3Path
import stor


class TestPath(unittest.TestCase):
    def test_s3_returned(self):
        p = Path('s3://my/s3/path')
        self.assertTrue(isinstance(p, S3Path))


class TestIsWriteableS3(unittest.TestCase):
    def setUp(self):
        super(TestIsWriteableS3, self).setUp()

        mock_copy_patcher = mock.patch('stor.utils.copy')
        self.mock_copy = mock_copy_patcher.start()
        self.addCleanup(mock_copy_patcher.stop)

        mock_remove_patcher = mock.patch('stor.remove', autospec=True)
        self.mock_remove = mock_remove_patcher.start()
        self.addCleanup(mock_remove_patcher.stop)

        mock_tmpfile_patcher = mock.patch(
            'stor.utils.tempfile.NamedTemporaryFile', autospec=True)
        self.filename = 'test_file'
        self.mock_tmpfile = mock_tmpfile_patcher.start()
        self.mock_tmpfile.return_value.__enter__.return_value.name = self.filename
        self.addCleanup(mock_tmpfile_patcher.stop)

    def test_success(self):
        path = 's3://stor-test/foo/bar'
        self.assertTrue(utils.is_writeable(path))
        self.mock_remove.assert_called_with(
            S3Path('{}/{}'.format(path, self.filename)))

    def test_path_no_perms(self):
        self.mock_copy.side_effect = stor.exceptions.FailedUploadError('foo')
        self.assertFalse(utils.is_writeable('s3://stor-test/foo/bar'))
        self.assertFalse(self.mock_remove.called)
