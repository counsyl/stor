import mock
from storage_utils.posix import PosixPath
import unittest


class TestCopy(unittest.TestCase):
    @mock.patch('storage_utils.utils.copy', autospec=True)
    def test_copy(self, mock_copy):
        p = PosixPath('p')
        p.copy('other/path', copy_cmd='mcp -rL', object_threads=30,
               segment_threads=40, num_retries=1)
        mock_copy.assert_called_once_with(p,
                                          'other/path',
                                          copy_cmd='mcp -rL',
                                          object_threads=30,
                                          segment_threads=40,
                                          num_retries=1)
