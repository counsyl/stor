import mock
import unittest

from stor import Path
from stor import utils as stor_utils
from stor_swift import utils
from stor_swift.swift import SwiftPath
import stor_swift


class TestPath(unittest.TestCase):
    def test_swift_returned(self):
        p = Path('swift://my/swift/path')
        self.assertTrue(isinstance(p, SwiftPath))


class TestIsSwiftPath(unittest.TestCase):
    def test_true(self):
        self.assertTrue(utils.is_swift_path('swift://my/swift/path'))

    def test_false(self):
        self.assertFalse(utils.is_swift_path('my/posix/path'))


class TestIsWriteableSwift(unittest.TestCase):
    def setUp(self):
        super(TestIsWriteableSwift, self).setUp()

        mock_exists_patcher = mock.patch('stor_swift.swift.SwiftPath.exists', autospec=True)
        self.mock_exists = mock_exists_patcher.start()
        self.addCleanup(mock_exists_patcher.stop)

        mock_copy_patcher = mock.patch('stor.copy', autospec=True)
        self.mock_copy = mock_copy_patcher.start()
        self.addCleanup(mock_copy_patcher.stop)

        mock_remove_container_patcher = mock.patch(
            'stor_swift.swift.SwiftPath.remove_container', autospec=True)
        self.mock_remove_container = mock_remove_container_patcher.start()
        self.addCleanup(mock_remove_container_patcher.stop)

        mock_remove_patcher = mock.patch('stor.remove', autospec=True)
        self.mock_remove = mock_remove_patcher.start()
        self.addCleanup(mock_remove_patcher.stop)

        mock_tmpfile_patcher = mock.patch(
            'stor.utils.tempfile.NamedTemporaryFile', autospec=True)
        self.filename = 'test_file'
        self.mock_tmpfile = mock_tmpfile_patcher.start()
        self.mock_tmpfile.return_value.__enter__.return_value.name = self.filename
        self.addCleanup(mock_tmpfile_patcher.stop)

    def test_existing_path(self):
        self.mock_exists.return_value = True
        path = SwiftPath('swift://AUTH_stor_test/container/test/')
        self.assertTrue(utils.is_writeable(path))
        self.mock_remove.assert_called_with(path / self.filename)

    def test_non_existing_path(self):
        self.mock_exists.return_value = False
        self.assertTrue(utils.is_writeable('swift://AUTH_stor_test/container/test/'))

    def test_path_unchanged(self):
        # Make the first call to exists() return False and the second return True.
        self.mock_exists.side_effect = [False, True]
        utils.is_writeable('swift://AUTH_stor_test/container/test/')
        self.mock_remove_container.assert_called_once_with(
            SwiftPath('swift://AUTH_stor_test/container/'))

    def test_existing_path_not_removed(self):
        self.mock_exists.return_value = True
        utils.is_writeable('swift://AUTH_stor_test/container/test/')
        self.assertFalse(self.mock_remove_container.called)

    def test_path_no_perms(self):
        self.mock_copy.side_effect = stor_swift.swift.UnauthorizedError('foo')
        self.assertFalse(utils.is_writeable('swift://AUTH_stor_test/container/test/'))

    def test_disable_backoff(self):
        path = Path('swift://AUTH_stor_test/container/test/')
        swift_opts = {'num_retries': 0}
        utils.is_writeable(path, swift_opts)
        self.mock_copy.assert_called_with(
            self.filename, path, swift_retry_options=swift_opts)

    def test_no_trailing_slash(self):
        path = SwiftPath('swift://AUTH_stor_test/container')
        utils.is_writeable(path)  # no trailing slash
        self.mock_copy.assert_called_with(
            self.filename,
            stor_utils.with_trailing_slash(path),
            swift_retry_options=None
        )

    def test_container_created_in_another_client(self):
        # Simulate that container doesn't exist at the beginning, but is created after the
        # is_writeable is called.
        self.mock_exists.side_effect = [False, True]
        self.mock_remove_container.side_effect = stor_swift.swift.ConflictError('foo')
        self.assertTrue(utils.is_writeable('swift://AUTH_stor_test/container/'))
