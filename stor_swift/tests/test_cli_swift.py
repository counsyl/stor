from __future__ import print_function

import contextlib
import os
import mock
import sys
from tempfile import NamedTemporaryFile

import six

from stor import cli
from stor_swift.swift import SwiftPath
from stor_swift import test


class BaseCliTest(test.SwiftTestCase):
    def setUp(self):
        patcher = mock.patch.object(sys, 'stdout', six.StringIO())
        self.addCleanup(patcher.stop)
        patcher.start()

    @contextlib.contextmanager
    def assertOutputMatches(self, exit_status=None, stdout='', stderr=''):
        patch = mock.patch('sys.stderr', new=six.StringIO())
        self.addCleanup(patch.stop)
        patch.start()
        if exit_status is not None:
            try:
                yield
            except SystemExit as e:
                self.assertEquals(e.code, int(exit_status))
            else:
                assert False, 'SystemExit not raised'
        else:
            yield
        if not stdout:
            self.assertEquals(sys.stdout.getvalue(), '', 'stdout')
        else:
            self.assertRegexpMatches(sys.stdout.getvalue(), stdout, 'stdout')
        if not stderr:
            self.assertEquals(sys.stderr.getvalue(), '', 'stderr')
        else:
            self.assertRegexpMatches(sys.stderr.getvalue(), stderr, 'stderr')

    def parse_args(self, args):
        with mock.patch.object(sys, 'argv', args.split()):
            cli.main()


@mock.patch('stor.cli._get_pwd', autospec=True)
class TestGetPath(BaseCliTest):
    def test_relpath_no_swift(self, mock_pwd):
        mock_pwd.return_value = 'swift://'
        with self.assertRaisesRegexp(ValueError, 'relative path'):
            cli.get_path('swift:../test')

    def test_relpath_empty_swift(self, mock_pwd):
        with self.assertRaisesRegexp(ValueError, 'invalid'):
            cli.get_path('swift:')

    def test_relpath_current_swift(self, mock_pwd):
        mock_pwd.return_value = 'swift://test'
        self.assertEquals(cli.get_path('swift:.'), SwiftPath('swift://test/'))
        mock_pwd.return_value = 'swift://test/'
        self.assertEquals(cli.get_path('swift:.'), SwiftPath('swift://test/'))

    def test_relpath_current_subdir_swift(self, mock_pwd):
        mock_pwd.return_value = 'swift://test/cont'
        self.assertEquals(cli.get_path('swift:./b/c'), SwiftPath('swift://test/cont/b/c'))
        mock_pwd.return_value = 'swift://test/cont/'
        self.assertEquals(cli.get_path('swift:./b/c'), SwiftPath('swift://test/cont/b/c'))

    def test_relpath_current_subdir_no_dot_swift(self, mock_pwd):
        mock_pwd.return_value = 'swift://test/cont'
        self.assertEquals(cli.get_path('swift:b/c'), SwiftPath('swift://test/cont/b/c'))
        mock_pwd.return_value = 'swift://test/cont/'
        self.assertEquals(cli.get_path('swift:b/c'), SwiftPath('swift://test/cont/b/c'))

    def test_relpath_parent_swift(self, mock_pwd):
        mock_pwd.return_value = 'swift://test/dir'
        self.assertEquals(cli.get_path('swift:..'), SwiftPath('swift://test/'))
        mock_pwd.return_value = 'swift://test/dir/'
        self.assertEquals(cli.get_path('swift:..'), SwiftPath('swift://test/'))

    def test_relpath_parent_subdir_swift(self, mock_pwd):
        mock_pwd.return_value = 'swift://test/cont/dir'
        self.assertEquals(cli.get_path('swift:../b/c'), SwiftPath('swift://test/cont/b/c'))
        mock_pwd.return_value = 'swift://test/cont/dir/'
        self.assertEquals(cli.get_path('swift:../b/c'), SwiftPath('swift://test/cont/b/c'))

    def test_relpath_no_parent_swift(self, mock_pwd):
        mock_pwd.return_value = 'swift://test'
        with self.assertRaisesRegexp(ValueError, 'Relative path.*invalid'):
            cli.get_path('swift:../b/c')
        mock_pwd.return_value = 'swift://test/'
        with self.assertRaisesRegexp(ValueError, 'Relative path.*invalid'):
            cli.get_path('swift:../b/c')

    def test_relpath_nested_parent(self, mock_pwd):
        mock_pwd.return_value = 'swift://a/b/c/'
        self.assertEquals(cli.get_path('swift:../../d'), SwiftPath('swift://a/d'))

    def test_invalid_abspath_swift(self, mock_pwd):
        with self.assertRaisesRegexp(ValueError, 'invalid path'):
            cli.get_path('swift:/some/path')


class TestList(BaseCliTest):
    @mock.patch.object(SwiftPath, 'list', autospec=True)
    def test_list_swift(self, mock_list):
        mock_list.return_value = [
            SwiftPath('swift://t/c/file1'),
            SwiftPath('swift://t/c/dir/file2'),
            SwiftPath('swift://t/c/file3')
        ]
        self.parse_args('stor list swift://t/c/')
        self.assertEquals(sys.stdout.getvalue(),
                          'swift://t/c/file1\n'
                          'swift://t/c/dir/file2\n'
                          'swift://t/c/file3\n')
        mock_list.assert_called_once_with(SwiftPath('swift://t/c/'))


class TestLs(BaseCliTest):
    @mock.patch.object(SwiftPath, 'listdir', autospec=True)
    def test_listdir_swift(self, mock_listdir):
        mock_listdir.return_value = [
            SwiftPath('swift://t/c/file1'),
            SwiftPath('swift://t/c/dir/'),
            SwiftPath('swift://t/c/file3')
        ]
        self.parse_args('stor ls swift://t/c')
        self.assertEquals(sys.stdout.getvalue(),
                          'swift://t/c/file1\n'
                          'swift://t/c/dir/\n'
                          'swift://t/c/file3\n')
        mock_listdir.assert_called_once_with(SwiftPath('swift://t/c'))


class TestRemove(BaseCliTest):
    @mock.patch.object(SwiftPath, 'remove', autospec=True)
    def test_remove_swift(self, mock_remove):
        self.parse_args('stor rm swift://t/c/file1')
        mock_remove.assert_called_once_with(SwiftPath('swift://t/c/file1'))

    @mock.patch.object(SwiftPath, 'rmtree', autospec=True)
    def test_rmtree_swift(self, mock_rmtree):
        self.parse_args('stor rm -r swift://t/c/dir')
        mock_rmtree.assert_called_once_with(SwiftPath('swift://t/c/dir'))


class TestWalkfiles(BaseCliTest):
    @mock.patch.object(SwiftPath, 'walkfiles', autospec=True)
    def test_walkfiles_swift(self, mock_walkfiles):
        mock_walkfiles.return_value = [
            'swift://t/c/a/b.txt',
            'swift://t/c/c.txt',
            'swift://t/c/d.txt'
        ]
        self.parse_args('stor walkfiles -p=*.txt swift://bucket')
        self.assertEquals(sys.stdout.getvalue(),
                          'swift://t/c/a/b.txt\n'
                          'swift://t/c/c.txt\n'
                          'swift://t/c/d.txt\n')
        mock_walkfiles.assert_called_once_with(SwiftPath('swift://bucket'), pattern='*.txt')


class TestCat(BaseCliTest):
    @mock.patch.object(SwiftPath, 'read_object', autospec=True)
    def test_cat_swift(self, mock_read):
        mock_read.return_value = b'hello world'
        self.parse_args('stor cat swift://some/test/file')
        self.assertEquals(sys.stdout.getvalue(), 'hello world\n')
        mock_read.assert_called_once_with(SwiftPath('swift://some/test/file'))


class TestCd(BaseCliTest):
    def setUp(self):
        # set up a temp file to use as env file so we don't mess up real defaults
        self.test_env_file = NamedTemporaryFile(delete=False).name
        mock_env_file_patcher = mock.patch('stor.cli.ENV_FILE', self.test_env_file)
        self.mock_env_file = mock_env_file_patcher.start()
        self.addCleanup(mock_env_file_patcher.stop)

        with mock.patch('os.path.exists', return_value=False, autospec=True):
            cli._clear_env()
        super(TestCd, self).setUp()

    def tearDown(self):
        cli._clear_env()
        os.remove(self.test_env_file)
        super(TestCd, self).tearDown()

    def generate_env_text(self, s3_path='s3://', swift_path='swift://'):
        return '[env]\ns3 = %s\nswift = %s\n' % (s3_path, swift_path)

    @mock.patch.object(SwiftPath, 'isdir', return_value=True, autospec=True)
    def test_cd_swift(self, mock_isdir):
        self.parse_args('stor cd swift://test/container')
        self.assertIn(self.generate_env_text(swift_path='swift://test/container'),
                      open(self.test_env_file).read())

    @mock.patch.object(SwiftPath, 'isdir', return_value=False, autospec=True)
    def test_cd_not_dir_swift(self, mock_isdir):
        with self.assertOutputMatches(exit_status=1, stderr='not a directory'):
            self.parse_args('stor cd swift://test/container/file')

    def test_cd_bad_path_error(self):
        with self.assertOutputMatches(exit_status=1, stderr='invalid path'):
            self.parse_args('stor cd drive://not/correct')

    def test_clear_all(self):
        with open(self.test_env_file, 'w') as outfile:
            outfile.write(self.generate_env_text(s3_path='s3://test',
                                                 swift_path='swift://test/container'))
        self.parse_args('stor clear')
        self.assertIn(self.generate_env_text(), open(self.test_env_file).read())

    def test_clear_s3(self):
        with open(self.test_env_file, 'w') as outfile:
            outfile.write(self.generate_env_text(s3_path='s3://test',
                                                 swift_path='swift://test/container'))
        self.parse_args('stor clear s3')
        self.assertIn(self.generate_env_text(swift_path='swift://test/container'),
                      open(self.test_env_file).read())

    def test_clear_swift(self):
        with open(self.test_env_file, 'w') as outfile:
            outfile.write(self.generate_env_text(s3_path='s3://test',
                                                 swift_path='swift://test/container'))
        self.parse_args('stor clear swift')
        self.assertIn(self.generate_env_text(s3_path='s3://test'),
                      open(self.test_env_file).read())

    def test_pwd_all(self):
        with open(self.test_env_file, 'w') as outfile:
            outfile.write(self.generate_env_text(s3_path='s3://test',
                                                 swift_path='swift://test/container'))
        self.parse_args('stor pwd')
        self.assertEquals(sys.stdout.getvalue(), 's3://test/\nswift://test/container/\n')

    def test_pwd_s3(self):
        with open(self.test_env_file, 'w') as outfile:
            outfile.write(self.generate_env_text(s3_path='s3://test',
                                                 swift_path='swift://test/container'))
        self.parse_args('stor pwd s3')
        self.assertEquals(sys.stdout.getvalue(), 's3://test/\n')

    def test_pwd_swift(self):
        with open(self.test_env_file, 'w') as outfile:
            outfile.write(self.generate_env_text(s3_path='s3://test',
                                                 swift_path='swift://test/container'))
        self.parse_args('stor pwd swift')
        self.assertEquals(sys.stdout.getvalue(), 'swift://test/container/\n')

    def test_pwd_error(self):
        with self.assertOutputMatches(exit_status=1, stderr='invalid service'):
            self.parse_args('stor pwd service')
