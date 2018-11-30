from __future__ import print_function

import contextlib
import os
import mock
import sys
from tempfile import NamedTemporaryFile

import six

from stor import cli
from stor import exceptions
from stor_s3 import test
from stor_s3.s3 import S3Path


class BaseCliTest(test.S3TestCase):
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


class TestCliTestUtils(BaseCliTest):
    def test_no_exit_status(self):
        with six.assertRaisesRegex(self, AssertionError, 'SystemExit'):
            with self.assertOutputMatches(exit_status='1'):
                pass


class TestCliBasics(BaseCliTest):
    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_cli_error(self, mock_list):
        mock_list.side_effect = exceptions.RemoteError('some error')
        with self.assertOutputMatches(exit_status='1', stderr='RemoteError: some error'):
            self.parse_args('stor list s3://bucket')


@mock.patch('stor.cli._get_pwd', autospec=True)
class TestGetPath(BaseCliTest):
    def test_relpath_no_s3(self, mock_pwd):
        mock_pwd.return_value = 's3://'
        with self.assertRaisesRegexp(ValueError, 'relative path'):
            cli.get_path('s3:../test')

    def test_relpath_empty_s3(self, mock_pwd):
        with self.assertRaisesRegexp(ValueError, 'invalid'):
            cli.get_path('s3:')

    def test_relpath_current_s3(self, mock_pwd):
        mock_pwd.return_value = 's3://test'
        self.assertEquals(cli.get_path('s3:.'), S3Path('s3://test/'))
        mock_pwd.return_value = 's3://test/'
        self.assertEquals(cli.get_path('s3:.'), S3Path('s3://test/'))

    def test_relpath_current_subdir_s3(self, mock_pwd):
        mock_pwd.return_value = 's3://test'
        self.assertEquals(cli.get_path('s3:./b/c'), S3Path('s3://test/b/c'))
        mock_pwd.return_value = 's3://test/'
        self.assertEquals(cli.get_path('s3:./b/c'), S3Path('s3://test/b/c'))

    def test_relpath_current_subdir_no_dot_s3(self, mock_pwd):
        mock_pwd.return_value = 's3://test'
        self.assertEquals(cli.get_path('s3:b/c'), S3Path('s3://test/b/c'))
        mock_pwd.return_value = 's3://test/'
        self.assertEquals(cli.get_path('s3:b/c'), S3Path('s3://test/b/c'))

    def test_relpath_parent_s3(self, mock_pwd):
        mock_pwd.return_value = 's3://test/dir'
        self.assertEquals(cli.get_path('s3:..'), S3Path('s3://test/'))
        mock_pwd.return_value = 's3://test/dir/'
        self.assertEquals(cli.get_path('s3:..'), S3Path('s3://test/'))

    def test_relpath_parent_subdir_s3(self, mock_pwd):
        mock_pwd.return_value = 's3://test/dir'
        self.assertEquals(cli.get_path('s3:../b/c'), S3Path('s3://test/b/c'))
        mock_pwd.return_value = 's3://test/dir/'
        self.assertEquals(cli.get_path('s3:../b/c'), S3Path('s3://test/b/c'))

    def test_relpath_no_parent_s3(self, mock_pwd):
        mock_pwd.return_value = 's3://test'
        with self.assertRaisesRegexp(ValueError, 'Relative path.*invalid'):
            cli.get_path('s3:../b/c')
        mock_pwd.return_value = 's3://test/'
        with self.assertRaisesRegexp(ValueError, 'Relative path.*invalid'):
            cli.get_path('s3:../b/c')

    def test_relpath_nested_parent(self, mock_pwd):
        mock_pwd.return_value = 's3://a/b/c'
        self.assertEquals(cli.get_path('s3:../../d'), S3Path('s3://a/d'))

    def test_relpath_nested_parent_error(self, mock_pwd):
        mock_pwd.return_value = 's3://a/b/c'
        with self.assertRaisesRegexp(ValueError, 'Relative path.*invalid'):
            cli.get_path('s3:../../../d')
        mock_pwd.return_value = 'swift://a/b/c/'
        with self.assertRaisesRegexp(ValueError, 'Relative path.*invalid'):
            cli.get_path('swift:../../../d')

    def test_invalid_abspath_s3(self, mock_pwd):
        with self.assertRaisesRegexp(ValueError, 'invalid path'):
            cli.get_path('s3:/some/path')


class TestList(BaseCliTest):
    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_list_s3(self, mock_list):
        mock_list.return_value = [
            S3Path('s3://a/b/c'),
            S3Path('s3://a/file1'),
            S3Path('s3://a/file2')
        ]
        self.parse_args('stor list s3://a')
        self.assertEquals(sys.stdout.getvalue(),
                          's3://a/b/c\ns3://a/file1\ns3://a/file2\n')
        mock_list.assert_called_once_with(S3Path('s3://a'))

    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_list_options(self, mock_list):
        mock_list.side_effect = [[
            S3Path('s3://some-bucket/dir/a'),
            S3Path('s3://some-bucket/dir/b'),
            S3Path('s3://some-bucket/dir/c/d')
        ], [
            S3Path('s3://some-bucket/a'),
            S3Path('s3://some-bucket/b')
        ]]

        self.parse_args('stor list s3://some-bucket -s dir')
        self.assertEquals(sys.stdout.getvalue(),
                          's3://some-bucket/dir/a\n'
                          's3://some-bucket/dir/b\n'
                          's3://some-bucket/dir/c/d\n')

        # clear stdout
        sys.stdout = six.StringIO()

        self.parse_args('stor list s3://some-bucket -l2')
        self.assertEquals(sys.stdout.getvalue(),
                          's3://some-bucket/a\n'
                          's3://some-bucket/b\n')

        mock_list.assert_has_calls([
            mock.call(S3Path('s3://some-bucket'), starts_with='dir'),
            mock.call(S3Path('s3://some-bucket'), limit=2)
        ])

    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_list_not_found(self, mock_list):
        mock_list.side_effect = exceptions.NotFoundError('not found')
        with self.assertOutputMatches(exit_status='1', stderr='s3://bucket/path'):
            self.parse_args('stor list s3://bucket/path')


class TestLs(BaseCliTest):
    @mock.patch.object(S3Path, 'listdir', autospec=True)
    def test_listdir_s3(self, mock_listdir):
        mock_listdir.return_value = [
            S3Path('s3://bucket/file1'),
            S3Path('s3://bucket/file2'),
            S3Path('s3://bucket/dir/')
        ]
        self.parse_args('stor ls s3://bucket')
        self.assertEquals(sys.stdout.getvalue(),
                          's3://bucket/file1\n'
                          's3://bucket/file2\n'
                          's3://bucket/dir/\n')
        mock_listdir.assert_called_once_with(S3Path('s3://bucket'))


@mock.patch('stor.copy', autospec=True)
class TestCopy(BaseCliTest):
    def test_copy(self, mock_copy):
        self.parse_args('stor cp s3://bucket/file.txt ./file1')
        mock_copy.assert_called_once_with(path='s3://bucket/file.txt', dest='./file1')


@mock.patch('stor.copytree', autospec=True)
class TestCopytree(BaseCliTest):
    def test_copytree(self, mock_copytree):
        self.parse_args('stor cp -r s3://bucket .')
        mock_copytree.assert_called_once_with(path='s3://bucket', dest='.')

    def test_copytree_stdin_error(self, mock_copytree):
        with self.assertOutputMatches(exit_status='2', stderr='- cannot be used with -r'):
            self.parse_args('stor cp -r - s3://bucket')


class TestRemove(BaseCliTest):
    @mock.patch.object(S3Path, 'remove', autospec=True)
    def test_remove_s3(self, mock_remove):
        self.parse_args('stor rm s3://bucket/file1')
        mock_remove.assert_called_once_with(S3Path('s3://bucket/file1'))

    @mock.patch.object(S3Path, 'rmtree', autospec=True)
    def test_rmtree_s3(self, mock_rmtree):
        self.parse_args('stor rm -r s3://bucket/dir')
        mock_rmtree.assert_called_once_with(S3Path('s3://bucket/dir'))


class TestWalkfiles(BaseCliTest):
    @mock.patch.object(S3Path, 'walkfiles', autospec=True)
    def test_walkfiles_s3(self, mock_walkfiles):
        mock_walkfiles.return_value = [
            's3://bucket/a/b.txt',
            's3://bucket/c.txt',
            's3://bucket/d.txt'
        ]
        self.parse_args('stor walkfiles -p=*.txt s3://bucket')
        self.assertEquals(sys.stdout.getvalue(),
                          's3://bucket/a/b.txt\n'
                          's3://bucket/c.txt\n'
                          's3://bucket/d.txt\n')
        mock_walkfiles.assert_called_once_with(S3Path('s3://bucket'), pattern='*.txt')


class TestToUri(BaseCliTest):
    def test_to_url(self):
        with self.assertOutputMatches(stdout='^https://test.s3.amazonaws.com/file\n$'):
            self.parse_args('stor url s3://test/file')
        with self.assertRaisesRegexp(AssertionError, 'stdout'):
            with self.assertOutputMatches(stdout='mystdout'):
                self.parse_args('stor url s3://test/file')


class TestCat(BaseCliTest):
    @mock.patch.object(S3Path, 'read_object', autospec=True)
    def test_cat_s3(self, mock_read):
        mock_read.return_value = b'hello world\n'
        self.parse_args('stor cat s3://test/file')
        self.assertEquals(sys.stdout.getvalue(), 'hello world\n')
        mock_read.assert_called_once_with(S3Path('s3://test/file'))


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

    @mock.patch.object(S3Path, 'isdir', return_value=True, autospec=True)
    def test_cd_s3(self, mock_isdir):
        self.parse_args('stor cd s3://test')
        self.assertIn(self.generate_env_text(s3_path='s3://test'),
                      open(self.test_env_file).read())

    @mock.patch.object(S3Path, 'isdir', return_value=False, autospec=True)
    def test_cd_not_dir_s3(self, mock_isdir):
        with self.assertOutputMatches(exit_status=1, stderr='not a directory'):
            self.parse_args('stor cd s3://test/file')

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