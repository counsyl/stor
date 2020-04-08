from __future__ import print_function

import contextlib
import io
import os
from unittest import mock
import sys
from tempfile import NamedTemporaryFile

import pytest

from stor.dx import DXPath
from stor.posix import PosixPath
from stor.s3 import S3Path
from stor.swift import SwiftPath
from stor import cli
from stor import exceptions
from stor import settings
from stor import test


class BaseCliTest(test.S3TestCase, test.SwiftTestCase):
    def setUp(self):
        patcher = mock.patch.object(sys, 'stdout', io.StringIO())
        self.addCleanup(patcher.stop)
        patcher.start()

    @contextlib.contextmanager
    def assertOutputMatches(self, exit_status=None, stdout='', stderr=''):
        patch = mock.patch('sys.stderr', new=io.StringIO())
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
        with pytest.raises(AssertionError, match='SystemExit'):
            with self.assertOutputMatches(exit_status='1'):
                pass

    def test_stdout_matching(self):
        with pytest.raises(AssertionError, match='stdout'):
            with self.assertOutputMatches(stdout=None):
                print('blah')

    def test_stderr_matching(self):
        with pytest.raises(AssertionError, match='stderr'):
            with self.assertOutputMatches(stderr=None):
                print('blah', file=sys.stderr)

    def test_stderr_and_stdout_matching(self):
        with pytest.raises(AssertionError, match='stderr'):
            with self.assertOutputMatches(stdout='apple', stderr=None):
                print('apple')
                print('blah', file=sys.stderr)


class TestCliBasics(BaseCliTest):
    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_cli_error(self, mock_list):
        mock_list.side_effect = exceptions.RemoteError('some error')
        with self.assertOutputMatches(exit_status='1', stderr='RemoteError: some error'):
            self.parse_args('stor list s3://bucket')

    @mock.patch.dict('stor.settings._global_settings', {}, clear=True)
    @mock.patch.dict(os.environ, {}, clear=True)
    @mock.patch('stor.copytree', autospec=True)
    @mock.patch('stor.settings.USER_CONFIG_FILE', '')
    def test_cli_config(self, mock_copytree):
        expected_settings = {
            'stor': {},
            's3': {
                'aws_access_key_id': '',
                'aws_secret_access_key': '',
                'aws_session_token': '',
                'profile_name': '',
                'region_name': ''
            },
            's3:upload': {
                'segment_size': 8388608,
                'object_threads': 10,
                'segment_threads': 10
            },
            's3:download': {
                'segment_size': 8388608,
                'object_threads': 10,
                'segment_threads': 10
            },
            'swift': {
                'username': 'fake_user',
                'password': 'fake_password',
                'auth_url': '',
                'temp_url_key': '',
                'num_retries': 0
            },
            'swift:delete': {
                'object_threads': 10
            },
            'swift:download': {
                'container_threads': 10,
                'object_threads': 10,
                'shuffle': True,
                'skip_identical': True
            },
            'swift:upload': {
                'changed': False,
                'checksum': True,
                'leave_segments': True,
                'object_threads': 10,
                'segment_size': 1073741824,
                'segment_threads': 10,
                'skip_identical': False,
                'use_slo': True
            },
            'dx': {
                'auth_token': 'fake_token',
                'wait_on_close': 0,
                'file_proxy_url': ''
            }
        }
        filename = os.path.join(os.path.dirname(__file__), 'file_data', 'test.cfg')
        self.parse_args('stor --config %s cp -r source dest' % filename)
        self.assertEquals(settings._global_settings, expected_settings)

    @mock.patch('stor.copy', autospec=True)
    def test_not_implemented_error(self, mock_copy):
        mock_copy.side_effect = NotImplementedError
        with self.assertOutputMatches(exit_status='1', stderr='not a valid command'):
            self.parse_args('stor cp some/path some/where')

    @mock.patch('stor.cli._clear_env', autospec=True)
    def test_not_implemented_error_no_args(self, mock_clear):
        mock_clear.side_effect = NotImplementedError
        with self.assertOutputMatches(exit_status='1', stderr='not a valid command'):
            self.parse_args('stor clear')

    @mock.patch.object(PosixPath, 'list', autospec=True)
    def test_not_implemented_error_path(self, mock_list):
        mock_list.side_effect = NotImplementedError
        with self.assertOutputMatches(exit_status='1', stderr='not a valid command'):
            self.parse_args('stor list some_path')

    def test_no_cmd_provided(self):
        with self.assertOutputMatches(exit_status='2', stderr='stor: error:.*arguments'):
            with mock.patch.object(sys, 'argv', ['stor']):
                cli.main()


@mock.patch('stor.cli._get_pwd', autospec=True)
class TestGetPath(BaseCliTest):
    def test_relpath_no_s3(self, mock_pwd):
        mock_pwd.return_value = 's3://'
        with self.assertRaisesRegexp(ValueError, 'relative path'):
            cli.get_path('s3:../test')

    def test_relpath_no_swift(self, mock_pwd):
        mock_pwd.return_value = 'swift://'
        with self.assertRaisesRegexp(ValueError, 'relative path'):
            cli.get_path('swift:../test')

    def test_relpath_empty_s3(self, mock_pwd):
        with self.assertRaisesRegexp(ValueError, 'invalid'):
            cli.get_path('s3:')

    def test_relpath_empty_swift(self, mock_pwd):
        with self.assertRaisesRegexp(ValueError, 'invalid'):
            cli.get_path('swift:')

    def test_relpath_current_s3(self, mock_pwd):
        mock_pwd.return_value = 's3://test'
        self.assertEquals(cli.get_path('s3:.'), S3Path('s3://test/'))
        mock_pwd.return_value = 's3://test/'
        self.assertEquals(cli.get_path('s3:.'), S3Path('s3://test/'))

    def test_relpath_current_swift(self, mock_pwd):
        mock_pwd.return_value = 'swift://test'
        self.assertEquals(cli.get_path('swift:.'), SwiftPath('swift://test/'))
        mock_pwd.return_value = 'swift://test/'
        self.assertEquals(cli.get_path('swift:.'), SwiftPath('swift://test/'))

    def test_relpath_current_subdir_s3(self, mock_pwd):
        mock_pwd.return_value = 's3://test'
        self.assertEquals(cli.get_path('s3:./b/c'), S3Path('s3://test/b/c'))
        mock_pwd.return_value = 's3://test/'
        self.assertEquals(cli.get_path('s3:./b/c'), S3Path('s3://test/b/c'))

    def test_relpath_current_subdir_swift(self, mock_pwd):
        mock_pwd.return_value = 'swift://test/cont'
        self.assertEquals(cli.get_path('swift:./b/c'), SwiftPath('swift://test/cont/b/c'))
        mock_pwd.return_value = 'swift://test/cont/'
        self.assertEquals(cli.get_path('swift:./b/c'), SwiftPath('swift://test/cont/b/c'))

    def test_relpath_current_subdir_no_dot_s3(self, mock_pwd):
        mock_pwd.return_value = 's3://test'
        self.assertEquals(cli.get_path('s3:b/c'), S3Path('s3://test/b/c'))
        mock_pwd.return_value = 's3://test/'
        self.assertEquals(cli.get_path('s3:b/c'), S3Path('s3://test/b/c'))

    def test_relpath_current_subdir_no_dot_swift(self, mock_pwd):
        mock_pwd.return_value = 'swift://test/cont'
        self.assertEquals(cli.get_path('swift:b/c'), SwiftPath('swift://test/cont/b/c'))
        mock_pwd.return_value = 'swift://test/cont/'
        self.assertEquals(cli.get_path('swift:b/c'), SwiftPath('swift://test/cont/b/c'))

    def test_relpath_parent_s3(self, mock_pwd):
        mock_pwd.return_value = 's3://test/dir'
        self.assertEquals(cli.get_path('s3:..'), S3Path('s3://test/'))
        mock_pwd.return_value = 's3://test/dir/'
        self.assertEquals(cli.get_path('s3:..'), S3Path('s3://test/'))

    def test_relpath_parent_swift(self, mock_pwd):
        mock_pwd.return_value = 'swift://test/dir'
        self.assertEquals(cli.get_path('swift:..'), SwiftPath('swift://test/'))
        mock_pwd.return_value = 'swift://test/dir/'
        self.assertEquals(cli.get_path('swift:..'), SwiftPath('swift://test/'))

    def test_relpath_parent_subdir_s3(self, mock_pwd):
        mock_pwd.return_value = 's3://test/dir'
        self.assertEquals(cli.get_path('s3:../b/c'), S3Path('s3://test/b/c'))
        mock_pwd.return_value = 's3://test/dir/'
        self.assertEquals(cli.get_path('s3:../b/c'), S3Path('s3://test/b/c'))

    def test_relpath_parent_subdir_swift(self, mock_pwd):
        mock_pwd.return_value = 'swift://test/cont/dir'
        self.assertEquals(cli.get_path('swift:../b/c'), SwiftPath('swift://test/cont/b/c'))
        mock_pwd.return_value = 'swift://test/cont/dir/'
        self.assertEquals(cli.get_path('swift:../b/c'), SwiftPath('swift://test/cont/b/c'))

    def test_relpath_no_parent_s3(self, mock_pwd):
        mock_pwd.return_value = 's3://test'
        with self.assertRaisesRegexp(ValueError, 'Relative path.*invalid'):
            cli.get_path('s3:../b/c')
        mock_pwd.return_value = 's3://test/'
        with self.assertRaisesRegexp(ValueError, 'Relative path.*invalid'):
            cli.get_path('s3:../b/c')

    def test_relpath_no_parent_swift(self, mock_pwd):
        mock_pwd.return_value = 'swift://test'
        with self.assertRaisesRegexp(ValueError, 'Relative path.*invalid'):
            cli.get_path('swift:../b/c')
        mock_pwd.return_value = 'swift://test/'
        with self.assertRaisesRegexp(ValueError, 'Relative path.*invalid'):
            cli.get_path('swift:../b/c')

    def test_relpath_nested_parent(self, mock_pwd):
        mock_pwd.return_value = 's3://a/b/c'
        self.assertEquals(cli.get_path('s3:../../d'), S3Path('s3://a/d'))
        mock_pwd.return_value = 'swift://a/b/c/'
        self.assertEquals(cli.get_path('swift:../../d'), SwiftPath('swift://a/d'))

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

    def test_invalid_abspath_swift(self, mock_pwd):
        with self.assertRaisesRegexp(ValueError, 'invalid path'):
            cli.get_path('swift:/some/path')


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

    @mock.patch.object(DXPath, 'walkfiles', autospec=True)
    def test_list_dx(self, mock_list):
        mock_list.return_value = [
            DXPath('dx://t:/c/file1'),
            DXPath('dx://t:/c/dir/file2'),
            DXPath('dx://t:/c/file3')
        ]
        self.parse_args('stor list dx://t:/c/')
        self.assertEquals(sys.stdout.getvalue(),
                          'dx://t:/c/file1\n'
                          'dx://t:/c/dir/file2\n'
                          'dx://t:/c/file3\n')
        mock_list.assert_called_once_with(DXPath('dx://t:/c/'))

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

        self.parse_args('stor list s3://some-bucket -s dir -l2 --canonicalize')
        self.assertEquals(sys.stdout.getvalue(),
                          's3://some-bucket/dir/a\n'
                          's3://some-bucket/dir/b\n'
                          's3://some-bucket/dir/c/d\n')

        mock_list.assert_has_calls([
            mock.call(S3Path('s3://some-bucket'), starts_with='dir', limit=2, canonicalize=True)
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

    @mock.patch.object(SwiftPath, 'listdir', autospec=True)
    def test_listdir_swift_options(self, mock_listdir):
        mock_listdir.return_value = [
            SwiftPath('swift://t/c/file1'),
            SwiftPath('swift://t/c/dir/'),
            SwiftPath('swift://t/c/file3')
        ]
        self.parse_args('stor ls swift://t/c --canonicalize')
        self.assertEquals(sys.stdout.getvalue(),
                          'swift://t/c/file1\n'
                          'swift://t/c/dir/\n'
                          'swift://t/c/file3\n')
        mock_listdir.assert_called_once_with(SwiftPath('swift://t/c'), canonicalize=True)


@mock.patch('stor.copy', autospec=True)
class TestCopy(BaseCliTest):
    def mock_copy_source(self, source, dest, *args, **kwargs):
        with open(dest, 'w') as outfile, open(source) as infile:
            outfile.write(infile.read())

    def test_copy(self, mock_copy):
        self.parse_args('stor cp s3://bucket/file.txt ./file1')
        mock_copy.assert_called_once_with(source='s3://bucket/file.txt', dest='./file1')

    @mock.patch('sys.stdin', new=io.StringIO('some stdin input\n'))
    def test_copy_stdin(self, mock_copy):
        mock_copy.side_effect = self.mock_copy_source
        with NamedTemporaryFile(delete=False) as ntf:
            test_file = ntf.name
            self.parse_args('stor cp - %s' % test_file)
        self.assertEquals(open(test_file).read(), 'some stdin input\n')
        temp_file = mock_copy.call_args_list[0][1]['source']
        self.assertFalse(os.path.exists(temp_file))
        os.remove(test_file)
        self.assertFalse(os.path.exists(test_file))


@mock.patch('stor.copytree', autospec=True)
class TestCopytree(BaseCliTest):
    def test_copytree(self, mock_copytree):
        self.parse_args('stor cp -r s3://bucket .')
        mock_copytree.assert_called_once_with(source='s3://bucket', dest='.')

    def test_copytree_stdin_error(self, mock_copytree):
        with self.assertOutputMatches(exit_status='2', stderr='- cannot be used with -r'):
            self.parse_args('stor cp -r - s3://bucket')


class TestRemove(BaseCliTest):
    @mock.patch.object(S3Path, 'remove', autospec=True)
    def test_remove_s3(self, mock_remove):
        self.parse_args('stor rm s3://bucket/file1')
        mock_remove.assert_called_once_with(S3Path('s3://bucket/file1'))

    @mock.patch.object(SwiftPath, 'remove', autospec=True)
    def test_remove_swift(self, mock_remove):
        self.parse_args('stor rm swift://t/c/file1')
        mock_remove.assert_called_once_with(SwiftPath('swift://t/c/file1'))

    @mock.patch.object(S3Path, 'rmtree', autospec=True)
    def test_rmtree_s3(self, mock_rmtree):
        self.parse_args('stor rm -r s3://bucket/dir')
        mock_rmtree.assert_called_once_with(S3Path('s3://bucket/dir'))

    @mock.patch.object(SwiftPath, 'rmtree', autospec=True)
    def test_rmtree_swift(self, mock_rmtree):
        self.parse_args('stor rm -r swift://t/c/dir')
        mock_rmtree.assert_called_once_with(SwiftPath('swift://t/c/dir'))


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

    @mock.patch.object(PosixPath, 'walkfiles', autospec=True)
    def test_walkfiles_posix(self, mock_walkfiles):
        mock_walkfiles.return_value = [
            './a/b.txt',
            './c.txt',
            './d.txt'
        ]
        self.parse_args('stor walkfiles -p=*.txt .')
        self.assertEquals(sys.stdout.getvalue(),
                          './a/b.txt\n'
                          './c.txt\n'
                          './d.txt\n')
        mock_walkfiles.assert_called_once_with(PosixPath('.'), pattern='*.txt')

    @mock.patch.object(PosixPath, 'walkfiles', autospec=True)
    def test_walkfiles_posix_options(self, mock_walkfiles):
        mock_walkfiles.return_value = [
            './a/b.txt',
            './c.txt',
            './d.txt'
        ]
        self.parse_args('stor walkfiles -p=*.txt . --canonicalize')
        self.assertEquals(sys.stdout.getvalue(),
                          './a/b.txt\n'
                          './c.txt\n'
                          './d.txt\n')
        mock_walkfiles.assert_called_once_with(PosixPath('.'), pattern='*.txt', canonicalize=True)

    @mock.patch.object(PosixPath, 'walkfiles', autospec=True)
    def test_walkfiles_no_pattern(self, mock_walkfiles):
        mock_walkfiles.return_value = [
            './a/b.txt',
            './c.txt',
            './d.txt',
            './file'
        ]
        self.parse_args('stor walkfiles .')
        self.assertEquals(sys.stdout.getvalue(),
                          './a/b.txt\n'
                          './c.txt\n'
                          './d.txt\n'
                          './file\n')
        mock_walkfiles.assert_called_once_with(PosixPath('.'))


class TestToUri(BaseCliTest):
    def test_to_url(self):
        with self.assertOutputMatches(stdout='^https://test.s3.amazonaws.com/file\n$'):
            self.parse_args('stor url s3://test/file')

    def test_file_uri_error(self):
        with self.assertOutputMatches(exit_status='1', stderr='must be swift or s3 path'):
            self.parse_args('stor url /test/file')


class TestCat(BaseCliTest):
    @mock.patch.object(S3Path, 'read_object', autospec=True)
    def test_cat_s3(self, mock_read):
        mock_read.return_value = b'hello world\n'
        self.parse_args('stor cat s3://test/file')
        self.assertEquals(sys.stdout.getvalue(), 'hello world\n')
        mock_read.assert_called_once_with(S3Path('s3://test/file'))

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

    @mock.patch.object(S3Path, 'isdir', return_value=True, autospec=True)
    def test_cd_s3(self, mock_isdir):
        self.parse_args('stor cd s3://test')
        self.assertIn(self.generate_env_text(s3_path='s3://test'),
                      open(self.test_env_file).read())

    @mock.patch.object(SwiftPath, 'isdir', return_value=True, autospec=True)
    def test_cd_swift(self, mock_isdir):
        self.parse_args('stor cd swift://test/container')
        self.assertIn(self.generate_env_text(swift_path='swift://test/container'),
                      open(self.test_env_file).read())

    @mock.patch.object(S3Path, 'isdir', return_value=False, autospec=True)
    def test_cd_not_dir_s3(self, mock_isdir):
        with self.assertOutputMatches(exit_status=1, stderr='not a directory'):
            self.parse_args('stor cd s3://test/file')

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


class TestCompletions(BaseCliTest):
    def test_completion(self):
        self.parse_args('stor completions')
        assert '_stor_complete' in sys.stdout.getvalue()
        assert '_get_comp_words_by_ref' in sys.stdout.getvalue()
