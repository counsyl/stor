from __future__ import print_function

import contextlib
import os
import mock
import sys
import unittest
from tempfile import NamedTemporaryFile

import six

from stor.posix import PosixPath
from stor import cli
from stor import settings


class BaseCliTest(unittest.TestCase):
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

    def test_stdout_matching(self):
        with six.assertRaisesRegex(self, AssertionError, 'stdout'):
            with self.assertOutputMatches(stdout=None):
                print('blah')

    def test_stderr_matching(self):
        with six.assertRaisesRegex(self, AssertionError, 'stderr'):
            with self.assertOutputMatches(stderr=None):
                print('blah', file=sys.stderr)

    def test_stderr_and_stdout_matching(self):
        with six.assertRaisesRegex(self, AssertionError, 'stderr'):
            with self.assertOutputMatches(stdout='apple', stderr=None):
                print('apple')
                print('blah', file=sys.stderr)


class TestCliBasics(BaseCliTest):
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
                'wait_on_close': 0
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


@mock.patch('stor.copy', autospec=True)
class TestCopy(BaseCliTest):
    def mock_copy_source(self, source, *args, **kwargs):
        dest = kwargs.pop('dest')
        with open(dest, 'w') as outfile, open(source) as infile:
            outfile.write(infile.read())

    @mock.patch('sys.stdin', new=six.StringIO('some stdin input\n'))
    def test_copy_stdin(self, mock_copy):
        mock_copy.side_effect = self.mock_copy_source
        with NamedTemporaryFile(delete=False) as ntf:
            test_file = ntf.name
            self.parse_args('stor cp - %s' % test_file)
        self.assertEquals(open(test_file).read(), 'some stdin input\n')
        os.remove(test_file)
        self.assertFalse(os.path.exists(test_file))


class TestWalkfiles(BaseCliTest):
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
    def test_file_uri_error(self):
        with self.assertOutputMatches(exit_status='1', stderr='must be swift or s3 path'):
            self.parse_args('stor url /test/file')


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
