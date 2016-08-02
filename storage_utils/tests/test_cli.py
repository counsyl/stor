from cStringIO import StringIO
import os
import mock
import sys
from tempfile import NamedTemporaryFile
from storage_utils.posix import PosixPath
from storage_utils.experimental.s3 import S3Path
from storage_utils.swift import SwiftPath
from storage_utils import cli
from storage_utils import exceptions
from storage_utils import settings
from storage_utils import test


@mock.patch('sys.stdout', new=StringIO())
class BaseCliTest(test.S3TestCase, test.SwiftTestCase):
    def parse_args(self, args):
        with mock.patch.object(sys, 'argv', args.split()):
            cli.main()


class TestCliBasics(BaseCliTest):
    @mock.patch('sys.stderr', autospec=True)
    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_cli_error(self, mock_list, mock_stderr):
        mock_list.side_effect = exceptions.RemoteError('some error')
        with self.assertRaisesRegexp(SystemExit, '1'):
            self.parse_args('stor list s3://bucket')

    @mock.patch.dict('storage_utils.settings._global_settings', {}, clear=True)
    @mock.patch('storage_utils.copytree', autospec=True)
    def test_cli_config(self, mock_copytree):
        expected_settings = {
            'stor': {
                'str_val': 'this is a string'
            },
            'something': {
                'just': 'another value'
            },
            'swift': {
                'num_retries': 5,
                'fake_secret_key': '7jsdf0983j""SP{}?//'
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
            }
        }
        filename = os.path.join(os.path.dirname(__file__), 'file_data', 'test.cfg')
        self.parse_args('stor --config %s copytree source dest' % filename)
        self.assertEquals(settings._global_settings, expected_settings)

    @mock.patch('storage_utils.copy', autospec=True)
    @mock.patch('sys.stderr', new=StringIO())
    def test_not_implemented_error(self, mock_copy):
        mock_copy.side_effect = NotImplementedError
        with self.assertRaisesRegexp(SystemExit, '1'):
            self.parse_args('stor copy some/path some/where')
        self.assertIn('not a valid command', sys.stderr.getvalue())

    @mock.patch('storage_utils.cli._clear_env', autospec=True)
    @mock.patch('sys.stderr', new=StringIO())
    def test_not_implemented_error_no_args(self, mock_clear):
        mock_clear.side_effect = NotImplementedError
        with self.assertRaisesRegexp(SystemExit, '1'):
            self.parse_args('stor clear')
        self.assertIn('not a valid command', sys.stderr.getvalue())

    @mock.patch.object(PosixPath, 'list', autospec=True)
    @mock.patch('sys.stderr', new=StringIO())
    def test_not_implemented_error_path(self, mock_list):
        mock_list.side_effect = NotImplementedError
        with self.assertRaisesRegexp(SystemExit, '1'):
            self.parse_args('stor list some_path')
        self.assertIn('not a valid command', sys.stderr.getvalue())


@mock.patch('storage_utils.cli._get_pwd', autospec=True)
class TestGetPath(BaseCliTest):
    def test_relpath_no_s3(self, mock_pwd):
        mock_pwd.return_value = 's3://'
        with self.assertRaisesRegexp(ValueError, 'relative path'):
            cli.get_path('s3://../test')

    def test_relpath_no_swift(self, mock_pwd):
        mock_pwd.return_value = 'swift://'
        with self.assertRaisesRegexp(ValueError, 'relative path'):
            cli.get_path('swift://../test')

    def test_relpath_current_s3(self, mock_pwd):
        mock_pwd.return_value = 's3://test'
        self.assertEquals(cli.get_path('s3://./b/c'), S3Path('s3://test/b/c'))
        mock_pwd.return_value = 's3://test/'
        self.assertEquals(cli.get_path('s3://./b/c'), S3Path('s3://test/b/c'))

    def test_relpath_current_swift(self, mock_pwd):
        mock_pwd.return_value = 'swift://test/cont'
        self.assertEquals(cli.get_path('swift://./b/c'), SwiftPath('swift://test/cont/b/c'))
        mock_pwd.return_value = 'swift://test/cont/'
        self.assertEquals(cli.get_path('swift://./b/c'), SwiftPath('swift://test/cont/b/c'))

    def test_relpath_parent_s3(self, mock_pwd):
        mock_pwd.return_value = 's3://test/dir'
        self.assertEquals(cli.get_path('s3://../b/c'), S3Path('s3://test/b/c'))
        mock_pwd.return_value = 's3://test/dir/'
        self.assertEquals(cli.get_path('s3://../b/c'), S3Path('s3://test/b/c'))

    def test_relpath_parent_swift(self, mock_pwd):
        mock_pwd.return_value = 'swift://test/cont/dir'
        self.assertEquals(cli.get_path('swift://../b/c'), SwiftPath('swift://test/cont/b/c'))
        mock_pwd.return_value = 'swift://test/cont/dir/'
        self.assertEquals(cli.get_path('swift://../b/c'), SwiftPath('swift://test/cont/b/c'))

    def test_relpath_no_parent_s3(self, mock_pwd):
        mock_pwd.return_value = 's3://test'
        with self.assertRaisesRegexp(ValueError, 'Relative path.*invalid'):
            cli.get_path('s3://../b/c')
        mock_pwd.return_value = 's3://test/'
        with self.assertRaisesRegexp(ValueError, 'Relative path.*invalid'):
            cli.get_path('s3://../b/c')

    def test_relpath_no_parent_swift(self, mock_pwd):
        mock_pwd.return_value = 'swift://test'
        with self.assertRaisesRegexp(ValueError, 'Relative path.*invalid'):
            cli.get_path('swift://../b/c')
        mock_pwd.return_value = 'swift://test/'
        with self.assertRaisesRegexp(ValueError, 'Relative path.*invalid'):
            cli.get_path('swift://../b/c')


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

        self.parse_args('stor list s3://some-bucket -us dir')
        self.assertEquals(sys.stdout.getvalue(),
                          's3://some-bucket/dir/a\n'
                          's3://some-bucket/dir/b\n'
                          's3://some-bucket/dir/c/d\n')

        # clear stdout
        sys.stdout = StringIO()

        self.parse_args('stor list s3://some-bucket -l2')
        self.assertEquals(sys.stdout.getvalue(),
                          's3://some-bucket/a\n'
                          's3://some-bucket/b\n')

        mock_list.assert_has_calls([
            mock.call(S3Path('s3://some-bucket'), starts_with='dir', use_manifest=True),
            mock.call(S3Path('s3://some-bucket'), limit=2)
        ])


class TestListdir(BaseCliTest):
    @mock.patch.object(S3Path, 'listdir', autospec=True)
    def test_listdir_s3(self, mock_listdir):
        mock_listdir.return_value = [
            S3Path('s3://bucket/file1'),
            S3Path('s3://bucket/file2'),
            S3Path('s3://bucket/dir/')
        ]
        self.parse_args('stor listdir s3://bucket')
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
        self.parse_args('stor listdir swift://t/c')
        self.assertEquals(sys.stdout.getvalue(),
                          'swift://t/c/file1\n'
                          'swift://t/c/dir/\n'
                          'swift://t/c/file3\n')
        mock_listdir.assert_called_once_with(SwiftPath('swift://t/c'))

    @mock.patch.object(S3Path, 'listdir', autospec=True)
    def test_ls(self, mock_listdir):
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


class TestCopytree(BaseCliTest):
    @mock.patch('storage_utils.copytree', autospec=True)
    def test_copytree(self, mock_copytree):
        self.parse_args('stor copytree s3://bucket .')
        mock_copytree.assert_called_once_with(source='s3://bucket', dest='.')


@mock.patch('storage_utils.copy', autospec=True)
class TestCopy(BaseCliTest):
    def mock_copy_source(self, source, dest, *args, **kwargs):
        with open(dest, 'w') as outfile, open(source) as infile:
            outfile.write(infile.read())

    def test_copy(self, mock_copy):
        self.parse_args('stor copy s3://bucket/file.txt ./file1')
        mock_copy.assert_called_once_with(source='s3://bucket/file.txt', dest='./file1')

    def test_cp(self, mock_copy):
        self.parse_args('stor cp ./file1 swift://a/b/c')
        mock_copy.assert_called_once_with(source='./file1', dest='swift://a/b/c')

    @mock.patch('sys.stdin', new=StringIO('some stdin input\n'))
    def test_copy_stdin(self, mock_copy):
        mock_copy.side_effect = self.mock_copy_source
        with NamedTemporaryFile(delete=False) as ntf:
            test_file = ntf.name
            self.parse_args('stor copy - %s' % test_file)
        self.assertEquals(open(test_file).read(), 'some stdin input\n')
        temp_file = mock_copy.call_args_list[0][1]['source']
        self.assertFalse(os.path.exists(temp_file))
        os.remove(test_file)
        self.assertFalse(os.path.exists(test_file))


class TestRemove(BaseCliTest):
    @mock.patch.object(S3Path, 'remove', autospec=True)
    def test_remove_s3(self, mock_remove):
        self.parse_args('stor remove s3://bucket/file1')
        mock_remove.assert_called_once_with(S3Path('s3://bucket/file1'))

    @mock.patch.object(SwiftPath, 'remove', autospec=True)
    def test_remove_swift(self, mock_remove):
        self.parse_args('stor remove swift://t/c/file1')
        mock_remove.assert_called_once_with(SwiftPath('swift://t/c/file1'))

    @mock.patch.object(S3Path, 'remove', autospec=True)
    def test_rm(self, mock_remove):
        self.parse_args('stor rm s3://bucket/file1')
        mock_remove.assert_called_once_with(S3Path('s3://bucket/file1'))


class TestRmtree(BaseCliTest):
    @mock.patch.object(S3Path, 'rmtree', autospec=True)
    def test_rmtree_s3(self, mock_rmtree):
        self.parse_args('stor rmtree s3://bucket/dir')
        mock_rmtree.assert_called_once_with(S3Path('s3://bucket/dir'))

    @mock.patch.object(SwiftPath, 'rmtree', autospec=True)
    def test_rmtree_swift(self, mock_rmtree):
        self.parse_args('stor rmtree swift://t/c/dir')
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


class TestCat(BaseCliTest):
    @mock.patch.object(S3Path, 'read_object', autospec=True)
    def test_cat_s3(self, mock_read):
        mock_read.return_value = 'hello world\n'
        self.parse_args('stor cat s3://test/file')
        self.assertEquals(sys.stdout.getvalue(), 'hello world\n')
        mock_read.assert_called_once_with(S3Path('s3://test/file'))

    @mock.patch.object(SwiftPath, 'read_object', autospec=True)
    def test_cat_swift(self, mock_read):
        mock_read.return_value = 'hello world'
        self.parse_args('stor cat swift://some/test/file')
        self.assertEquals(sys.stdout.getvalue(), 'hello world\n')
        mock_read.assert_called_once_with(SwiftPath('swift://some/test/file'))


class TestCd(BaseCliTest):
    def setUp(self):
        # set up a temp file to use as env file so we don't mess up real defaults
        self.test_env_file = NamedTemporaryFile(delete=False).name
        mock_env_file_patcher = mock.patch('storage_utils.cli.ENV_FILE', self.test_env_file)
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

    def test_cd_s3(self):
        self.parse_args('stor cd s3://test')
        self.assertIn(self.generate_env_text(s3_path='s3://test'),
                      open(self.test_env_file).read())

    def test_cd_swift(self):
        self.parse_args('stor cd swift://test/container')
        self.assertIn(self.generate_env_text(swift_path='swift://test/container'),
                      open(self.test_env_file).read())

    @mock.patch('sys.stderr', new=StringIO())
    def test_cd_error(self):
        with self.assertRaisesRegexp(SystemExit, '1'):
            self.parse_args('stor cd drive://not/correct')
        self.assertIn('invalid path', sys.stderr.getvalue())

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
        self.assertEquals(sys.stdout.getvalue(), 's3://test\nswift://test/container\n')

    def test_pwd_s3(self):
        with open(self.test_env_file, 'w') as outfile:
            outfile.write(self.generate_env_text(s3_path='s3://test',
                                                 swift_path='swift://test/container'))
        self.parse_args('stor pwd s3')
        self.assertEquals(sys.stdout.getvalue(), 's3://test\n')

    def test_pwd_swift(self):
        with open(self.test_env_file, 'w') as outfile:
            outfile.write(self.generate_env_text(s3_path='s3://test',
                                                 swift_path='swift://test/container'))
        self.parse_args('stor pwd swift')
        self.assertEquals(sys.stdout.getvalue(), 'swift://test/container\n')

    @mock.patch('sys.stderr', new=StringIO())
    def test_pwd_error(self):
        with self.assertRaisesRegexp(SystemExit, '1'):
            self.parse_args('stor pwd service')
        self.assertIn('invalid service', sys.stderr.getvalue())
