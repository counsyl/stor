from cStringIO import StringIO
import os
import mock
import sys
from storage_utils.experimental.s3 import S3Path
from storage_utils.swift import SwiftPath
from storage_utils import cli
from storage_utils import exceptions
from storage_utils import settings
from storage_utils import test


class BaseCliTest(test.S3TestCase, test.SwiftTestCase):
    def parse_args(self, args):
        parser = cli.create_parser()
        results = parser.parse_args(args.split())
        return cli.process_args(results)


class TestCliBasics(BaseCliTest):
    @mock.patch('sys.stderr', autospec=True)
    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_cli_error(self, mock_list, mock_stderr):
        mock_list.side_effect = exceptions.RemoteError('some error')
        with self.assertRaisesRegexp(SystemExit, '1'):
            self.parse_args('list s3://bucket')

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
        self.parse_args('--config %s copytree source dest' % filename)
        self.assertEquals(settings._global_settings, expected_settings)

    @mock.patch.object(sys, 'argv', ['stor', 'list', 's3://a'])
    @mock.patch.object(S3Path, 'list', autospec=True)
    @mock.patch('sys.stdout', new=StringIO())
    def test_cli_main_print(self, mock_list):
        mock_list.return_value = [
            S3Path('s3://a/b/c'),
            S3Path('s3://a/file1'),
            S3Path('s3://a/file2')
        ]
        cli.main()
        self.assertEquals(sys.stdout.getvalue(),
                          's3://a/b/c\ns3://a/file1\ns3://a/file2\n')

    @mock.patch.object(sys, 'argv', ['stor', 'upload', '.', 's3://bucket'])
    @mock.patch.object(S3Path, 'upload', autospec=True)
    def test_cli_no_print(self, mock_upload):
        cli.main()
        mock_upload.assert_called_once_with(S3Path('s3://bucket'), source=['.'])


class TestList(BaseCliTest):
    @mock.patch.object(S3Path, 'list', autospec=True)
    def test_list_s3(self, mock_list):
        mock_list.return_value = [
            S3Path('s3://some-bucket/a'),
            S3Path('s3://some-bucket/b'),
            S3Path('s3://some-bucket/c/d')
        ]
        results = self.parse_args('list s3://some-bucket')
        self.assertEquals(results, [
            's3://some-bucket/a',
            's3://some-bucket/b',
            's3://some-bucket/c/d'

        ])
        mock_list.assert_called_once_with(S3Path('s3://some-bucket'))

    @mock.patch.object(SwiftPath, 'list', autospec=True)
    def test_list_swift(self, mock_list):
        mock_list.return_value = [
            SwiftPath('swift://t/c/file1'),
            SwiftPath('swift://t/c/dir/file2'),
            SwiftPath('swift://t/c/file3')
        ]
        results = self.parse_args('list swift://t/c/')
        self.assertEquals(results, [
            'swift://t/c/file1',
            'swift://t/c/dir/file2',
            'swift://t/c/file3'
        ])
        mock_list.assert_called_once_with(SwiftPath('swift://t/c/'))

    @mock.patch('sys.stderr', autospec=True)
    def test_list_bad_path(self, mock_stderr):
        with self.assertRaisesRegexp(SystemExit, '1'):
            self.parse_args('list some_path')

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

        results = self.parse_args('list s3://some-bucket -us dir')
        self.assertEquals(results, [
            's3://some-bucket/dir/a',
            's3://some-bucket/dir/b',
            's3://some-bucket/dir/c/d'

        ])

        results = self.parse_args('list s3://some-bucket -l2')
        self.assertEquals(results, [
            's3://some-bucket/a',
            's3://some-bucket/b'
        ])

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
        results = self.parse_args('listdir s3://bucket')
        self.assertEquals(results, [
            's3://bucket/file1',
            's3://bucket/file2',
            's3://bucket/dir/'
        ])
        mock_listdir.assert_called_once_with(S3Path('s3://bucket'))

    @mock.patch.object(SwiftPath, 'listdir', autospec=True)
    def test_listdir_swift(self, mock_listdir):
        mock_listdir.return_value = [
            SwiftPath('swift://t/c/file1'),
            SwiftPath('swift://t/c/dir/'),
            SwiftPath('swift://t/c/file3')
        ]
        results = self.parse_args('listdir swift://t/c')
        self.assertEquals(results, [
            'swift://t/c/file1',
            'swift://t/c/dir/',
            'swift://t/c/file3'
        ])
        mock_listdir.assert_called_once_with(SwiftPath('swift://t/c'))

    @mock.patch.object(S3Path, 'listdir', autospec=True)
    def test_ls(self, mock_listdir):
        mock_listdir.return_value = [
            S3Path('s3://bucket/file1'),
            S3Path('s3://bucket/file2'),
            S3Path('s3://bucket/dir/')
        ]
        results = self.parse_args('ls s3://bucket')
        self.assertEquals(results, [
            's3://bucket/file1',
            's3://bucket/file2',
            's3://bucket/dir/'
        ])
        mock_listdir.assert_called_once_with(S3Path('s3://bucket'))


class TestUpload(BaseCliTest):
    @mock.patch.object(S3Path, 'upload', autospec=True)
    def test_upload_s3(self, mock_upload):
        self.parse_args('upload . s3://bucket/dir')
        mock_upload.assert_called_once_with(S3Path('s3://bucket/dir'), source=['.'])

    @mock.patch.object(SwiftPath, 'upload', autospec=True)
    def test_upload_swift(self, mock_upload):
        self.parse_args('upload . swift://t/c/dir')
        mock_upload.assert_called_once_with(SwiftPath('swift://t/c/dir'), source=['.'])

    @mock.patch.object(S3Path, 'upload', autospec=True)
    def test_upload_multiple(self, mock_upload):
        self.parse_args('upload test dir file.txt s3://bucket')
        mock_upload.assert_called_once_with(S3Path('s3://bucket'),
                                            source=['test', 'dir', 'file.txt'])


class TestDownload(BaseCliTest):
    @mock.patch.object(S3Path, 'download', autospec=True)
    def test_download_s3(self, mock_download):
        self.parse_args('download s3://bucket/dir .')
        mock_download.assert_called_once_with(S3Path('s3://bucket/dir'), dest='.')

    @mock.patch.object(SwiftPath, 'download', autospec=True)
    def test_upload_swift(self, mock_download):
        self.parse_args('download swift://t/c/dir .')
        mock_download.assert_called_once_with(SwiftPath('swift://t/c/dir'), dest='.')


class TestCopytree(BaseCliTest):
    @mock.patch('storage_utils.copytree', autospec=True)
    def test_copytree(self, mock_copytree):
        self.parse_args('copytree s3://bucket .')
        mock_copytree.assert_called_once_with(source='s3://bucket', dest='.')


class TestCopy(BaseCliTest):
    @mock.patch('storage_utils.copy', autospec=True)
    def test_copy(self, mock_copy):
        self.parse_args('copy s3://bucket/file.txt ./file1')
        mock_copy.assert_called_once_with(source='s3://bucket/file.txt', dest='./file1')

    @mock.patch('storage_utils.copy', autospec=True)
    def test_cp(self, mock_copy):
        self.parse_args('cp s3://bucket/file.txt ./file1')
        mock_copy.assert_called_once_with(source='s3://bucket/file.txt', dest='./file1')


class TestRemove(BaseCliTest):
    @mock.patch.object(S3Path, 'remove', autospec=True)
    def test_remove_s3(self, mock_remove):
        self.parse_args('remove s3://bucket/file1')
        mock_remove.assert_called_once_with(S3Path('s3://bucket/file1'))

    @mock.patch.object(SwiftPath, 'remove', autospec=True)
    def test_remove_swift(self, mock_remove):
        self.parse_args('remove swift://t/c/file1')
        mock_remove.assert_called_once_with(SwiftPath('swift://t/c/file1'))

    @mock.patch.object(S3Path, 'remove', autospec=True)
    def test_rm(self, mock_remove):
        self.parse_args('rm s3://bucket/file1')
        mock_remove.assert_called_once_with(S3Path('s3://bucket/file1'))


class TestRmtree(BaseCliTest):
    @mock.patch.object(S3Path, 'rmtree', autospec=True)
    def test_rmtree_s3(self, mock_rmtree):
        self.parse_args('rmtree s3://bucket/dir')
        mock_rmtree.assert_called_once_with(S3Path('s3://bucket/dir'))

    @mock.patch.object(SwiftPath, 'rmtree', autospec=True)
    def test_rmtree_swift(self, mock_rmtree):
        self.parse_args('rmtree swift://t/c/dir')
        mock_rmtree.assert_called_once_with(SwiftPath('swift://t/c/dir'))
