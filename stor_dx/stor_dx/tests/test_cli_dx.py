from __future__ import print_function

import mock
import sys

import six

from stor import cli
from stor_dx import DXPath
from stor_dx import test


class BaseCliTest(test.DXTestCase):
    def setUp(self):
        patcher = mock.patch.object(sys, 'stdout', six.StringIO())
        self.addCleanup(patcher.stop)
        patcher.start()

    def parse_args(self, args):
        with mock.patch.object(sys, 'argv', args.split()):
            cli.main()


class TestList(BaseCliTest):
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
