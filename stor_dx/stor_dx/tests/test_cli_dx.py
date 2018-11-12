from __future__ import print_function

import contextlib
import mock
import sys

import six

from stor_dx.dx import DXPath
from stor import cli
from stor_dx import test


class BaseCliTest(test.DXTestCase):
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
