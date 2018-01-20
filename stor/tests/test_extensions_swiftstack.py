import unittest
import sys

import mock
import six

from stor import cli
from stor.extensions import swiftstack


@mock.patch('sys.stdout', new=six.StringIO())
class SwiftStackExtensions(unittest.TestCase):
    swift_path = "swift://AUTH_seq_upload_prod/D00576"
    s3_path = "s3://ex-bucket/a9bf76/AUTH_seq_upload_prod/D00576"

    def test_cli(self):
        with mock.patch.object(
            sys, 'argv',
            ['stor', 'convert-swiftstack', self.swift_path, '--bucket', 'ex-bucket']
        ):
            cli.main()
        self.assertEqual(sys.stdout.getvalue(), self.s3_path + '\n')

        with self.assertRaisesRegexp(TypeError, 'bucket is required'):
            with mock.patch.object(
                sys, 'argv',
                ['stor', 'convert-swiftstack', self.swift_path]
            ):
                cli.main()

    def test_swift_to_s3(self):
        assert str(swiftstack.swift_to_s3(self.swift_path, "ex-bucket")) == self.s3_path

    def test_s3_to_swift(self):
        assert str(swiftstack.s3_to_swift(self.s3_path)) == self.swift_path
        assert cli._convert_swiftstack(self.s3_path, None) == self.swift_path
