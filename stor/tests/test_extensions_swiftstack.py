import sys

from stor import cli
from stor.tests.test_cli import BaseCliTest
from stor.extensions import swiftstack


class TestSwiftStackExtensions(BaseCliTest):
    swift_path = 'swift://AUTH_seq_upload_prod/D00576'
    s3_path = 's3://ex-bucket/a9bf76/AUTH_seq_upload_prod/D00576'

    def test_s3_to_swift_cli(self):
        self.parse_args('stor convert-swiftstack %s --bucket ex-bucket' % self.swift_path)
        self.assertEqual(sys.stdout.getvalue(), self.s3_path + '\n')

    def test_s3_to_swift_no_bucket_errors(self):
        with self.assertOutputMatches(exit_status='1', stderr='bucket'):
            self.parse_args('stor convert-swiftstack %s' % self.swift_path)

        with self.assertRaisesRegexp(TypeError, 'bucket is required'):
            swiftstack.swift_to_s3(self.swift_path, None)

    def test_filesystem_path_errors(self):
        with self.assertOutputMatches(exit_status='1', stderr="invalid path.* '/test/path'"):
            self.parse_args('stor convert-swiftstack /test/path')

    def test_swift_to_s3(self):
        assert str(swiftstack.swift_to_s3(self.swift_path, 'ex-bucket')) == self.s3_path
        assert (str(swiftstack.swift_to_s3(self.swift_path + '/some/object', 'ex-bucket')) ==
                self.s3_path + '/some/object')

    def test_s3_to_swift(self):
        assert str(swiftstack.s3_to_swift(self.s3_path)) == self.swift_path
        assert cli._convert_swiftstack(self.s3_path, None) == self.swift_path
