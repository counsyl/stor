import mock
import os
import storage_utils
from storage_utils import swift
from storage_utils import utils
import subprocess
import tempfile
import unittest


class TestCopytree(unittest.TestCase):
    def test_posix_destination(self):
        with storage_utils.NamedTemporaryDirectory() as tmp_d:
            source = tmp_d / 'source'
            os.mkdir(source)
            with open(source / '1', 'w') as tmp_file:
                tmp_file.write('1')

            dest = tmp_d / 'my' / 'dest'
            utils.copytree(source, dest)
            self.assertTrue((dest / '1').exists())

    def test_posix_destination_w_error(self):
        with storage_utils.NamedTemporaryDirectory() as tmp_d:
            invalid_source = tmp_d / 'source'
            dest = tmp_d / 'my' / 'dest'

            with self.assertRaises(subprocess.CalledProcessError):
                utils.copytree(invalid_source, dest)

    @mock.patch.object(swift.SwiftPath, 'upload', autospec=True)
    def test_swift_destination(self, mock_upload):
        source = '.'
        dest = storage_utils.path('swift://tenant/container')
        utils.copytree(source, dest, object_threads=30, segment_threads=40)
        mock_upload.assert_called_once_with(
            dest,
            ['.'],
            object_threads=30,
            segment_threads=40)


class TestOpen(unittest.TestCase):
    def test_open_works_w_swift_params(self):
        with tempfile.NamedTemporaryFile() as f:
            p = storage_utils.path(f.name).open(swift_upload_kwargs={
                'use_slo': True
            })
            p.close()

    def test_open_works_wo_swift_params(self):
        with tempfile.NamedTemporaryFile() as f:
            p = storage_utils.path(f.name).open()
            p.close()
