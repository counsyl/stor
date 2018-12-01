import os
import mock
import tempfile
import unittest

import stor
from stor import Path
from stor import posix
from stor import settings
from stor_swift import swift


class TestDiv(unittest.TestCase):
    def test_w_swift_component(self):
        p = posix.PosixPath('my/path') / swift.SwiftPath('swift://t/c/name').name
        self.assertEquals(p, posix.PosixPath('my/path/name'))
        self.assertEquals(stor.join('my/path',
                                    swift.SwiftPath('swift://t/c/name').name),
                          p)


class TestAdd(unittest.TestCase):
    def test_w_swift_component(self):
        p = posix.PosixPath('my/path') + swift.SwiftPath('swift://t/c/name').name
        self.assertEquals(p, posix.PosixPath('my/pathname'))


class TestCopy(unittest.TestCase):
    @mock.patch.object(swift.SwiftPath, 'upload', autospec=True)
    def test_swift_destination(self, mock_upload):
        dest = Path('swift://tenant/container/file.txt')
        with tempfile.NamedTemporaryFile() as tmp_f:
            Path(tmp_f.name).copy(dest)
            upload_args = mock_upload.call_args_list[0][0]
            self.assertEquals(upload_args[0], dest.parent)
            self.assertEquals(upload_args[1][0].source, tmp_f.name)
            self.assertEquals(upload_args[1][0].object_name, 'file.txt')

    def test_ambigious_swift_resource_destination(self):
        with stor.NamedTemporaryDirectory() as tmp_d:
            source = tmp_d / '1'
            with open(source, 'w') as tmp_file:
                tmp_file.write('1')

            dest = 'swift://tenant/container/ambiguous-resource'
            with self.assertRaisesRegexp(ValueError, 'Swift destination'):
                stor.copy(source, dest)

    def test_ambigious_swift_container_destination(self):
        with stor.NamedTemporaryDirectory() as tmp_d:
            source = tmp_d / '1'
            with open(source, 'w') as tmp_file:
                tmp_file.write('1')

            dest = 'swift://tenant/ambiguous-container'
            with self.assertRaisesRegexp(ValueError, 'Swift destination'):
                stor.copy(source, dest)

    def test_tenant_swift_destination(self):
        with stor.NamedTemporaryDirectory() as tmp_d:
            source = tmp_d / 'source'
            os.mkdir(source)
            with open(source / '1.txt', 'w') as tmp_file:
                tmp_file.write('1')

            dest = 'swift://tenant/'
            with self.assertRaisesRegexp(ValueError, 'copy to tenant'):
                stor.copy(source / '1.txt', dest)


class TestCopytree(unittest.TestCase):
    @mock.patch.object(swift.SwiftPath, 'upload', autospec=True)
    def test_swift_destination(self, mock_upload):
        source = '.'
        dest = Path('swift://tenant/container')
        options = {
            'swift:upload': {
                'object_threads': 30,
                'segment_threads': 40
            }
        }

        with settings.use(options):
            stor.copytree(source, dest)
        mock_upload.assert_called_once_with(
            dest,
            ['.'],
            condition=None,
            use_manifest=False,
            headers=None)
