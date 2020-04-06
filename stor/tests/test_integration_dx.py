import gzip
import os
import time
import unittest

import dxpy

from stor import NamedTemporaryDirectory
from stor import Path
from stor.dx import DXPath
from stor.test import DXTestCase
from stor.tests.shared import assert_same_data
from stor.tests.test_integration import BaseIntegrationTest
import stor
import stor.tests.test_integration as test_integration


class DXIntegrationTest(BaseIntegrationTest.BaseTestCases, DXTestCase):
    # we need to give enough time for file to enter `closed` state so we can read it
    DX_WAIT_SETTINGS = {'dx': {'wait_on_close': 30}}

    def setUp(self):
        self.vcr_enabled = False  # don't want vcr playback for integration tests
        super(DXIntegrationTest, self).setUp()
        if not os.environ.get('DX_AUTH_TOKEN'):
            raise unittest.SkipTest(
                'DX_AUTH_TOKEN env var not set. Skipping integration test')

        self.setup_temporary_project()
        self.project = DXPath('dx://' + self.project + ':')
        self.test_dir = self.project / 'test'

    def tearDown(self):
        super(DXIntegrationTest, self).tearDown()

    def test_copy_to_from_dir(self):
        num_test_objs = 5
        min_obj_size = 100
        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.create_dataset(tmp_d, num_test_objs, min_obj_size)
            for which_obj in self.get_dataset_obj_names(num_test_objs):
                obj_path = stor.join(self.test_dir, '%s.txt' % which_obj)
                stor.copy(which_obj, obj_path)
                file_h = dxpy.DXFile(dxid=obj_path.canonical_resource,
                                     project=obj_path.canonical_project)
                file_h.wait_on_close(20)  # wait for file to go to closed state
                stor.copy(obj_path, 'copied_file')
                self.assertCorrectObjectContents('copied_file', which_obj, min_obj_size)

    def test_copytree_to_from_dir(self):
        num_test_objs = 10
        test_obj_size = 100
        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.create_dataset(tmp_d, num_test_objs, test_obj_size)
            stor.copytree(
                '.',
                self.test_dir)
        time.sleep(10)  # for uploaded files to close
        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.test_dir.copytree(
                'test')
            # Verify contents of all downloaded test objects
            for which_obj in self.get_dataset_obj_names(num_test_objs):
                obj_path = Path('test') / which_obj
                self.assertCorrectObjectContents(obj_path, which_obj, test_obj_size)

    def test_file_read_write(self):
        self._skip_if_filesystem(self.test_dir)
        non_with_file = self.test_dir / 'nonwithfile.txt'
        test_file = self.test_dir / 'test_file.txt'
        copy_file = self.test_dir / 'copy_file.txt'

        fp = stor.open(non_with_file, mode='wb')
        # File opened in wb mode requires: bytes on py3k, str on py27
        fp.write('blah'.encode())
        del fp

        self.assertTrue(non_with_file.exists())
        self.assertTrue(non_with_file.isfile())
        self.assertFalse(non_with_file.isdir())

        with stor.settings.use(self.DX_WAIT_SETTINGS):
            with test_file.open(mode='wb') as obj:
                obj.write('this is a test\n'.encode())
                obj.write('this is another line.\n'.encode())

        self.assertTrue(test_file.exists())
        self.assertTrue(test_file.isfile())
        self.assertFalse(test_file.isdir())

        with stor.settings.use(self.DX_WAIT_SETTINGS):
            with test_file.open(mode='rb') as obj:
                with copy_file.open(mode='wb') as copy_obj:
                    copy_obj.write(obj.read())

        self.assertTrue(copy_file.exists())
        self.assertTrue(copy_file.isfile())
        self.assertFalse(copy_file.isdir())

        test_contents = test_file.open(mode='rb').read()
        copy_contents = copy_file.open(mode='rb').read()
        self.assertEquals(test_contents, 'this is a test\nthis is another line.\n'.encode())
        self.assertEquals(test_contents, copy_contents)

    def test_hidden_file_nested_dir_copytree(self):
        with NamedTemporaryDirectory(change_dir=True):
            open('.hidden_file', 'w').close()
            os.symlink('.hidden_file', 'symlink')
            os.mkdir('.hidden_dir')
            os.mkdir('.hidden_dir/nested')
            open('.hidden_dir/nested/file1', 'w').close()
            open('.hidden_dir/nested/file2', 'w').close()
            Path('.').copytree(self.test_dir)

        time.sleep(10)  # for uploaded files to close
        with NamedTemporaryDirectory(change_dir=True):
            self.test_dir.copytree('test')
            self.assertTrue(Path('test/.hidden_file').isfile())
            self.assertTrue(Path('test/symlink').isfile())
            self.assertTrue(Path('test/.hidden_dir').isdir())
            self.assertTrue(Path('test/.hidden_dir/nested').isdir())
            self.assertTrue(Path('test/.hidden_dir/nested/file1').isfile())
            self.assertTrue(Path('test/.hidden_dir/nested/file2').isfile())

    def test_read_bytes_from_binary(self):
        test_file = self.test_dir / 'test_file.txt'
        with stor.settings.use(self.DX_WAIT_SETTINGS):
            with stor.open(test_file, mode='wb') as fp:
                fp.write(test_integration.BYTE_STRING)

        with stor.open(test_file, mode='rb') as fp:
            result = fp.read()
        assert result == test_integration.BYTE_STRING

    def test_read_string_from_text(self):
        test_file = self.test_dir / 'test_file.txt'
        with stor.settings.use(self.DX_WAIT_SETTINGS):
            with stor.open(test_file, mode='w') as fp:
                fp.write(test_integration.STRING_STRING)

        with stor.open(test_file, mode='r') as fp:
            result = fp.read()
        assert result == test_integration.STRING_STRING

    def test_custom_encoding_text(self):
        # explicit encoding is only supported for py3 in general
        # dxpy py3 assumes the encoding is utf-8. can't support other encoding for dx
        pass

    def test_over_100_files(self):
        num_test_objs = 123
        min_obj_size = 0

        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.create_dataset(tmp_d, num_test_objs, min_obj_size)
            self.test_dir.upload(['.'])

        self.assertEquals(123, len(self.test_dir.list()))
        self.assertEquals(120, len(self.test_dir.list(limit=120)))
        self.assertTrue(self.test_dir.isdir())

        time.sleep(20)  # for uploaded files to close

        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.test_dir.download('./')
            self.assertEquals(123, len(os.listdir(tmp_d)))

    def test_is_methods(self):
        project = self.project
        file_with_prefix = stor.join(project, 'analysis.txt')

        # ensure container is created but empty
        self.assertTrue(stor.isdir(project))
        self.assertFalse(stor.isfile(project))
        self.assertTrue(stor.exists(project))
        self.assertFalse(stor.listdir(project))

        folder = stor.join(project, 'analysis')
        subfolder = stor.join(project, 'analysis', 'alignments')
        file_in_folder = stor.join(project, 'analysis', 'alignments',
                                   'bam.bam')
        self.assertFalse(stor.exists(file_in_folder))
        self.assertFalse(stor.isdir(folder))
        self.assertFalse(stor.isdir(folder + '/'))
        with stor.open(file_with_prefix, 'w') as fp:
            fp.write('data\n')
        self.assertFalse(stor.isdir(folder))
        self.assertTrue(stor.isfile(file_with_prefix))

        with stor.open(file_in_folder, 'w') as fp:
            fp.write('blah.txt\n')

        self.assertTrue(stor.isdir(folder))
        self.assertFalse(stor.isfile(folder))
        self.assertTrue(stor.isdir(subfolder))

    def test_upload_multiple_dirs(self):
        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            num_test_objs = 10
            tmp_d = Path(tmp_d)

            # Create files filled with random data.
            path1 = tmp_d / 'dir1'
            os.mkdir(path1)
            self.create_dataset(path1, num_test_objs, 10)

            # Create empty dir and file.
            path2 = tmp_d / 'dir2'
            os.mkdir(path2)
            os.mkdir(path2 / 'my_dir')
            open(path2 / 'my_dir' / 'included_file', 'w').close()
            open(path2 / 'my_dir' / 'excluded_file', 'w').close()
            os.mkdir(path2 / 'my_dir' / 'included_dir')
            os.mkdir(path2 / 'my_dir' / 'excluded_dir')

            # Create file in the top level directory.
            open(tmp_d / 'top_level_file', 'w').close()

            to_upload = [
                'dir1',
                'dir2/my_dir/included_file',
                'dir2/my_dir/included_dir',
                'top_level_file',
            ]
            with tmp_d:
                dx_p = self.test_dir / 'subdir'
                dx_p.upload(to_upload)

            # Validate the contents of the manifest file
            uploaded_contents = stor.list(dx_p)
            expected_contents = [
                Path('dir1') / name
                for name in self.get_dataset_obj_names(num_test_objs)
            ]
            expected_contents.extend([
                'dir2/my_dir/included_file',
                'top_level_file',
            ])

            expected_contents = [dx_p / c for c in expected_contents]
            self.assertEquals(set(uploaded_contents), set(expected_contents))

            empty_dir = dx_p / 'dir2/my_dir/included_dir'
            self.assertTrue(stor.isdir(empty_dir))

    def test_upload_download_remove(self):
        num_test_objs = 10
        min_obj_size = 50
        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.create_dataset(tmp_d, num_test_objs, min_obj_size)
            self.test_dir.upload(['.'])

        which_obj = self.get_dataset_obj_names(num_test_objs)[-1]
        dx_p = self.test_dir / which_obj
        file_h = dxpy.DXFile(dxid=dx_p.canonical_resource,
                             project=dx_p.canonical_project)
        file_h.wait_on_close(20)  # wait for file to go to closed state

        for which_obj in self.get_dataset_obj_names(num_test_objs):
            self.assertTrue((self.test_dir / which_obj).exists())

        with NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.test_dir.download(tmp_d)
            for which_obj in self.get_dataset_obj_names(num_test_objs):
                self.assertCorrectObjectContents(which_obj, which_obj, min_obj_size)
                (self.test_dir / which_obj).remove()

                # consistency check
                while (self.test_dir / which_obj).exists():
                    time.sleep(.5)
                self.assertFalse((self.test_dir / which_obj).exists())

    def test_gzip_on_remote(self):
        self._skip_if_filesystem(self.test_dir)
        local_gzip = os.path.join(os.path.dirname(__file__),
                                  'file_data/s_3_2126.bcl.gz')
        remote_gzip = stor.join(self.test_dir,
                                stor.basename(local_gzip))
        stor.copy(local_gzip, remote_gzip)
        file_h = dxpy.DXFile(dxid=remote_gzip.canonical_resource,
                             project=remote_gzip.canonical_project)
        file_h.wait_on_close(20)  # wait for file to go to closed state

        with stor.open(remote_gzip, mode='rb') as fp:
            with gzip.GzipFile(fileobj=fp) as remote_gzip_fp:
                with gzip.open(local_gzip) as local_gzip_fp:
                    assert_same_data(remote_gzip_fp, local_gzip_fp)
