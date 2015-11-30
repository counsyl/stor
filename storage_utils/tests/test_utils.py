from storage_utils import path
from storage_utils import utils
import unittest


class TestWalkFilesAndDirs(unittest.TestCase):
    def test_w_dir(self):
        # Create an empty directory for this test in ./swift_upload. This
        # is because git doesnt allow a truly empty directory to be checked
        # in
        swift_dir = path(__file__).absexpand().parent / 'swift_upload'
        with utils.NamedTemporaryDirectory(dir=swift_dir) as tmp_dir:
            uploads = utils.walk_files_and_dirs([swift_dir])
            self.assertEquals(set(uploads), set([
                swift_dir / 'file1',
                tmp_dir,
                swift_dir / 'data_dir' / 'file2',
            ]))

    def test_w_file(self):
        name = path(__file__).absexpand().parent / 'swift_upload' / 'file1'

        uploads = utils.walk_files_and_dirs([name])
        self.assertEquals(set(uploads), set([name]))

    def test_w_invalid_file(self):
        name = path(__file__).absexpand().parent / 'swift_upload' / 'invalid'

        with self.assertRaises(ValueError):
            utils.walk_files_and_dirs([name])


class TestChdir(unittest.TestCase):
    def test_chdir(self):
        p = path().absexpand()
        self.assertTrue(p.endswith('counsyl-storage-utils'))

        with utils.chdir(p / 'storage_utils' / 'tests'):
            p = path().absexpand()
            self.assertTrue(p.endswith('tests'))

        p = path().absexpand()
        self.assertTrue(p.endswith('counsyl-storage-utils'))


class TestNamedTemporaryDirectory(unittest.TestCase):
    def test_wo_chdir(self):
        tmp_d = None
        with utils.NamedTemporaryDirectory(change_dir=True) as tmp_d:
            self.assertTrue(tmp_d.exists())
            p = path().absexpand()
            self.assertTrue(tmp_d in p)

        self.assertFalse(tmp_d.exists())

    def test_w_chdir(self):
        tmp_d = None
        with utils.NamedTemporaryDirectory() as tmp_d:
            self.assertTrue(tmp_d.exists())

        self.assertFalse(tmp_d.exists())
