from stor.tests import integration
import stor
import six


class FilesystemIntegrationTest(integration.FromFilesystemTestCase):
    def setUp(self):
        super(FilesystemIntegrationTest, self).setUp()
        ntd_obj = stor.NamedTemporaryDirectory()
        # ensure that it's empty and does not exist to start
        self.test_dir = stor.join(ntd_obj.__enter__(), 'parent')
        self.addCleanup(ntd_obj.__exit__, None, None, None)

    def test_non_empty_directory_errors(self):
        example_dir = stor.join(self.test_dir, 'example')
        assert not example_dir.exists()
        other_dir = stor.join(self.test_dir, 'otherdir')
        self.create_dataset(other_dir, 1, 1)
        self.create_dataset(example_dir, 1, 1)
        example_dir.makedirs_p()
        exc_type = OSError if six.PY2 else FileExistsError  # nopep8
        with self.assertRaisesRegexp(exc_type, '.*File exists'):
            try:
                stor.copytree(other_dir, example_dir)
            except exc_type as e:
                assert e.errno == 17
                raise
