from stor.tests.test_integration import BaseIntegrationTest
import stor
import six


class FilesystemIntegrationTest(BaseIntegrationTest.BaseTestCases):
    def setUp(self):
        super(FilesystemIntegrationTest, self).setUp()
        ntp_obj = stor.NamedTemporaryDirectory()
        # ensure that it's empty and does not exist to start
        self.test_dir = stor.join(ntp_obj.__enter__(), 'parent')
        self.addCleanup(ntp_obj.__exit__, None, None, None)

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
