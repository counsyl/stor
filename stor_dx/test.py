import inspect
import mock
import unittest
import os
import sys
import uuid

import dxpy
import vcr

from stor import Path


class DXTestMixin(object):
    """A mixin with helpers for testing dxpy.

    DXTestMixin should be used to create base test classes for anything
    that accesses DNAnexus. This Mixin introduces vcrpy into the test case
    which records all http interactions for playback.
    """
    vcr_enabled = True  # switch this to False to deactivate vcr recording

    def setUp(self):  # pragma: no cover
        """Sets us vcr cassettes if enabled, and starts patcher for time.sleep.
        To update the cassettes, the easiest error-free way is to delete
        the cassettes and rerecord them.

        Note that changing the record_mode to 'all' temporarily updates the cassettes,
        but playback from two same set of requests errors in certain scenarios.
        """
        super(DXTestMixin, self).setUp()
        self.cassette = None
        if self.vcr_enabled:
            myvcr = vcr.VCR(cassette_library_dir=self._get_cassette_library_dir(),
                            filter_headers=['authorization'])
            cm = myvcr.use_cassette(self._get_cassette_name())
            self.cassette = cm.__enter__()
            self.addCleanup(cm.__exit__, None, None, None)
        if self.cassette and self.cassette.rewound:
            patcher = mock.patch('time.sleep')
            self.addCleanup(patcher.stop)
            patcher.start()

    def _get_cassette_library_dir(self):
        """Sets up different directories for Python 2 and 3, as well as by TestClass
        subdir, because cassette recording and playback are in different formats
        (unicode/binary) in Python 2 vs 3, making them incompatible with each other.
        """
        testdir = os.path.dirname(inspect.getfile(self.__class__))
        cassette_dir = os.path.join(testdir, 'cassettes_py{}'.format(sys.version_info[0]))
        return os.path.join(cassette_dir, self.__class__.__name__)

    def _get_cassette_name(self):
        return '{}.yaml'.format(self._testMethodName)

    def assert_dx_lists_equal(self, r1, r2):
        self.assertEqual(sorted(r1), sorted(r2))


class DXTestCase(DXTestMixin, unittest.TestCase):
    """A TestCase class that sets up DNAnexus vars and provides additional assertions.

    Since DXTestCase inherits from DXTestMixin, all the tests under DXTestCase are
    auto-wrapped with VCRpy, and hence use cassettes for playback.
    Look into `DXTestMixin` to turn off VCRpy and additional details.
    """

    def new_proj_name(self):
        """Output a unique project name for each test case.
        Should only be called once within a test case, and the result reused
        everywhere within a test case.
        """
        return '{0}.{1}.{2}'.format(self.__class__.__name__,
                                    self._testMethodName,
                                    str(uuid.uuid4())[:8])

    def setup_temporary_project(self):
        self.project_handler = self.setup_project()
        self.project = self.project_handler.name
        self.proj_id = self.project_handler.get_id()
        self.addCleanup(self.teardown_project)

    def setup_project(self):
        test_proj = dxpy.DXProject()
        test_proj.new(self.new_proj_name())
        return test_proj

    def setup_files(self, files):
        """Sets up files for testing.
        This does not assume the files will be closed by the end of this function.

        Args:
            files (List[str]): list of files relative to project root to be created on DX
            Only virtual paths are allowed. Path must start with '/'
        """
        for i, curr_file in enumerate(files):
            dx_p = Path(curr_file)
            self.project_handler.new_folder(dx_p.parent, parents=True)
            with dxpy.new_dxfile(name=dx_p.name,
                                 folder='/'+dx_p.parent.lstrip('/'),
                                 project=self.proj_id) as f:
                f.write('data{}'.format(i).encode())

    def setup_file(self, obj):
        """Set up a closed file for testing.

        Args:
            obj (str): file relative to project root to be created on DX
            Only virtual paths are allowed. Path must start with '/'
        """
        dx_p = Path(obj)
        self.project_handler.new_folder(dx_p.parent, parents=True)
        with dxpy.new_dxfile(name=dx_p.name,
                             folder='/'+dx_p.parent.lstrip('/'),
                             project=self.proj_id) as f:
            f.write('data'.encode())
        # to allow for max of 20s for file state to go to closed
        f.wait_on_close(20)
        return f

    def setup_posix_files(self, files):
        """Sets up posix files for testing

        Args:
            files (List[Str]): list of relative posix files to be created.
        """
        for i, curr_file in enumerate(files):
            posix_p = Path('./{test_folder}/{path}'.format(
                test_folder=self.project, path=curr_file))
            posix_p.open(mode='w').write('data'+str(i))
        self.addCleanup(self.teardown_posix_files)

    def teardown_posix_files(self):
        posix_p = Path('./{test_folder}'.format(
                test_folder=self.project))
        posix_p.rmtree()

    def teardown_project(self):
        self.project_handler.destroy()
        self.project_handler = None
