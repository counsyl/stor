import inspect
import unittest
import os
import sys
import uuid

import dxpy
import vcr

from unittest import mock

from stor import Path
from stor import s3
from stor.s3 import S3Path
from stor.swift import SwiftPath
from stor import settings


class SwiftTestMixin(object):
    """A mixin with helpers for mocking out swift.

    SwiftTestMixin should be used to create base test classes for anything
    that accesses swift.
    """
    def disable_get_swift_service_mock(self):
        """Disables the mock for getting the swift service.
        """
        try:
            self._get_swift_patcher.stop()
        except RuntimeError:  # pragma: no cover
            # If the user disables the mock, the mock will try
            # to be stopped on test cleanup. Disable errors from that
            pass

    def setup_swift_mocks(self):
        """Sets all of the relevant mocks for Swift communication.

        If you are testing outside of this library, you should either mock
        swift object methods or you should focus on manipulating return value
        of mock_swift.

        The following variables are set up when calling this:

        - mock_swift_service: A mock of the SwiftService class defined in
          swiftclient.service.

        - mock_swift_get_conn: A mock of the get_conn function in the
          swiftclient.service module

        - mock_swift_conn: A mock of the SwiftConnection returned by
          get_conn

        - mock_swift_get_auth_keystone: mock of the get_keystone_auth function
          that caches identity credentials

        - mock_get_swift_service: A mock of the _get_swift_service method of
          SwiftPath

        - mock_swift: A mock of the SwiftService instance returned by
          _get_swift_service in SwiftPath
        """
        # Ensure that SwiftService will never be instantiated in tests
        swift_service_patcher = mock.patch('swiftclient.service.SwiftService',
                                           autospec=True)
        self.addCleanup(swift_service_patcher.stop)
        self.mock_swift_service = swift_service_patcher.start()

        # Ensure that SwiftConnections will never be instantiated in tests
        swift_get_conn_patcher = mock.patch('swiftclient.service.get_conn',
                                            autospec=True)
        self.addCleanup(swift_get_conn_patcher.stop)
        self.mock_swift_get_conn = swift_get_conn_patcher.start()
        self.mock_swift_conn = mock.Mock()
        self.mock_swift_get_conn.return_value = self.mock_swift_conn

        # Ensure that no keystone auth calls will go out
        swift_keystone_mock_patcher = mock.patch('swiftclient.client.get_auth_keystone',
                                                 autospec=True)
        self.addCleanup(swift_keystone_mock_patcher.stop)
        self.mock_swift_get_auth_keystone = swift_keystone_mock_patcher.start()
        self.mock_swift_get_auth_keystone.return_value = ('dummy_storage_url', 'dummy_auth_token')

        # This is the mock that will always be returned by _get_swift_service.
        # The user can mock out any swift methods on this mock
        self.mock_swift = mock.Mock()
        self._get_swift_patcher = mock.patch.object(SwiftPath,
                                                    '_get_swift_service',
                                                    autospec=True)
        self.addCleanup(self.disable_get_swift_service_mock)
        self.mock_get_swift_service = self._get_swift_patcher.start()
        self.mock_get_swift_service.return_value = self.mock_swift

        # ensures we never cache data between tests
        _cache_patcher = mock.patch.dict('stor.swift._cached_auth_token_map', clear=True)
        self.addCleanup(_cache_patcher.stop)
        _cache_patcher.start()

    def assertSwiftListResultsEqual(self, r1, r2):
        """
        Swift list resolves duplicates, so the ordering of the results are not
        always the same as what the swift client returns. Compare results as
        sorted lists
        """
        self.assertEquals(sorted(r1), sorted(r2))


class S3TestMixin(object):
    """A mixin with helpers for mocking out S3.

    S3TestMixin should be used to create base test classes for anything
    that accesses S3.
    """
    def disable_get_s3_client_mock(self):
        """Disables the mock for getting the S3 client."""
        try:
            self._get_s3_client_patcher.stop()
        except RuntimeError:  # pragma: no cover
            # If the user disables the mock, the mock will try
            # to be stopped on test cleanup. Disable errors from that
            pass

    def disable_get_s3_iterator_mock(self):
        """Disables the mock for getting the S3 iterator."""
        try:
            self._get_s3_iterator_patcher.stop()
        except RuntimeError:  # pragma: no cover
            pass

    def setup_s3_mocks(self):
        """Sets all of the relevant mocks for S3 communication.

        If you are testing outside of this library, you should either mock
        S3 client methods or you should focus on manipulating return value
        of mock_s3.

        Tests of methods that directly make API calls via _s3_client_call should
        mock the return values of the API calls on mock_s3. Tests of methods that
        do not directly make the API calls should mock any S3Path methods being called.

        The following variables are set up when calling this:

        - mock_s3_client: A mock of the Client instance returned by boto3.client

        - mock_s3: A mock of the Client instance returned by _get_s3_client in S3Path.

        - mock_get_s3_client: A mock of the _get_s3_client method in S3Path.

        - mock_get_s3_iterator: A mock of the _get_s3_iterator method in S3Path.

        - mock_s3_iterator: A mock of the iterable object returned by _get_s3_iterator in S3Path.

        - mock_s3_transfer: A mock of the Transfer instance returned by S3Transfer

        - mock_get_s3_transfer: A mock of the boto3.s3.transfer.S3Transfer object
        """
        # Ensure that the S3 session will never be instantiated in tests
        s3_session_patcher = mock.patch('boto3.session.Session', autospec=True)
        self.addCleanup(s3_session_patcher.stop)
        self.mock_s3_session = s3_session_patcher.start()

        # This is the mock returned by _get_s3_client.
        # User can mock s3 methods on this mock.
        self.mock_s3 = mock.Mock()
        self._get_s3_client_patcher = mock.patch('stor.s3._get_s3_client',
                                                 autospec=True)
        self.addCleanup(self.disable_get_s3_client_mock)
        self.mock_get_s3_client = self._get_s3_client_patcher.start()
        self.mock_get_s3_client.return_value = self.mock_s3

        # This is the mock returned by _get_s3_iterator.
        # User should modify the __iter__.return_value property to specify return values.
        self.mock_s3_iterator = mock.MagicMock()
        self._get_s3_iterator_patcher = mock.patch.object(S3Path, '_get_s3_iterator',
                                                          autospec=True)
        self.addCleanup(self.disable_get_s3_iterator_mock)
        self.mock_get_s3_iterator = self._get_s3_iterator_patcher.start()
        self.mock_get_s3_iterator.return_value = self.mock_s3_iterator

        # Ensure that an S3Transfer object will never be instantiated in tests.
        # User can mock methods associated with S3Transfer on this mock.
        self.mock_s3_transfer = mock.Mock()
        s3_transfer_patcher = mock.patch('stor.s3.S3Transfer', autospec=True)
        self.addCleanup(s3_transfer_patcher.stop)
        self.mock_get_s3_transfer = s3_transfer_patcher.start()
        self.mock_get_s3_transfer.return_value = self.mock_s3_transfer

        # Mock the TransferConfig object
        s3_transfer_config_patcher = mock.patch('stor.s3.TransferConfig',
                                                autospec=True)
        self.addCleanup(s3_transfer_config_patcher.stop)
        self.mock_get_s3_transfer_config = s3_transfer_config_patcher.start()


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


class SwiftTestCase(unittest.TestCase, SwiftTestMixin):
    """A TestCase class that sets up swift mocks and provides additional assertions"""
    def setUp(self):
        super(SwiftTestCase, self).setUp()
        self.setup_swift_mocks()

        # make sure swift credentials aren't included
        settings.update({
            'swift': {
                'username': '__dummy__',
                'password': '__dummy__',
                'auth_url': '__dummy__'
            }
        })


class S3TestCase(unittest.TestCase, S3TestMixin):
    """A TestCase class that sets up S3 mocks"""
    def setUp(self):
        super(S3TestCase, self).setUp()
        self.setup_s3_mocks()
        try:
            del s3._thread_local.s3_transfer
            del s3._thread_local.s3_transfer_config
        except AttributeError:
            pass


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
        posix_p = Path('./{test_folder}'.format(test_folder=self.project))
        posix_p.rmtree()

    def teardown_project(self):
        self.project_handler.destroy()
        self.project_handler = None
