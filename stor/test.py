import mock
from stor import s3
from stor.s3 import S3Path
from stor.swift import SwiftPath
from stor import settings
import unittest


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
        except RuntimeError:
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
        except RuntimeError:
            # If the user disables the mock, the mock will try
            # to be stopped on test cleanup. Disable errors from that
            pass

    def disable_get_s3_iterator_mock(self):
        """Disables the mock for getting the S3 iterator."""
        try:
            self._get_s3_iterator_patcher.stop()
        except RuntimeError:
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
