import mock
import unittest

from stor import settings
from stor_swift.swift import SwiftPath


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
        _cache_patcher = mock.patch.dict('stor_swift.swift._cached_auth_token_map', clear=True)
        self.addCleanup(_cache_patcher.stop)
        _cache_patcher.start()

    def assertSwiftListResultsEqual(self, r1, r2):
        """
        Swift list resolves duplicates, so the ordering of the results are not
        always the same as what the swift client returns. Compare results as
        sorted lists
        """
        self.assertEquals(sorted(r1), sorted(r2))


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
