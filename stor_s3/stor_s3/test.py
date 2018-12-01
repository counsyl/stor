import mock
import unittest

from stor_s3 import s3
from stor_s3.s3 import S3Path


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
        self._get_s3_client_patcher = mock.patch('stor_s3.s3._get_s3_client',
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
        s3_transfer_patcher = mock.patch('stor_s3.s3.S3Transfer', autospec=True)
        self.addCleanup(s3_transfer_patcher.stop)
        self.mock_get_s3_transfer = s3_transfer_patcher.start()
        self.mock_get_s3_transfer.return_value = self.mock_s3_transfer

        # Mock the TransferConfig object
        s3_transfer_config_patcher = mock.patch('stor_s3.s3.TransferConfig',
                                                autospec=True)
        self.addCleanup(s3_transfer_config_patcher.stop)
        self.mock_get_s3_transfer_config = s3_transfer_config_patcher.start()


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
