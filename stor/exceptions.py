"""
Provides exceptions classes thrown by various utilities of stor
"""


class RemoteError(Exception):
    """
    The top-level exception thrown for any errors from remote services
    (eg. swift, S3).

    The 'caught_exception' attribute of the exception must be examined in order
    to inspect the exception thrown by the swift or S3 service. A swift exception
    can either be a ``SwiftError`` (thrown by ``swiftclient.service``) or a
    ``ClientError`` (thrown by ``swiftclient.client``). A S3 exception can be
    a ``ClientError`` (thrown by ``botocore.client``).
    """
    def __init__(self, message, caught_exception=None):
        super(RemoteError, self).__init__(message)
        self.caught_exception = caught_exception


class NotFoundError(RemoteError):
    """Thrown when a 404 response is returned."""
    pass


class UnauthorizedError(RemoteError):
    """Thrown when a 403 response is returned."""
    pass


class UnavailableError(RemoteError):
    """Thrown when a 503 response is returned."""
    pass


class ConditionNotMetError(RemoteError):
    """Thrown when a condition is not met."""
    pass


class FailedTransferError(RemoteError):
    """Thrown when a file transfer fails."""
    pass


class FailedUploadError(FailedTransferError):
    """Thrown when an upload fails."""
    pass


class FailedDownloadError(FailedTransferError):
    """Thrown when a download fails."""
    pass
