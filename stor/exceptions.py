"""
Provides exceptions classes thrown by various utilities of stor
"""


class RemoteError(Exception):
    """
    The top-level exception thrown for any errors from remote services
    (eg. swift, S3).

    The 'caught_exception' attribute of the exception must be examined in order
    to inspect the exception thrown by the swift or S3 service. A swift exception

    Swift has two types of caught exceptions:

    * ``SwiftError`` (thrown by ``swiftclient.service``)
    * ``ClientError`` (thrown by ``swiftclient.client``)

    S3 raises:

    * ``ClientError`` (thrown by ``botocore.client``).

    Attributes:
        caught_exception (Exception): the underlying exception that was raised from service.
    """
    def __init__(self, message, caught_exception=None):
        super(RemoteError, self).__init__(message)
        self.caught_exception = caught_exception


class NotFoundError(RemoteError):
    """Thrown when a 404 response is returned."""
    pass


class InvalidObjectStateError(RemoteError):
    """Base class for 403 errors from S3 dealing with storage classes."""


class ObjectInColdStorageError(InvalidObjectStateError):
    """Thrown when a 403 is returned from S3/SwiftStack because backing data is on Glacier.

    Note:
        We don't want to retry on this
        one, because the object will always be in cold storage.
        See AWS `S3 Rest API GET`_ spec for more details.

    .. _S3 Rest API GET: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
    """


class AlreadyRestoredError(InvalidObjectStateError):
    """Thrown on attempt to restore object already not in Glacier"""


class UnauthorizedError(RemoteError):
    """Thrown when a 403 response is returned.

    Note:
        Internal swift connection errors (e.g., when a particular node is
        unavailable) appear to translate themselves into 403 errors at the
        proxy layer, thus in general it's a good idea to retry on authorization
        errors as equivalent to unavailable errors when doing PUT or GET
        operations (list / stat / etc never hit this issue).
    """
    pass


class UnavailableError(RemoteError):
    """Thrown when a 503 response is returned."""
    pass


class ConflictError(RemoteError):
    """Thrown when a 409 response is returned.

    Notes:
        * **Swift**: This error is thrown when deleting a container and
          some object storage nodes report that the container
          has objects while others don't.
        * **S3**: Raised when attempting to restore object that's already being restored (as
          RestoreAlreadyInProgressError). Possibly in other cases.
    """


class RestoreAlreadyInProgressError(ConflictError):
    """Thrown when RestoreAlreadyInProgress on glacier restore"""


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
