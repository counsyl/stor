import tempfile

from stor import exceptions
import stor.utils as stor_utils


def is_swift_path(p):
    """Determines if the path is a Swift path.

    All Swift paths start with swift://

    Args:
        p (str): The path string

    Returns:
        bool: True if p is a Swift path, False otherwise.
    """
    from stor_swift.swift import SwiftPath
    return p.startswith(SwiftPath.drive)


def is_writeable(path, swift_retry_options=None):
    """
    Determine whether we have permission to write to path.

    Behavior of this method is slightly different for different storage types when the
    directory doesn't exist:
    1. For local file systems, this function will return True if the target directory
       exists and a file written to it.
    2. For AWS S3, this function will return True only if the target bucket is already
       present and we have write access to the bucket.
    3. For Swift, this function will return True, only if the target tenant is already
       present and we have write access to the tenant and container. The container doesn't
       have to be present.

    This is function is useful, because `stor.stat()` will succeed if we have read-only
    permissions to `path`, but the eventual attempt to upload will fail.

    Secondly, `path` might not exist yet. If the intent of the caller is to create it, ,
    stor.stat() will fail, however the eventual upload attempt would succeed.

    Args:
        path (stor.Path|str): The path to check.
        swift_retry_options (dict): Optional retry arguments to use for swift
            upload or download. View the
            `swift module-level documentation <swiftretry>` for more
            information on retry arguments. If the goal is to not use
            exponential backoff, pass ``{'num_retries': 0}`` here.

    Returns:
        bool: Whether ``path`` is writeable or not.
    """
    from stor import basename
    from stor import join
    from stor import Path
    from stor import remove
    from stor_swift.swift import ConflictError
    from stor_swift.swift import SwiftPath
    from stor_swift.swift import UnauthorizedError
    from stor_swift.swift import UnavailableError
    import stor

    path = stor_utils.with_trailing_slash(Path(path))

    # We want this function to behave as a no-op with regards to the underlying
    # container structure. Therefore we need to remove any containers created by this
    # function that were not present when it was called. The `container_existed`
    # defined below will store whether the container that we're checking existed when
    # calling this function, so that we know if it should be removed at the end.
    container_path = Path('{}{}/{}/'.format(
        SwiftPath.drive,
        path.tenant,
        path.container
    ))
    container_existed = container_path.exists()

    with tempfile.NamedTemporaryFile() as tmpfile:
        try:
            # Attempt to create a file in the `path`.
            stor.copy(tmpfile.name, path, swift_retry_options=swift_retry_options)
            # Remove the file that was created.
            remove(join(path, basename(tmpfile.name)))
            answer = True
        except (UnauthorizedError, UnavailableError, IOError, OSError, exceptions.FailedUploadError):  # nopep8
            answer = False

    # Remove the Swift container if it didn't exist when calling this function, but exists
    # now. This way this function remains a no-op with regards to container structure.
    if container_existed is False and container_path.exists():
        try:
            container_path.remove_container()
        except ConflictError:
            # Ignore if some other thread/user created the container in the meantime.
            pass

    return answer
