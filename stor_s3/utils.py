import tempfile

import stor.utils as stor_utils


def is_s3_path(p):
    """Determines if the path is a S3 path.

    All S3 paths start with ``s3://``

    Args:
        p (str): The path string

    Returns
        bool: True if p is a S3 path, False otherwise.
    """
    from stor_s3.s3 import S3Path
    return p.startswith(S3Path.drive)


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
    from stor.exceptions import UnauthorizedError
    from stor.exceptions import UnavailableError
    from stor.exceptions import ConflictError
    from stor.exceptions import FailedUploadError

    path = stor_utils.with_trailing_slash(Path(path))

    container_path = None
    container_existed = None

    with tempfile.NamedTemporaryFile() as tmpfile:
        try:
            # Attempt to create a file in the `path`.
            stor_utils.copy(tmpfile.name, path, swift_retry_options=swift_retry_options)
            # Remove the file that was created.
            remove(join(path, basename(tmpfile.name)))
            answer = True
        except (UnauthorizedError, UnavailableError, IOError, OSError, FailedUploadError):  # nopep8
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
