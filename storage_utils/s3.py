import posixpath
import threading

import boto3
from botocore import exceptions as s3_exceptions

from storage_utils import exceptions
from storage_utils.base import Path
from storage_utils.posix import PosixPath

# boto3 defined limit to number of returned objects
MAX_LIMIT = 1000

# Thread-local variable used to cache the client
thread_local = threading.local()


def _parse_s3_error(exc):
    """
    Parses botocore.exception.ClientError exceptions to throw a more
    informative exception.
    """
    http_status = exc.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
    msg = exc.response['Error'].get('Message', 'Unknown')

    if http_status == 403:
        return exceptions.UnauthorizedError(msg, exc)
    elif http_status == 404:
        return exceptions.NotFoundError(msg, exc)
    elif http_status == 503:
        return exceptions.UnavailableError(msg, exc)

    return exceptions.RemoteError(msg, exc)


def _get_s3_client():
    """Returns the boto3 client and initializes one if it doesn't already exist.

    Returns:
        boto3.Client: An instance of the S3 client.
    """
    if not hasattr(thread_local, 's3_client'):
        thread_local.s3_client = boto3.client('s3')
    return thread_local.s3_client


class S3Path(Path):
    """
    Provides the ability to manipulate and access resources on Amazon S3
    with a similar interface to the path library.
    """
    s3_drive = 's3://'
    path_module = posixpath
    parts_class = PosixPath

    def __init__(self, pth):
        """
        Validates S3 path is in the proper format.

        Args:
            pth (str): A path that matches the format of
                ``s3://{bucket_name}/{rest_of_path}``
                The ``s3://`` prefix is required in the path.
        """
        if not hasattr(pth, 'startswith') or not pth.startswith(self.s3_drive):
            raise ValueError('path must have %s (got %r)' % (self.s3_drive, pth))
        return super(S3Path, self).__init__(pth)

    def __repr__(self):
        return '%s("%s")' % (type(self).__name__, self)

    def _get_parts(self):
        """Returns the path parts (excluding s3://) as a list of strings."""
        if len(self) > len(self.s3_drive):
            return self[len(self.s3_drive):].split('/')
        else:
            return []

    @property
    def name(self):
        """The name of the path, mimicking path.py's name property"""
        return self.parts_class(super(S3Path, self).name)

    @property
    def parent(self):
        """The parent of the path, mimicking path.py's parent property"""
        return self.path_class(super(S3Path, self).parent)

    @property
    def bucket(self):
        """Returns the bucket name from the path or None"""
        parts = self._get_parts()
        return parts[0] if len(parts) > 0 and parts[0] else None

    @property
    def resource(self):
        """
        Returns the resource as a ``PosixPath`` object or None.

        A resource can be a single object or a prefix to objects.
        Note that it's important to keep the trailing slash in a resource
        name for prefix queries.
        """
        parts = self._get_parts()
        joined_resource = '/'.join(parts[1:]) if len(parts) > 1 else None

        return self.parts_class(joined_resource) if joined_resource else None

    def _s3_client_call(self, method_name, *args, **kwargs):
        """
        Creates a boto3 S3 ``Client`` object and runs ``method_name``.
        """
        s3_client = _get_s3_client()
        method = getattr(s3_client, method_name)
        try:
            return method(*args, **kwargs)
        except s3_exceptions.ClientError as e:
            raise _parse_s3_error(e)

    def _get_s3_iterator(self, method_name, *args, **kwargs):
        """
        Creates a boto3 ``S3.Paginator`` object and returns an iterator
        that runs over results from ``method_name``.
        """
        s3_client = _get_s3_client()
        paginator = s3_client.get_paginator(method_name)
        return paginator.paginate(**kwargs)

    def open(self, **kwargs):
        raise NotImplementedError

    def list(self, starts_with=None, limit=None, list_as_dir=False):
        """
        List contents using the resource of the path as a prefix.

        Args:
            starts_with (str): Allows for an additional search path to be
                appended to the current swift path. The current path will be
                treated as a directory.
            limit (int): Limit the amount of results returned.

        Returns:
            List[S3Path]: Every path in the listing

        TODO:
        - Currently errors just returned as a ClientError with helpful
        text. Want to be able to parse the error.
        - Handle errors, such as bucket does not exist
        """
        bucket = self.bucket
        prefix = self.resource

        if starts_with:
            prefix = prefix / starts_with if prefix else starts_with
        else:
            prefix = prefix or ''

        list_kwargs = {
            'Bucket': bucket,
            'Prefix': prefix,
            'PaginationConfig': {}
        }

        if limit:
            list_kwargs['PaginationConfig']['MaxItems'] = limit

        if list_as_dir:
            # Ensure the the prefix has a trailing slash if there is a prefix
            list_kwargs['Prefix'] = prefix / '' if prefix else ''
            list_kwargs['Delimiter'] = '/'

        path_prefix = S3Path('%s%s' % (self.s3_drive, bucket))

        results = self._get_s3_iterator('list_objects_v2', **list_kwargs)
        list_results = []
        try:
            for page in results:
                if 'Contents' in page:
                    list_results.extend([
                        path_prefix / result['Key']
                        for result in page['Contents']
                    ])
                if list_as_dir and 'CommonPrefixes' in page:
                    list_results.extend([
                        path_prefix / result['Prefix']
                        for result in page['CommonPrefixes']
                    ])
        except s3_exceptions.ClientError as e:
            raise _parse_s3_error(e)

        return list_results

    def listdir(self):
        """List the path as a dir, returning top-level directories and files."""
        return self.list(list_as_dir=True)

    def exists(self):
        """
        Checks existence of the path.

        Returns:
            bool: True if the path exists, False otherwise.
        """
        raise NotImplementedError

    def isabs(self):
        return True

    def isdir(self):
        raise NotImplementedError

    def isfile(self):
        raise NotImplementedError

    def islink(self):
        return False

    def ismount(self):
        return True

    def getsize(self):
        raise NotImplementedError

    def remove(self):
        raise NotImplementedError

    def rmtree(self):
        raise NotImplementedError

    def stat(self):
        """
        Performs a stat on the path.

        ``stat`` only works on paths that are buckets or objects.
        Using ``stat`` on a directory of objects will produce a `NotFoundError`.
        """
        raise NotImplementedError

    def walkfiles(self, pattern=None):
        """
        Iterate over listed files whose filenames match an optional pattern.

        Args:
            pattern (str, optional): Only return files that match this pattern.

        Returns:
            Iter[S3Path]: All files that match the optional pattern. Directories
                are not returned.
        """
        for f in self.list():
            if pattern is None or f.fnmatch(pattern):
                yield f
