"""
An experimental implementation of S3 in storage-utils.
"""
import posixpath
import threading

import boto3
from botocore import exceptions as boto_s3_exceptions

from storage_utils import exceptions
from storage_utils.base import Path
from storage_utils.posix import PosixPath
from storage_utils import utils

# Thread-local variable used to cache the client
_thread_local = threading.local()


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

    We use a different boto3 client for each thread/process because boto3 clients
    are not thread-safe.

    Returns:
        boto3.Client: An instance of the S3 client.
    """
    client_kwargs = {}
    if not hasattr(_thread_local, 's3_client'):
        _thread_local.s3_client = boto3.client('s3', **client_kwargs)
    return _thread_local.s3_client


class S3Path(Path):
    """
    Provides the ability to manipulate and access S3 resources
    with a similar interface to the path library.

    Right now, the client defaults to Amazon S3 endpoints, but in the
    near-future, users should be able to custom configure the S3 client.
    """
    drive = 's3://'
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
        if not hasattr(pth, 'startswith') or not pth.startswith(self.drive):
            raise ValueError('path must have %s (got %r)' % (self.drive, pth))
        return super(S3Path, self).__init__(pth)

    def __repr__(self):
        return '%s("%s")' % (type(self).__name__, self)

    def _get_parts(self):
        """Returns the path parts (excluding s3://) as a list of strings."""
        if len(self) > len(self.drive):
            return self[len(self.drive):].split('/')
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
        except boto_s3_exceptions.ClientError as e:
            raise _parse_s3_error(e)

    def _get_s3_iterator(self, method_name, *args, **kwargs):
        """
        Creates a boto3 ``S3.Paginator`` object and returns an iterator
        that runs over results from ``method_name``.
        """
        s3_client = _get_s3_client()
        paginator = s3_client.get_paginator(method_name)
        return paginator.paginate(**kwargs)

    def open(self, mode='r'):
        """
        Opens a S3File that can be read or written to.

        For examples of reading and writing opened objects, view
        S3File.

        Args:
            mode (str): The mode of object IO. Currently supports reading
                ("r" or "rb") and writing ("w", "wb")

        Returns:
            S3File: The s3 object.

        Raises:
            RemoteError: A s3 client error occurred.
        """
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
            list_kwargs['Prefix'] = utils.with_trailing_slash(prefix) if prefix else ''
            list_kwargs['Delimiter'] = '/'

        path_prefix = S3Path('%s%s' % (self.drive, bucket))

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
        except boto_s3_exceptions.ClientError as e:
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

        Raises:
            RemoteError: A non-404 error occurred.
        """
        try:
            return bool(self.list(limit=1))
        except exceptions.NotFoundError:
            return False

    def isabs(self):
        return True

    def isdir(self):
        """
        TODO: Check for directory markers (once implemented)
        """
        try:
            return self.list(limit=1)[0] != self
        except (exceptions.NotFoundError, IndexError):
            return False

    def isfile(self):
        try:
            contents = self.list(limit=1)
        except exceptions.NotFoundError:
            return False
        return len(contents) > 0 and contents[0] == self

    def islink(self):
        return False

    def ismount(self):
        return True

    def getsize(self):
        """
        Returns the content length of an object in S3.

        Directories and buckets have no length and will return 0.
        """
        bucket = self.bucket
        if not self.resource:
            # check for existence of bucket
            self._s3_client_call('head_bucket', Bucket=bucket)
        else:
            try:
                return self._s3_client_call('head_object',
                                            Bucket=bucket,
                                            Key=self.resource).get('ContentLength', 0)
            except exceptions.NotFoundError:
                # Check if path is a directory
                if not self.exists():
                    raise

        return 0

    def remove(self):
        """
        Removes a single object.

        Raises:
            ValueError: The path is invalid.
            RemoteError: An s3 client error occurred.
        """
        resource = self.resource
        if not resource:
            raise ValueError('cannot remove a bucket')
        return self._s3_client_call('delete_object', Bucket=self.bucket, Key=resource)

    def rmtree(self):
        """
        Removes a resource and all of its contents. The path should point to a directory.

        If the specified resource is an object, nothing will happen.
        """
        # Ensure there is a trailing slash (path is a dir)
        delete_path = utils.with_trailing_slash(self)
        delete_list = delete_path.list()

        while len(delete_list) > 0:
            # boto3 only allows deletion of up to 1000 objects at a time
            len_range = min(len(delete_list), 1000)
            objects = {
                'Objects': [
                    {'Key': delete_list.pop(0).resource}
                    for i in range(len_range)
                ]
            }
            response = self._s3_client_call('delete_objects', Bucket=self.bucket, Delete=objects)

            if 'Errors' in response:
                raise exceptions.RemoteError('an error occurred while using rmtree',
                                             response['Errors'])

    def stat(self):
        """
        Performs a stat on the path.

        ``stat`` only works on paths that are objects.
        Using ``stat`` on a directory of objects will produce a `NotFoundError`.

        An example return dictionary is the following::

            {
                'DeleteMarker': True|False,
                'AcceptRanges': 'string',
                'Expiration': 'string',
                'Restore': 'string',
                'LastModified': datetime(2015, 1, 1),
                'ContentLength': 123,
                'ETag': 'string',
                'MissingMeta': 123,
                'VersionId': 'string',
                'CacheControl': 'string',
                'ContentDisposition': 'string',
                'ContentEncoding': 'string',
                'ContentLanguage': 'string',
                'ContentType': 'string',
                'Expires': datetime(2015, 1, 1),
                'WebsiteRedirectLocation': 'string',
                'ServerSideEncryption': 'AES256'|'aws:kms',
                'Metadata': {
                    'string': 'string'
                },
                'SSECustomerAlgorithm': 'string',
                'SSECustomerKeyMD5': 'string',
                'SSEKMSKeyId': 'string',
                'StorageClass': 'STANDARD'|'REDUCED_REDUNDANCY'|'STANDARD_IA',
                'RequestCharged': 'requester',
                'ReplicationStatus': 'COMPLETE'|'PENDING'|'FAILED'|'REPLICA'
            }
        """
        if not self.resource:
            raise ValueError('stat cannot be called on a bucket')

        response = self._s3_client_call('head_object', Bucket=self.bucket, Key=self.resource)
        del response['ResponseMetadata']
        return response

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

    def download_object(self, dest):
        """
        Downloads a file from S3 to a destination file.

        Args:
            dest (str): The destination path to download file to.

        Notes:
            - The destination directory will be created automatically if it doesn't exist.

            - This method downloads to paths relative to the current
              directory.
        """
        dl_kwargs = {
            'Bucket': self.bucket,
            'Key': self.resource
        }
        dl_kwargs['Filename'] = dest
        utils.make_dest_dir(self.parts_class(dest).parent)
        self._s3_client_call('download_file', **dl_kwargs)

    def download(self, dest):
        """Downloads a directory from S3 to a destination directory.

        Args:
            dest (str): The destination path to download file to. If downloading to a directory,
                there must be a trailing slash. The directory will be created if it doesn't exist.

        Notes:
            - The destination directory will be created automatically if it doesn't exist.

            - This method downloads to paths relative to the current
              directory.

        """
        source = utils.with_trailing_slash(self)
        files_to_download = source.list()
        for f in files_to_download:
            name = self.parts_class(f[len(source):])
            ul_kwargs = {
                'Bucket': self.bucket,
                'Key': f.resource,
                'Filename': dest / name
            }
            utils.make_dest_dir(ul_kwargs['Filename'].parent)
            self._s3_client_call('download_file', **ul_kwargs)

    def upload(self, source):
        """Uploads a list of files and directories to s3.

        Note that the S3Path is treated as a directory.

        Args:
            source (List[str]): A list of source files and directories to upload to S3.

        Notes:

        - This method uploads to paths relative to the current
          directory.

        TODO:

        - Update once directory markers are implemented

        """
        files_to_upload = utils.walk_files_and_dirs([
            name for name in source
        ])
        for f in files_to_upload:
            # Skip empty directories for now
            if Path(f).isdir():
                continue
            object_name = utils.file_name_to_object_name(f)
            ul_kwargs = {
                'Bucket': self.bucket,
                'Key': self.resource / object_name if self.resource else object_name,
                'Filename': f
            }
            self._s3_client_call('upload_file', **ul_kwargs)
