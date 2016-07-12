"""
An experimental implementation of S3 in storage-utils.
"""
from multiprocessing.pool import ThreadPool
import tempfile
import threading

import boto3
from botocore import exceptions as boto_s3_exceptions

from storage_utils import exceptions
from storage_utils.base import Path
from storage_utils.obs import OBSFile
from storage_utils.obs import OBSPath
from storage_utils.obs import OBSUploadObject
from storage_utils import settings
from storage_utils import utils

# Thread-local variable used to cache the client
_thread_local = threading.local()

S3File = OBSFile


def _parse_s3_error(exc, **kwargs):
    """
    Parses botocore.exception.ClientError exceptions to throw a more
    informative exception.
    """
    http_status = exc.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
    msg = exc.response['Error'].get('Message', 'Unknown')
    if 'Bucket' in kwargs:
        msg += ' Bucket: ' + kwargs.get('Bucket')
    if 'Key' in kwargs:
        msg += ', Key: ' + kwargs.get('Key')

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
    if not hasattr(_thread_local, 's3_client'):
        session = boto3.session.Session()
        _thread_local.s3_client = session.client('s3')
    return _thread_local.s3_client


class S3Path(OBSPath):
    """
    Provides the ability to manipulate and access S3 resources
    with a similar interface to the path library.

    Right now, the client defaults to Amazon S3 endpoints, but in the
    near-future, users should be able to custom configure the S3 client.
    """
    drive = 's3://'

    @property
    def bucket(self):
        """Returns the bucket name from the path or None"""
        parts = self._get_parts()
        return parts[0] if len(parts) > 0 and parts[0] else None

    def _s3_client_call(self, method_name, *args, **kwargs):
        """
        Creates a boto3 S3 ``Client`` object and runs ``method_name``.
        """
        s3_client = _get_s3_client()
        method = getattr(s3_client, method_name)
        try:
            return method(*args, **kwargs)
        except boto_s3_exceptions.ClientError as e:
            raise _parse_s3_error(e, **kwargs)

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
        return S3File(self, mode=mode)

    def list(self, starts_with=None, limit=None, list_as_dir=False, condition=None):
        """
        List contents using the resource of the path as a prefix.

        Args:
            starts_with (str): Allows for an additional search path to be
                appended to the current swift path. The current path will be
                treated as a directory.
            limit (int): Limit the amount of results returned.
            condition (function(results) -> bool): The method will only return
                when the results matches the condition.


        Returns:
            List[S3Path]: Every path in the listing
        """
        bucket = self.bucket
        prefix = self.resource
        utils.validate_condition(condition)

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

        utils.check_condition(condition, list_results)
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
        if not self.resource:
            try:
                return bool(self._s3_client_call('head_bucket', Bucket=self.bucket))
            except exceptions.NotFoundError:
                return False
        try:
            return bool(self.stat())
        except exceptions.NotFoundError:
            pass
        try:
            return bool(utils.with_trailing_slash(self).list(limit=1))
        except exceptions.NotFoundError:
            return False

    def isabs(self):
        return True

    def isdir(self):
        """
        TODO: Check for directory markers (once implemented)
        """
        # Handle buckets separately (in case the bucket is empty)
        if not self.resource:
            try:
                return bool(self._s3_client_call('head_bucket', Bucket=self.bucket))
            except exceptions.NotFoundError:
                return False
        try:
            return bool(utils.with_trailing_slash(self).list(limit=1))
        except exceptions.NotFoundError:
            return False

    def isfile(self):
        try:
            return bool(self.stat())
        except (exceptions.NotFoundError, ValueError):
            return False

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
                raise exceptions.RemoteError('an error occurred while using rmtree: %s, Key: %s'
                                             % (response['Errors'][0].get('Message'),
                                                response['Errors'][0].get('Key')),
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

    def read_object(self):
        """"Reads an individual object."""
        body = self._s3_client_call('get_object', Bucket=self.bucket, Key=self.resource)['Body']
        return body.read()

    def write_object(self, content):
        """Writes an individual object.

        Note that this method writes the provided content to a temporary
        file before uploading.

        Args:
            content (str): The content of the object
        """
        with tempfile.NamedTemporaryFile() as fp:
            fp.write(content)
            fp.flush()
            self.upload([OBSUploadObject(fp.name, self.resource)])

    def download_object(self, dest, **kwargs):
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
            'Key': str(self.resource),
            'Filename': str(dest)
        }
        utils.make_dest_dir(self.parts_class(dest).parent)
        self._s3_client_call('download_file', **dl_kwargs)
        return dl_kwargs['Filename']

    def _download_object(self, obj):
        """Downloads a single object. Helper for threaded download."""
        name = self.parts_class(obj['source'][len(utils.with_trailing_slash(self)):])
        return obj['source'].download_object(obj['dest'] / name)

    def download(self, dest, condition=None, use_manifest=False, **kwargs):
        """Downloads a directory from S3 to a destination directory.

        Args:
            dest (str): The destination path to download file to. If downloading to a directory,
                there must be a trailing slash. The directory will be created if it doesn't exist.
            condition (function(results) -> bool): The method will only return
                when the results of download matches the condition.

        Returns:
            List[str]: A list of the downloaded file paths as strings.

        Notes:
            - The destination directory will be created automatically if it doesn't exist.

            - This method downloads to paths relative to the current
              directory.
        """
        utils.validate_condition(condition)
        source = utils.with_trailing_slash(self)
        files_to_download = [
            {'source': file, 'dest': dest}
            for file in source.list()
        ]

        options = settings.get()['s3:download']

        pool = ThreadPool(options['num_threads'])
        downloaded = pool.map(self._download_object, files_to_download)
        pool.close()
        pool.join()

        utils.check_condition(condition, downloaded)
        return downloaded

    def _upload_object(self, upload_obj):
        """Upload a single object given an OBSUploadObject."""
        if not Path(upload_obj.source).isdir():
            ul_kwargs = {
                'Bucket': self.bucket,
                'Key': str(upload_obj.object_name),
                'Filename': upload_obj.source
            }
            if upload_obj.options and 'headers' in upload_obj.options:
                ul_kwargs.update({'ExtraArgs': upload_obj.options['headers']})
            self._s3_client_call('upload_file', **ul_kwargs)
            return S3Path(self.drive + self.bucket) / ul_kwargs['Key']

    def upload(self, source, condition=None, use_manifest=False, headers=None, **kwargs):
        """Uploads a list of files and directories to s3.

        Note that the S3Path is treated as a directory.

        Args:
            source (List[str|OBSUploadObject]): A list of source files, directories, and
                OBSUploadObjects to upload to S3.
            condition (function(results) -> bool): The method will only return
                when the results of upload matches the condition.
            headers (dict): A dictionary of object headers to apply to the object.
                Headers will not be applied to OBSUploadObjects and any headers
                specified by an OBSUploadObject will override these headers.
                Headers should be specified as key-value pairs,
                e.g. {'ContentLanguage': 'en'}

        Returns:
            List[S3Path]: A list of the uploaded files as S3Paths.

        Notes:

        - This method uploads to paths relative to the current
          directory.

        TODO:
        - Update once directory markers are implemented

        """
        utils.validate_condition(condition)

        files_to_convert = utils.walk_files_and_dirs([
            name for name in source if not isinstance(name, OBSUploadObject)
        ])
        files_to_upload = [
            obj for obj in source if isinstance(obj, OBSUploadObject)
        ]
        files_to_upload.extend([
            OBSUploadObject(name,
                            (self.resource or Path('')) / utils.file_name_to_object_name(name),
                            options={'headers': headers} if headers else None)
            for name in files_to_convert
        ])

        options = settings.get()['s3:upload']

        pool = ThreadPool(options['num_threads'])
        uploaded = pool.map(self._upload_object, files_to_upload)
        pool.close()
        pool.join()

        utils.check_condition(condition, uploaded)
        return uploaded
