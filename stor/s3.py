"""
An experimental implementation of S3 in stor
"""
from functools import partial
import logging
from multiprocessing.pool import ThreadPool
import os
import tempfile
import threading

import boto3
from boto3 import exceptions as boto3_exceptions
from boto3.s3.transfer import S3Transfer
from boto3.s3.transfer import TransferConfig
from botocore import exceptions as botocore_exceptions

from stor import exceptions
from stor import settings
from stor import utils
from stor.base import Path
from stor.obs import OBSFile
from stor.obs import OBSPath
from stor.obs import OBSUploadObject

# Thread-local variable used to cache the client
_thread_local = threading.local()

logger = logging.getLogger(__name__)
progress_logger = logging.getLogger('%s.progress' % __name__)

S3File = OBSFile


def _parse_s3_error(exc, **kwargs):
    """
    Parses botocore.exception.ClientError exceptions to throw a more
    informative exception.
    """
    http_status = exc.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
    msg = exc.response['Error'].get('Message', 'Unknown')

    # Give some more info about the error's origins
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


def _get_s3_transfer(config=None):
    """Returns a boto3 S3Transfer object and initializes one if it doesn't
    already exist or if config options are different.

    Like the boto3 client, we use a different transfer for each thread/process
    for thread-safety.

    Args:
        config (dict): A dict of config options

    Returns:
        boto3.s3.S3Transfer: An instance of an S3Transfer object.
    """
    if (not hasattr(_thread_local, 's3_transfer') or
            getattr(_thread_local, 's3_transfer_config', None) != config):
        transfer_config = None
        if config:
            transfer_config = TransferConfig(**config)
        _thread_local.s3_transfer = S3Transfer(_get_s3_client(), config=transfer_config)
        _thread_local.s3_transfer_config = config
    return _thread_local.s3_transfer


class S3DownloadLogger(utils.BaseProgressLogger):
    def __init__(self, total_download_objects):
        super(S3DownloadLogger, self).__init__(progress_logger)
        self.total_download_objects = total_download_objects
        self.downloaded_bytes = 0

    def update_progress(self, result):
        """Tracks number of bytes downloaded."""
        self.downloaded_bytes += (os.path.getsize(result['dest'])
                                  if not utils.has_trailing_slash(result['source']) else 0)

    def get_start_message(self):
        return 'starting download of %s objects' % self.total_download_objects

    def get_finish_message(self):
        return 'download complete - %s' % self.get_progress_message()

    def get_progress_message(self):
        elapsed_time = self.get_elapsed_time()
        formatted_elapsed_time = self.format_time(elapsed_time)
        mb = self.downloaded_bytes / (1024 * 1024.0)
        mb_s = mb / elapsed_time.total_seconds() if elapsed_time else 0.0
        return (
            '%s/%s\t'
            '%s\t'
            '%0.2f MB\t'
            '%0.2f MB/s'
        ) % (self.num_results, self.total_download_objects, formatted_elapsed_time, mb, mb_s)


class S3UploadLogger(utils.BaseProgressLogger):
    def __init__(self, total_upload_objects):
        super(S3UploadLogger, self).__init__(progress_logger)
        self.total_upload_objects = total_upload_objects
        self.uploaded_bytes = 0

    def update_progress(self, result):
        """Keep track of total uploaded bytes by referencing the object sizes"""
        self.uploaded_bytes += (os.path.getsize(result['source'])
                                if not utils.has_trailing_slash(result['dest']) else 0)

    def get_start_message(self):
        return 'starting upload of %s objects' % self.total_upload_objects

    def get_finish_message(self):
        return 'upload complete - %s' % self.get_progress_message()

    def get_progress_message(self):
        elapsed_time = self.get_elapsed_time()
        formatted_elapsed_time = self.format_time(elapsed_time)
        mb = self.uploaded_bytes / (1024 * 1024.0)
        mb_s = mb / elapsed_time.total_seconds() if elapsed_time else 0.0
        return (
            '%s/%s\t'
            '%s\t'
            '%0.2f MB\t'
            '%0.2f MB/s'
        ) % (self.num_results, self.total_upload_objects, formatted_elapsed_time, mb, mb_s)


class S3Path(OBSPath):
    """
    Provides the ability to manipulate and access S3 resources
    with a similar interface to the path library.

    Right now, the client defaults to Amazon S3 endpoints, but in the
    near-future, users should be able to custom configure the S3 client.

    Note that any S3 object whose name ends with a ``/`` is considered to be an empty directory
    marker.
    These objects will not be downloaded and instead an empty directory will be created.
    This follows Amazon's convention as described in the
    `S3 User Guide <http://docs.aws.amazon.com/AmazonS3/latest/UG/FolderOperations.html>`_.
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
        except botocore_exceptions.ClientError as e:
            raise _parse_s3_error(e, **kwargs)

    def _get_s3_iterator(self, method_name, *args, **kwargs):
        """
        Creates a boto3 ``S3.Paginator`` object and returns an iterator
        that runs over results from ``method_name``.
        """
        s3_client = _get_s3_client()
        paginator = s3_client.get_paginator(method_name)
        return paginator.paginate(**kwargs)

    def _make_s3_transfer(self, method_name, config=None, *args, **kwargs):
        """
        Creates a boto3 ``S3Transfer`` object for doing multipart uploads
        and downloads and executes the given method.
        """
        transfer = _get_s3_transfer(config=config)
        method = getattr(transfer, method_name)
        try:
            return method(*args, **kwargs)
        except boto3_exceptions.S3UploadFailedError as e:
            raise exceptions.FailedUploadError(str(e), e)
        except boto3_exceptions.RetriesExceededError as e:
            raise exceptions.FailedDownloadError(str(e), e)

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

    def list(self,
             starts_with=None,
             limit=None,
             condition=None,
             use_manifest=False,
             # hidden args
             list_as_dir=False,
             ignore_dir_markers=False):
        """
        List contents using the resource of the path as a prefix.

        Args:
            starts_with (str): Allows for an additional search path to be
                appended to the current swift path. The current path will be
                treated as a directory.
            limit (int): Limit the amount of results returned.
            condition (function(results) -> bool): The method will only return
                when the results matches the condition.
            use_manifest (bool): Perform the list and use the data manfest file to validate
                the list.

        Returns:
            List[S3Path]: Every path in the listing

        Raises:
            RemoteError: An s3 client error occurred.
            ConditionNotMetError: Results were returned, but they did not meet the condition.
        """
        bucket = self.bucket
        prefix = self.resource
        utils.validate_condition(condition)

        if use_manifest:
            object_names = utils.get_data_manifest_contents(self)
            manifest_cond = partial(utils.validate_manifest_list, object_names)
            condition = (utils.join_conditions(condition, manifest_cond)
                         if condition else manifest_cond)

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
                        if not ignore_dir_markers or
                        (ignore_dir_markers and not utils.has_trailing_slash(result['Key']))
                    ])
                if list_as_dir and 'CommonPrefixes' in page:
                    list_results.extend([
                        path_prefix / result['Prefix']
                        for result in page['CommonPrefixes']
                    ])
        except botocore_exceptions.ClientError as e:
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

    def isdir(self):
        """
        Any S3 object whose name ends with a ``/`` is considered to be an empty directory marker.
        These objects will not be downloaded and instead an empty directory will be created.
        This follows Amazon's convention as described in the
        `S3 User Guide <http://docs.aws.amazon.com/AmazonS3/latest/UG/FolderOperations.html>`_.
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
            return self.stat() and not utils.has_trailing_slash(self)
        except (exceptions.NotFoundError, ValueError):
            return False

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
        response = {
            key: val for key, val in response.iteritems()
            if key is not 'ResponseMetadata'
        }
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

    def download_object(self, dest, config=None, **kwargs):
        """
        Downloads a file from S3 to a destination file.

        Args:
            dest (str): The destination path to download file to.

        Notes:
            - The destination directory will be created automatically if it doesn't exist.

            - This method downloads to paths relative to the current
              directory.
        """
        result = {
            'source': self,
            'dest': dest,
            'success': True
        }
        if utils.has_trailing_slash(self):
            # Handle directory markers separately
            utils.make_dest_dir(str(dest))
            return result

        dl_kwargs = {
            'bucket': self.bucket,
            'key': str(self.resource),
            'filename': str(dest),
            'config': config
        }
        utils.make_dest_dir(self.parts_class(dest).parent)
        try:
            self._make_s3_transfer('download_file', **dl_kwargs)
        except exceptions.RemoteError as e:
            result['success'] = False
            result['error'] = e
        return result

    def _download_object_worker(self, obj_params, config=None):
        """Downloads a single object. Helper for threaded download."""
        name = self.parts_class(obj_params['source'][len(utils.with_trailing_slash(self)):])
        return obj_params['source'].download_object(obj_params['dest'] / name, config=config)

    def download(self, dest, condition=None, use_manifest=False, **kwargs):
        """Downloads a directory from S3 to a destination directory.

        Args:
            dest (str): The destination path to download file to. If downloading to a directory,
                there must be a trailing slash. The directory will be created if it doesn't exist.
            condition (function(results) -> bool): The method will only return
                when the results of download matches the condition.

        Returns:
            List[S3Path]: A list of the downloaded objects.

        Notes:
        - The destination directory will be created automatically if it doesn't exist.
        - This method downloads to paths relative to the current directory.
        """
        utils.validate_condition(condition)

        if use_manifest:
            object_names = utils.get_data_manifest_contents(self)
            manifest_cond = partial(utils.validate_manifest_list, object_names)
            condition = (utils.join_conditions(condition, manifest_cond)
                         if condition else manifest_cond)

        source = utils.with_trailing_slash(self)
        files_to_download = [
            {'source': file, 'dest': dest}
            for file in source.list()
        ]

        options = settings.get()['s3:download']
        segment_size = utils.str_to_bytes(options.get('segment_size'))
        transfer_config = {
            'multipart_threshold': segment_size,
            'max_concurrency': options.get('segment_threads'),
            'multipart_chunksize': segment_size
        }
        download_w_config = partial(self._download_object_worker, config=transfer_config)

        downloaded = {'completed': [], 'failed': []}
        with S3DownloadLogger(len(files_to_download)) as dl:
            pool = ThreadPool(options['object_threads'])
            try:
                result_iter = pool.imap_unordered(download_w_config, files_to_download)
                while True:
                    try:
                        result = result_iter.next(0xFFFF)
                        if result['success']:
                            dl.add_result(result)
                            downloaded['completed'].append(result)
                        else:
                            downloaded['failed'].append(result)
                    except StopIteration:
                        break
                pool.close()
            except:
                pool.terminate()
                raise
            finally:
                pool.join()

        if downloaded['failed']:
            raise exceptions.FailedDownloadError('an error occurred while downloading', downloaded)

        utils.check_condition(condition, [r['source'] for r in downloaded['completed']])
        return downloaded

    def _upload_object(self, upload_obj, config=None):
        """Upload a single object given an OBSUploadObject."""
        if utils.has_trailing_slash(upload_obj.object_name):
            # Handle empty directories separately
            ul_kwargs = {
                'Bucket': self.bucket,
                'Key': utils.with_trailing_slash(str(upload_obj.object_name))
            }
            if upload_obj.options and 'headers' in upload_obj.options:
                ul_kwargs.update(upload_obj.options['headers'])
            s3_call = self._s3_client_call
            method = 'put_object'
        else:
            ul_kwargs = {
                'bucket': self.bucket,
                'key': str(upload_obj.object_name),
                'filename': upload_obj.source,
                'config': config
            }
            if upload_obj.options and 'headers' in upload_obj.options:
                ul_kwargs['extra_args'] = upload_obj.options['headers']
            s3_call = self._make_s3_transfer
            method = 'upload_file'

        result = {
            'source': upload_obj.source,
            'dest': S3Path(self.drive + self.bucket) / (ul_kwargs.get('key') or
                                                        ul_kwargs.get('Key')),
            'success': True
        }

        try:
            s3_call(method, **ul_kwargs)
        except exceptions.RemoteError as e:
            result['success'] = False
            result['error'] = e

        return result

    def upload(self, source, condition=None, use_manifest=False, headers=None, **kwargs):
        """Uploads a list of files and directories to s3.

        Note that the S3Path is treated as a directory.

        Note that for user-provided OBSUploadObjects, an empty directory's destination
        must have a trailing slash.

        Args:
            source (List[str|OBSUploadObject]): A list of source files, directories, and
                OBSUploadObjects to upload to S3.
            condition (function(results) -> bool): The method will only return
                when the results of upload matches the condition.
            use_manifest (bool): Generate a data manifest and validate the upload results
                are in the manifest.
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
        """
        if use_manifest and not (len(source) == 1 and os.path.isdir(source[0])):
            raise ValueError('can only upload one directory with use_manifest=True')
        utils.validate_condition(condition)

        files_to_convert = utils.walk_files_and_dirs([
            name for name in source if not isinstance(name, OBSUploadObject)
        ])
        files_to_upload = [
            obj for obj in source if isinstance(obj, OBSUploadObject)
        ]

        manifest_file_name = (Path(source[0]) / utils.DATA_MANIFEST_FILE_NAME
                              if use_manifest else None)
        resource_base = self.resource or Path('')
        files_to_upload.extend([
            OBSUploadObject(
                name,
                resource_base / (utils.with_trailing_slash(utils.file_name_to_object_name(name))
                                 if Path(name).isdir() else utils.file_name_to_object_name(name)),
                options={'headers': headers} if headers else None)
            for name in files_to_convert if name != manifest_file_name
        ])

        if use_manifest:
            # Generate the data manifest and save it remotely
            object_names = [o.object_name for o in files_to_upload]
            utils.generate_and_save_data_manifest(source[0], object_names)
            manifest_obj_name = resource_base / utils.file_name_to_object_name(manifest_file_name)
            manifest_obj = OBSUploadObject(str(manifest_file_name),
                                           manifest_obj_name,
                                           options={'headers': headers} if headers else None)
            self._upload_object(manifest_obj)

            # Make a condition for validating the upload
            manifest_cond = partial(utils.validate_manifest_list, object_names)
            condition = (utils.join_conditions(condition, manifest_cond)
                         if condition else manifest_cond)

        options = settings.get()['s3:upload']
        segment_size = utils.str_to_bytes(options.get('segment_size'))
        transfer_config = {
            'multipart_threshold': segment_size,
            'max_concurrency': options.get('segment_threads'),
            'multipart_chunksize': segment_size
        }
        upload_w_config = partial(self._upload_object, config=transfer_config)

        uploaded = {'completed': [], 'failed': []}
        with S3UploadLogger(len(files_to_upload)) as ul:
            pool = ThreadPool(options['object_threads'])
            try:
                result_iter = pool.imap_unordered(upload_w_config, files_to_upload)
                while True:
                    try:
                        result = result_iter.next(0xFFFF)
                        if result['success']:
                            ul.add_result(result)
                            uploaded['completed'].append(result)
                        else:
                            uploaded['failed'].append(result)
                    except StopIteration:
                        break
                pool.close()
            except:
                pool.terminate()
                raise
            finally:
                pool.join()

        if uploaded['failed']:
            raise exceptions.FailedUploadError('an error occurred while uploading', uploaded)

        utils.check_condition(condition, [r['dest'] for r in uploaded['completed']])
        return uploaded
