"""
Provides utilities for accessing swift object storage.

Different configuration options are available at the module level, and
these variables are documented in the default configuration.

For swift authentication, the `auth_url`, `username`, and
`password` variables are used.

For methods that take conditions, the `initial_retry_sleep`,
``num_retries``, and `retry_sleep_function` variables are used to
configure the logic around retrying when the condition is not met.
Note that these variables can also be passed to the methods themselves.

Examples:

    A basic example of configuring the swift authentication parameters
    and downloading a directory::

    >>> from stor import swift
    >>> from stor import settings
    >>> settings.update({
    ...     'swift': {
    ...         'auth_url': 'swift_auth_url.com',
    ...         'username': 'swift_user',
    ...         'password': 'swift_pass'
    ...     }
    ... })
    >>> swift_path = swift.SwiftPath('swift://tenant/container/prefix')
    >>> swift_path.download('dest_dir')

More examples and documentations for swift methods can be found under
the `SwiftPath` class.
"""
import copy
from functools import partial
from functools import wraps
import json
import logging
import os
import tempfile
import threading
import warnings

import six
from six.moves.urllib import parse
from swiftclient import exceptions as swift_exceptions
from swiftclient import service as swift_service
from swiftclient import client as swift_client
from swiftclient.utils import generate_temp_url

from stor import exceptions as stor_exceptions
from stor import is_swift_path
from stor import settings
from stor import utils
from stor.base import Path
from stor.obs import OBSFile
from stor.obs import OBSPath
from stor.obs import OBSUploadObject
from stor.posix import PosixPath
from stor.third_party.backoff import with_backoff


logger = logging.getLogger(__name__)
progress_logger = logging.getLogger('%s.progress' % __name__)

# python-swiftclient has a subtle bug in get_auth_keystone. If
# the auth_token is set beforehand and a token refresh happens,
# the invalid auth_token persists in the os_options argument,
# causing it to enter an infinite retry loop on authentication
# errors. Patch the get_auth_keystone function with one that
# clears the auth_token parameter on any Exceptions.
# Note that this behavior is tested in
# stor.tests.test_integration_swift:SwiftIntegrationTest.test_cached_auth_and_auth_invalidation
real_get_auth_keystone = swift_client.get_auth_keystone


def patched_get_auth_keystone(auth_url, user, key, os_options, **kwargs):
    try:
        return real_get_auth_keystone(auth_url, user, key, os_options, **kwargs)
    except:
        os_options.pop('auth_token', None)
        raise
swift_client.get_auth_keystone = patched_get_auth_keystone

# singleton that collects together auth tokens for storage URLs
_cached_auth_token_map = {}
_singleton_lock = threading.Lock()

# Content types that are assigned to empty directories
DIR_MARKER_TYPES = ('text/directory', 'application/directory')

# These variables are used to configure retry logic for swift.
# These variables can also be passed to the methods themselves
initial_retry_sleep = 1
"""The time to sleep before the first retry"""

# Make new Exceptions structure backwards compatible
SwiftError = stor_exceptions.RemoteError
NotFoundError = stor_exceptions.NotFoundError
ConditionNotMetError = stor_exceptions.ConditionNotMetError
ConflictError = stor_exceptions.ConflictError
UnavailableError = stor_exceptions.UnavailableError
UnauthorizedError = stor_exceptions.UnauthorizedError
SwiftFile = OBSFile
SwiftUploadObject = OBSUploadObject


def _default_retry_sleep_function(t, attempt):
    return t * 2

retry_sleep_function = _default_retry_sleep_function
"""The function that increases sleep time when retrying.

This function needs to take two integer
arguments (time slept last attempt, attempt number) and
return a time to sleep in seconds.
"""


def get_progress_logger():
    """Returns the swift progress logger"""
    return progress_logger


def _get_or_create_auth_credentials(tenant_name):
    """
    Gets the cached auth credential or creates one if none exists.

    If any auth setting is updated, all cached auth credentials are
    cleared and new auth credentials are created for the requested tenant.
    """
    options = settings.get()['swift']
    auth_url = options.get('auth_url')
    username = options.get('username')
    password = options.get('password')

    if tenant_name in _cached_auth_token_map:
        cached_params = _cached_auth_token_map[tenant_name]['params']
        if (auth_url == cached_params['auth_url'] and username == cached_params['username'] and
                password == cached_params['password']):
            return _cached_auth_token_map[tenant_name]['creds']
        else:
            _clear_cached_auth_credentials()

    storage_url, auth_token = swift_client.get_auth_keystone(
        auth_url, username, password,
        {'tenant_name': tenant_name},
    )
    creds = {
        'os_storage_url': storage_url,
        'os_auth_token': auth_token
    }
    cached_result = {
        'creds': creds,
        'params': {
            'auth_url': auth_url,
            'username': username,
            'password': password
        }
    }

    # Note: we are intentionally ignoring the rare race condition where
    # authentication starts in one thread, then settings are updated, and
    # then authentication finishes in the other.
    with _singleton_lock:
        _cached_auth_token_map[tenant_name] = cached_result
    return creds


def _clear_cached_auth_credentials():
    with _singleton_lock:
        _cached_auth_token_map.clear()


class FailedUploadError(stor_exceptions.FailedUploadError, UnavailableError):
    """Thrown when an upload fails because of availability issues.

    The exception hierarchy is different for swift because FailedUploadError is nearly always a 503
    in Swift world (and thus almost always retry-able)"""
    pass


class AuthenticationError(SwiftError):
    """Thrown when a client has improper authentication credentials.

    Swiftclient throws this error when trying to authenticate with
    the keystone client. This is similar to a 401 HTTP response.
    """
    pass


class InconsistentDownloadError(SwiftError):
    """Thrown when an etag or content length does not match.

    Currently, we experience this during periods when the cluster is under
    heavy load, potentially because of unexpectedly quick terminations"""
    pass


class ConfigurationError(SwiftError):
    """Thrown when swift is not configured properly.

    Swift needs either module-level or environment authentication
    variables set in order to be configured.
    """
    pass


def _swift_retry(exceptions=None):
    """Allows `SwiftPath` methods to take optional retry configuration
    parameters for doing retry logic
    """
    def decorated(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = kwargs.pop('num_retries', settings.get()['swift']['num_retries'])
            initial_sleep = kwargs.pop('initial_retry_sleep',
                                       initial_retry_sleep)
            sleep_function = kwargs.pop('retry_sleep_function',
                                        retry_sleep_function)

            return with_backoff(func,
                                exceptions=exceptions,
                                sleep_function=sleep_function,
                                retries=retries,
                                initial_sleep=initial_sleep)(*args, **kwargs)
        return wrapper
    return decorated


def _swiftclient_error_to_descriptive_exception(exc):
    """Converts swiftclient errors to more descriptive exceptions with
    transaction ID"""
    # SwiftErrors catch Client exceptions and store them in the
    # 'exception' attribute. Try to get the client exception
    # here if there is one so that its http status can be
    # examined to throw specific exceptions.
    client_exception = getattr(exc, 'exception', exc)

    http_status = getattr(client_exception, 'http_status', None)
    exc_str = str(exc)
    exc_headers = getattr(client_exception, 'http_response_headers', None)
    if exc_headers and exc_headers.get('X-Trans-Id'):
        exc_str += ' X-Trans-Id: %s' % exc_headers['X-Trans-Id']
    if http_status == 403:
        # pass through of the InvalidObjectState error from S3 (but we only get exception message)
        if "storage class" in exc_str:
            raise stor_exceptions.ObjectInColdStorageError(exc_str, exc)
        else:
            logger.error('unauthorized error in swift operation - %s', exc_str)
            return UnauthorizedError(exc_str, exc)
    elif http_status == 404:
        return NotFoundError(exc_str, exc)
    elif http_status == 409:
        return ConflictError(exc_str, exc)
    elif http_status == 503:
        logger.error('unavailable error in swift operation - %s', exc_str)
        return UnavailableError(exc_str, exc)
    elif 'reset contents for reupload' in exc_str:
        # When experiencing HA issues, we sometimes encounter a
        # ClientException from swiftclient during upload. The exception
        # is thrown here -
        # https://github.com/openstack/python-swiftclient/blob/84d110c63ecf671377d4b2338060e9b00da44a4f/swiftclient/client.py#L1625  # nopep8
        # Treat this as a FailedUploadError
        logger.error('upload error in swift put_object operation - %s', exc_str)
        six.raise_from(FailedUploadError(exc_str, exc), exc)
    elif 'Unauthorized.' in exc_str:
        # Swiftclient catches keystone auth errors at
        # https://github.com/openstack/python-swiftclient/blob/master/swiftclient/client.py#L536 # nopep8
        # Parse the message since they don't bubble the exception or
        # provide more information
        logger.warning('auth error in swift operation - %s', exc_str)
        six.raise_from(AuthenticationError(exc_str, exc), exc)
    elif 'md5sum != etag' in exc_str or 'read_length != content_length' in exc_str:
        # We encounter this error when cluster is under heavy
        # replication load (at least that's the theory). So retry and
        # ensure we track consistency errors
        logger.error('Hit consistency issue. Likely related to'
                     ' cluster load: %s', exc_str)
        return InconsistentDownloadError(exc_str, exc)
    else:
        logger.error('unexpected swift error - %s', exc_str)
        return SwiftError(exc_str, exc)


def _propagate_swift_exceptions(func):
    """Bubbles all swift exceptions as `SwiftError` classes
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (swift_service.SwiftError,
                swift_exceptions.ClientException) as e:
            six.raise_from(_swiftclient_error_to_descriptive_exception(e), e)
    return wrapper


def _retry_on_cached_auth_err(func):
    """Retry a function with cleared auth credentials on AuthenticationError"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except AuthenticationError:
            logger.info('auth failed, retrying with cleared auth cache')
            _clear_cached_auth_credentials()
            return func(*args, **kwargs)
    return wrapper


def _validate_manifest_upload(expected_objs, upload_results):
    """
    Given a list of expected object names and a list of dictionaries of
    `SwiftPath.upload` results, verify that all expected objects are in
    the upload results.
    """
    uploaded_objs = {
        r['object']
        for r in upload_results
        if r['success'] and r['action'] in ('upload_object', 'create_dir_marker')
    }
    return set(expected_objs).issubset(uploaded_objs)


def _validate_manifest_download(expected_objs, download_results):
    """
    Given a list of expected object names and a list of dictionaries of
    `SwiftPath.download` results, verify that all expected objects are in
    the download results.
    """
    downloaded_objs = {
        r['object']
        for r in download_results
        if r['success'] and r['action'] in ('download_object',)
    }
    return set(expected_objs).issubset(downloaded_objs)


class SwiftDownloadLogger(utils.BaseProgressLogger):
    def __init__(self):
        super(SwiftDownloadLogger, self).__init__(progress_logger)
        self.downloaded_bytes = 0

    def update_progress(self, result):
        """Tracks number of bytes downloaded.

        The ``read_length`` property in swift download results contains the
        total size of the object
        """
        self.downloaded_bytes += result.get('read_length', 0)

    def add_result(self, result):
        """Only add results to progress if they are ``download_object`` actions.

        The ``download_object`` action encompasses creating an empty directory
        marker
        """
        if result.get('action', None) == 'download_object':
            super(SwiftDownloadLogger, self).add_result(result)

    def get_start_message(self):
        return 'starting download'

    def get_finish_message(self):
        return 'download complete - %s' % self.get_progress_message()

    def get_progress_message(self):
        elapsed_time = self.get_elapsed_time()
        formatted_elapsed_time = self.format_time(elapsed_time)
        mb = self.downloaded_bytes / (1024 * 1024.0)
        mb_s = mb / elapsed_time.total_seconds() if elapsed_time else 0
        return (
            '%s\t'
            '%s\t'
            '%0.2f MB\t'
            '%0.2f MB/s'
        ) % (self.num_results, formatted_elapsed_time, mb, mb_s)


class SwiftUploadLogger(utils.BaseProgressLogger):
    def __init__(self, total_upload_objects, upload_object_sizes):
        super(SwiftUploadLogger, self).__init__(progress_logger)
        self.total_upload_objects = total_upload_objects
        self.upload_object_sizes = upload_object_sizes
        self.uploaded_bytes = 0

    def update_progress(self, result):
        """Keep track of total uploaded bytes by referencing the object sizes"""
        self.uploaded_bytes += self.upload_object_sizes.get(result['path'], 0)

    def add_result(self, result):
        """Only add results if they are ``upload_object`` and ``create_dir_marker``
        actions"""
        if result.get('action', None) in ('upload_object', 'create_dir_marker'):
            super(SwiftUploadLogger, self).add_result(result)

    def get_start_message(self):
        return 'starting upload of %s objects' % self.total_upload_objects

    def get_finish_message(self):
        return 'upload complete - %s' % self.get_progress_message()

    def get_progress_message(self):
        elapsed_time = self.get_elapsed_time()
        formatted_elapsed_time = self.format_time(elapsed_time)
        mb = self.uploaded_bytes / (1024 * 1024.0)
        mb_s = mb / elapsed_time.total_seconds() if elapsed_time else 0
        return (
            '%s/%s\t'
            '%s\t'
            '%0.2f MB\t'
            '%0.2f MB/s'
        ) % (self.num_results, self.total_upload_objects, formatted_elapsed_time, mb, mb_s)


class SwiftPath(OBSPath):
    """
    Provides the ability to manipulate and access resources on swift
    with a similar interface to the path library.
    """
    drive = 'swift://'

    def is_segment_container(self):
        """True if this path is a segment container"""
        container = self.container
        if not self.resource and container:
            return (container.startswith('.segments') or
                    container.endswith('_segments') or
                    container.endswith('+segments'))
        else:
            return False

    @property
    def tenant(self):
        """Returns the tenant name from the path or return None"""
        parts = self._get_parts()
        return parts[0] if len(parts) > 0 and parts[0] else None

    @property
    def container(self):
        """Returns the container name from the path or None."""
        parts = self._get_parts()
        return parts[1] if len(parts) > 1 and parts[1] else None

    @property
    def resource(self):
        """Returns the resource as a ``PosixPath`` object or None.

        A resource can be a single object or a prefix to objects.
        Note that it's important to keep the trailing slash in a resource
        name for prefix queries.
        """
        parts = self._get_parts()
        joined_resource = '/'.join(parts[2:]) if len(parts) > 2 else None

        return self.parts_class(joined_resource) if joined_resource else None

    def _get_swift_connection_options(self, **options):
        """Returns options for constructing ``SwiftService`` and
        ``Connection`` objects.

        Args:
            options: Additional options that are directly passed
                into connection options.

        Raises:
            ConfigurationError: The needed swift environment variables
                aren't set.
        """
        global_options = settings.get()['swift']
        auth_url = global_options.get('auth_url')
        username = global_options.get('username')
        password = global_options.get('password')

        if not username or not password or not auth_url:
            raise ConfigurationError((
                'OS_AUTH_URL, OS_USERNAME, and OS_PASSWORD environment vars '
                'must be set for swift authentication. The username, password '
                'and auth_url settings variables may also be set with settings.update.'
            ))

        # Set additional options on top of what was passed in
        options['os_tenant_name'] = self.tenant
        options['os_auth_url'] = auth_url
        options['os_username'] = username
        options['os_password'] = password
        options.update(**_get_or_create_auth_credentials(self.tenant))

        # Merge options with global and local ones
        options = dict(swift_service._default_global_options,
                       **dict(swift_service._default_local_options,
                              **options))
        swift_service.process_options(options)
        return options

    def _get_swift_service(self, **options):
        """Initialize a swift service based on the path.

        Uses the tenant name of the path and an auth url to instantiate
        the swift service. The ``OS_AUTH_URL`` environment variable is used
        as the authentication url if set, otherwise the default auth
        url setting is used.

        Args:
            options: Additional options that are directly passed
                into swift service creation.

        Returns:
            swiftclient.service.SwiftService: The service instance.
        """
        conn_opts = self._get_swift_connection_options(**options)
        return swift_service.SwiftService(conn_opts)

    def _get_swift_connection(self, **options):
        """Initialize a swift client connection based on the path.

        The python-swiftclient package offers a couple ways to access data,
        with a raw Connection object being a lower-level interface to swift.
        For cases like reading individual objects, a raw Connection object
        is easier to utilize.

        Args:
            options: Additional options that are directly passed
                into swift connection creation.

        Returns:
            swiftclient.client.Connection: The connection instance.
        """
        conn_opts = self._get_swift_connection_options(**options)
        return swift_service.get_conn(conn_opts)

    @_retry_on_cached_auth_err
    @_propagate_swift_exceptions
    def _swift_connection_call(self, method_name, *args, **kwargs):
        """Instantiates a ``Connection`` object and runs ``method_name``.

        Note that obtaining the connection and doing the call in one method
        is intentional (instead of allowing the user to get a connection and
        then call a method on it directly). This is because sometimes a cached
        auth token can expire, causing a method to fail. If a method is retried,
        we want it to always get the swift connection again so that it will also
        re-auth in the case of an expired or invalid auth token.
        """
        connection = self._get_swift_connection()
        method = getattr(connection, method_name)
        return method(*args, **kwargs)

    @_retry_on_cached_auth_err
    @_propagate_swift_exceptions
    def _swift_service_call(self, method_name, *args, **kwargs):
        """Instantiates a ``SwiftService`` object and runs ``method_name``.

        Note that getting the swift service and doing the call in the same method
        is done for the same reasons explained in ``_swift_connection_call``.
        """
        method_options = copy.copy(kwargs)
        service_options = copy.deepcopy(method_options.pop('_service_options', {}))
        service_progress_logger = method_options.pop('_progress_logger', None)
        service = self._get_swift_service(**service_options)
        method = getattr(service, method_name)
        results_iter = method(*args, **method_options)

        results_iter = [results_iter] if isinstance(results_iter, dict) else results_iter

        results = []
        for r in results_iter:
            if 'error' in r:
                http_status = getattr(r['error'], 'http_status', None)
                if not http_status or http_status >= 400:
                    raise r['error']
            results.append(r)
            if service_progress_logger:
                service_progress_logger.add_result(r)

        return results

    @_swift_retry(exceptions=(NotFoundError, UnavailableError,
                              InconsistentDownloadError, UnauthorizedError))
    def read_object(self):
        """Reads an individual object from OBS.

        Returns:
            bytes: the raw bytes from the object on OBS.

        This method retries ``num_retries`` times if swift is unavailable or if
        the object is not found. View
        `module-level documentation <swiftretry>` for more
        information about configuring retry logic at the module or method
        level.
        """
        headers, content = self._swift_connection_call('get_object',
                                                       self.container,
                                                       self.resource)
        return content

    def temp_url(self, lifetime=300, method='GET', inline=True, filename=None):
        """Obtains a temporary URL to an object.

        Args:
            lifetime (int): The time (in seconds) the temporary
                URL will be valid
            method (str): The HTTP method that can be used on
                the temporary URL
            inline (bool, default True): If False, URL will have a
                Content-Disposition header that causes browser to download as
                attachment.
            filename (str, optional): A urlencoded filename to use for
                attachment, otherwise defaults to object name
        """
        global_options = settings.get()['swift']
        auth_url = global_options.get('auth_url')
        temp_url_key = global_options.get('temp_url_key')

        if not self.resource:
            raise ValueError('can only create temporary URL on object')
        if not temp_url_key:
            raise ValueError(
                'a temporary url key must be set with settings.update '
                'or by setting the OS_TEMP_URL_KEY environment variable')
        if not auth_url:
            raise ValueError(
                'an auth url must be set with settings.update '
                'or by setting the OS_AUTH_URL environment variable')

        obj_path = '/v1/%s' % self[len(self.drive):]
        # Generate the temp url using swifts helper. Note that this method is ONLY
        # useful for obtaining the temp_url_sig and the temp_url_expires parameters.
        # These parameters will be used to construct a properly-escaped temp url
        obj_url = generate_temp_url(obj_path, lifetime, temp_url_key, method)
        query_begin = obj_url.rfind('temp_url_sig', 0, len(obj_url))
        obj_url_query = obj_url[query_begin:]
        obj_url_query = dict(parse.parse_qsl(obj_url_query))

        query = ['temp_url_sig=%s' % obj_url_query['temp_url_sig'],
                 'temp_url_expires=%s' % obj_url_query['temp_url_expires']]
        if inline:
            query.append('inline')
        if filename:
            query.append('filename=%s' % parse.quote(filename))

        auth_url_parts = parse.urlparse(auth_url)
        return parse.urlunparse((auth_url_parts.scheme,
                                 auth_url_parts.netloc,
                                 parse.quote(obj_path),
                                 auth_url_parts.params,
                                 '&'.join(query),
                                 auth_url_parts.fragment))

    def write_object(self, content, **swift_upload_args):
        """Writes an individual object.

        Note that this method writes the provided content to a temporary
        file before uploading. This allows us to reuse code from swift's
        uploader (static large object support, etc.).

        For information about the retry logic of this method, view
        `SwiftPath.upload`.

        Args:
            content (bytes): raw bytes to write to OBS
            **swift_upload_args: Keyword arguments to pass to
                `SwiftPath.upload`
        """
        if not isinstance(content, bytes):  # pragma: no cover
            warnings.warn('future versions of stor will raise a TypeError if content is not bytes')
        mode = 'wb' if type(content) == bytes else 'wt'
        with tempfile.NamedTemporaryFile(mode=mode) as fp:
            fp.write(content)
            fp.flush()
            suo = OBSUploadObject(fp.name, object_name=self.resource)
            return self.upload([suo], **swift_upload_args)

    def open(self, mode='r', encoding=None, swift_upload_options=None):
        """Opens a `SwiftFile` that can be read or written to.

        For examples of reading and writing opened objects, view
        `SwiftFile`.

        Args:
            mode (str): The mode of object IO. Currently supports reading
                ("r" or "rb") and writing ("w", "wb")
            encoding (str): text encoding to use. Defaults to
                ``locale.getpreferredencoding(False)`` (Python 3 only)
            swift_upload_options (dict): DEPRECATED (use `stor.settings.use()`
                instead). A dictionary of arguments that will be
                passed as keyword args to `SwiftPath.upload` if any writes
                occur on the opened resource.

        Returns:
            SwiftFile: The swift object.

        Raises:
            SwiftError: A swift client error occurred.
        """
        swift_upload_options = swift_upload_options or {}
        return SwiftFile(self, mode=mode, encoding=encoding, **swift_upload_options)

    @_swift_retry(exceptions=(ConditionNotMetError, UnavailableError))
    def list(self,
             starts_with=None,
             limit=None,
             condition=None,
             use_manifest=False,
             # intentionally not documented
             list_as_dir=False,
             ignore_segment_containers=True,
             ignore_dir_markers=False):
        """List contents using the resource of the path as a prefix.

        This method retries ``num_retries`` times if swift is unavailable
        or if the number of objects returned does not match the
        ``condition`` condition. View
        `module-level documentation <swiftretry>` for more
        information about configuring retry logic at the module or method
        level.

        Args:
            starts_with (str): Allows for an additional search path to
                be appended to the resource of the swift path. Note that the
                current resource path is treated as a directory
            limit (int): Limit the amount of results returned
            condition (function(results) -> bool): The method will only return
                when the results matches the condition.
            use_manifest (bool): Perform the list and use the data manfest file to validate
                the list.

        Returns:
            List[SwiftPath]: Every path in the listing.

        Raises:
            SwiftError: A swift client error occurred.
            ConditionNotMetError: Results were returned, but they did not
                meet the condition.
        """
        tenant = self.tenant
        prefix = self.resource
        full_listing = limit is None
        utils.validate_condition(condition)

        if use_manifest:
            object_names = utils.get_data_manifest_contents(self)
            manifest_cond = partial(utils.validate_manifest_list, object_names)
            condition = (utils.join_conditions(condition, manifest_cond)
                         if condition else manifest_cond)

        # When starts_with is provided, treat the resource as a
        # directory that has the starts_with parameter after it. This allows
        # the user to specify a path like tenant/container/mydir
        # and do an additional glob on a directory-like structure
        if starts_with:
            prefix = prefix / starts_with if prefix else starts_with

        list_kwargs = {
            'full_listing': full_listing,
            'limit': limit,
            'prefix': prefix
        }
        if self.container and list_as_dir:
            # Swift doesn't allow a delimeter for tenant-level listing,
            # however, this isn't a problem for list_as_dir since a tenant
            # will only have containers
            list_kwargs['delimiter'] = '/'

            # Ensure that the prefix has a '/' at the end of it for listdir
            list_kwargs['prefix'] = utils.with_trailing_slash(list_kwargs['prefix'])

        if self.container:
            results = self._swift_connection_call('get_container',
                                                  self.container,
                                                  **list_kwargs)
        else:
            results = self._swift_connection_call('get_account',
                                                  **list_kwargs)

        result_objs = results[1]
        if ignore_dir_markers:
            result_objs = [r for r in result_objs if r.get('content_type') not in DIR_MARKER_TYPES]

        path_pre = SwiftPath('%s%s' % (self.drive, tenant)) / (self.container or '')
        paths = list({
            path_pre / (r.get('name') or r['subdir'].rstrip('/'))
            for r in result_objs
        })

        if ignore_segment_containers:
            paths = [p for p in paths if not p.is_segment_container()]

        utils.check_condition(condition, paths)
        return paths

    def listdir(self, ignore_segment_containers=True):
        """Lists the path as a dir, returning top-level directories and files

        For information about retry logic on this method, see
        `SwiftPath.list`
        """
        return self.list(list_as_dir=True, ignore_segment_containers=ignore_segment_containers)

    @_swift_retry(exceptions=(ConditionNotMetError, UnavailableError))
    def glob(self, pattern, condition=None):
        """Globs all objects in the path with the pattern.

        This glob is only compatible with patterns that end in * because of
        swift's inability to do searches other than prefix queries.

        Note that this method assumes the current resource is a directory path
        and treats it as such. For example, if the user has a swift path of
        swift://tenant/container/my_dir (without the trailing slash), this
        method will perform a swift query with a prefix of mydir/pattern.

        This method retries ``num_retries`` times if swift is unavailable or if
        the number of globbed patterns does not match the ``condition``
        condition. View `module-level documentation <swiftretry>`
        for more information about configuring retry logic at the module or
        method level.

        Args:
            pattern (str): The pattern to match. The pattern can only have
                up to one '*' at the end.
            condition (function(results) -> bool): The method will only return
                when the number of results matches the condition.

        Returns:
            List[SwiftPath]: Every matching path.

        Raises:
            SwiftError: A swift client error occurred.
            ConditionNotMetError: Results were returned, but they did not
                meet the condition.
        """
        if pattern.count('*') > 1:
            raise ValueError('multiple pattern globs not supported')
        if '*' in pattern and not pattern.endswith('*'):
            raise ValueError('only prefix queries are supported')
        utils.validate_condition(condition)

        with settings.use({'swift': {'num_retries': 0}}):
            paths = self.list(starts_with=pattern.replace('*', ''))

        utils.check_condition(condition, paths)
        return paths

    @_swift_retry(exceptions=UnavailableError)
    def first(self):
        """Returns the first result from the list results of the path

        See `module-level retry <swiftretry>` documentation for more.

        Raises:
            SwiftError: A swift client error occurred.
        """
        with settings.use({'swift': {'num_retries': 0}}):
            results = self.list(limit=1)
        return results[0] if results else None

    @_swift_retry(exceptions=UnavailableError)
    def exists(self):
        """Checks existence of the path.

        Returns True if the path exists, False otherwise.

        See `module-level retry <swiftretry>` documentation for more.

        Returns:
            bool: True if the path exists, False otherwise.

        Raises:
            SwiftError: A non-404 swift client error occurred.
        """
        try:
            # first see if there is a specific corresponding object
            with settings.use({'swift': {'num_retries': 0}}):
                self.stat()
            return True
        except NotFoundError:
            pass
        try:
            # otherwise we could be a directory, so try to grab first
            # file/subfolder
            with settings.use({'swift': {'num_retries': 0}}):
                return bool(utils.with_trailing_slash(self).first())
        except NotFoundError:
            return False

    @_swift_retry(exceptions=(UnavailableError, InconsistentDownloadError,
                              UnauthorizedError))
    def download_object(self, out_file):
        """Downloads a single object to an output file.

        This method retries ``num_retries`` times if swift is unavailable.
        View module-level documentation for more information about configuring
        retry logic at the module or method level.

        Args:
            out_file (str): The output file

        Raises:
            ValueError: This method was called on a path that has no
                container or object
        """
        if not self.resource:
            raise ValueError('can only call download_object on object path')

        self._swift_service_call('download',
                                 container=self.container,
                                 objects=[self.resource],
                                 options={'out_file': out_file})

    @_swift_retry(exceptions=(UnavailableError, InconsistentDownloadError,
                              UnauthorizedError))
    def download_objects(self,
                         dest,
                         objects):
        """Downloads a list of objects to a destination folder.

        Note that this method takes a list of complete relative or absolute
        paths to objects (in contrast to taking a prefix). If any object
        does not exist, the call will fail with partially downloaded objects
        residing in the destination path.

        This method retries ``num_retries`` times if swift is unavailable.
        View `module-level documentation <swiftretry>` for more information
        about configuring retry logic at the module or method level.

        Args:
            dest (str): The destination folder to download to. The directory
                will be created if it doesnt exist.
            objects (List[str|PosixPath|SwiftPath]): The list of objects to
                download. The objects can paths relative to the download path
                or absolute swift paths. Any absolute swift path must be
                children of the download path

        Returns:
            dict: A mapping of all requested ``objs`` to their location on
                disk

        Raises:
            ValueError: This method was called on a path that has no
                container

        Examples:

            To download a objects to a ``dest/folder`` destination::

                from stor import path
                p = path('swift://tenant/container/dir/')
                results = p.download_objects('dest/folder', ['subdir/f1.txt',
                                                             'subdir/f2.txt'])
                print results
                {
                    'subdir/f1.txt': 'dest/folder/subdir/f1.txt',
                    'subdir/f2.txt': 'dest/folder/subdir/f2.txt'
                }

            To download full swift paths relative to a download path::

                from stor import path
                p = path('swift://tenant/container/dir/')
                results = p.download_objects('dest/folder', [
                    'swift://tenant/container/dir/subdir/f1.txt',
                    'swift://tenant/container/dir/subdir/f2.txt'
                ])
                print results
                {
                    'swift://tenant/container/dir/subdir/f1.txt': 'dest/folder/subdir/f1.txt',
                    'swift://tenant/container/dir/subdir/f2.txt': 'dest/folder/subdir/f2.txt'
                }
        """
        if not self.container:
            raise ValueError('cannot call download_objects on tenant with no container')

        # Convert requested download objects to full object paths
        obj_base = self.resource or PosixPath('')
        objs_to_download = {
            obj: SwiftPath(obj).resource if is_swift_path(obj) else obj_base / obj
            for obj in objects
        }

        for obj in objs_to_download:
            if is_swift_path(obj) and not obj.startswith(utils.with_trailing_slash(self)):
                raise ValueError(
                    '"%s" must be child of download path "%s"' % (obj, self))

        options = settings.get()['swift:download']

        service_options = {
            'object_dd_threads': options['object_threads'],
            'container_threads': options['container_threads']
        }
        download_options = {
            'prefix': utils.with_trailing_slash(self.resource),
            'out_directory': dest,
            'remove_prefix': True,
            'skip_identical': options['skip_identical'],
            'shuffle': options['shuffle']
        }
        results = self._swift_service_call('download',
                                           _service_options=service_options,
                                           container=self.container,
                                           objects=list(objs_to_download.values()),
                                           options=download_options)
        results = {r['object']: r['path'] for r in results}

        # Return results mapped back to their input name
        return {obj: results[objs_to_download[obj]] for obj in objects}

    @_swift_retry(exceptions=(ConditionNotMetError, UnavailableError,
                              InconsistentDownloadError))
    def download(self,
                 dest,
                 condition=None,
                 use_manifest=False):
        """Downloads a directory to a destination.

        This method retries ``num_retries`` times if swift is unavailable or if
        the returned download result does not match the ``condition``
        condition. View `module-level documentation <stor.swift>`
        for more information about configuring retry logic at the module or
        method level.

        Note that the destintation directory will be created automatically if
        it doesn't exist.

        Args:
            dest (str): The destination directory to download results to.
                The directory will be created if it doesn't exist.
            condition (function(results) -> bool): The method will only return
                when the results of download matches the condition. In the event of the
                condition never matching after retries, partially downloaded
                results will not be deleted. Note that users are not expected to write
                conditions for download without an understanding of the structure of the results.
            use_manifest (bool): Perform the download and use the data manfest file to validate
                the download.

        Raises:
            SwiftError: A swift client error occurred.
            ConditionNotMetError: Results were returned, but they did not
                meet the condition.

        Returns:
            List[dict]: A list of every operation performed in the download by the
                underlying swift client
        """
        if not self.container:
            raise ValueError('cannot call download on tenant with no container')
        utils.validate_condition(condition)

        if use_manifest:
            # Do a full list with the manifest before the download. This will retry until
            # all results in the manifest can be listed, which helps ensure the download
            # can be performed without having to be retried
            self.list(use_manifest=True)
            object_names = utils.get_data_manifest_contents(self)
            manifest_cond = partial(_validate_manifest_download, object_names)
            condition = (utils.join_conditions(condition, manifest_cond)
                         if condition else manifest_cond)

        options = settings.get()['swift:download']
        service_options = {
            'object_dd_threads': options['object_threads'],
            'container_threads': options['container_threads']
        }
        download_options = {
            'prefix': utils.with_trailing_slash(self.resource),
            'out_directory': dest,
            'remove_prefix': True,
            'skip_identical': options['skip_identical'],
            'shuffle': options['shuffle']
        }
        with SwiftDownloadLogger() as dl:
            results = self._swift_service_call('download',
                                               self.container,
                                               options=download_options,
                                               _progress_logger=dl,
                                               _service_options=service_options)

        utils.check_condition(condition, results)
        return results

    @_swift_retry(exceptions=(ConditionNotMetError, UnavailableError,
                              UnauthorizedError))
    def upload(self,
               to_upload,
               condition=None,
               use_manifest=False,
               headers=None):
        """Uploads a list of files and directories to swift.

        This method retries ``num_retries`` times if swift is unavailable or if
        the returned upload result does not match the ``condition``
        condition. View `module-level documentation <stor.swift>`
        for more information about configuring retry logic at the module or
        method level.

        For example::

            with path('/path/to/upload/dir'):
                path('swift://tenant/container').upload(['.'])

        Notes:

            - This method upload on paths relative to the current
              directory. In order to load files relative to a target directory,
              use path as a context manager to change the directory.

            - When large files are split into segments, they are uploaded
              to a segment container named .segments_${container_name}

        Args:
            to_upload (List): A list of file names, directory names, or
                OBSUploadObject objects to upload.
            condition (function(results) -> bool): The method will only return
                when the results of upload matches the condition. In the event of the
                condition never matching after retries, partially uploaded
                results will not be deleted. Note that users are not expected to write
                conditions for upload without an understanding of the structure of the results.
            use_manifest (bool): Generate a data manifest and validate the upload results
                are in the manifest. In case of a single directory being uploaded, the
                manifest file will be created inside this directory. For example::

                    stor.Path('swift://AUTH_foo/bar').upload(['logs'], use_manifest=True)

                The manifest will be located at
                ``swift://AUTH_foo/bar/logs/.data_manifest.csv``

                Alternatively, when multiple directories are uploaded, manifest file will
                be created in the current directory. For example::

                    stor.Path('swift://AUTH_foo/bar').upload(
                        ['logs', 'test.txt'], use_manifest=True)

                The manifest will be located at
                ``swift://AUTH_foo/bar/.data_manifest.csv``

            headers (List[str]): A list of object headers to apply to every object. Note
                that these are not applied if passing OBSUploadObjects directly to upload.
                Headers must be specified as a list of colon-delimited strings,
                e.g. ['X-Delete-After:1000']

        Raises:
            SwiftError: A swift client error occurred.
            ConditionNotMetError: The ``condition`` argument did not pass after ``num_retries``
                or the ``use_manifest`` option was turned on and the upload results could not
                be verified. Partially uploaded results are not deleted.

        Returns:
            List[dict]: A list of every operation performed in the upload by the
                underlying swift client
        """
        if not self.container:
            raise ValueError('must specify container when uploading')
        utils.validate_condition(condition)

        swift_upload_objects = [
            name for name in to_upload
            if isinstance(name, OBSUploadObject)
        ]
        all_files_to_upload = utils.walk_files_and_dirs([
            name for name in to_upload
            if not isinstance(name, OBSUploadObject)
        ])

        # Convert everything to swift upload objects and prepend the relative
        # resource directory to uploaded results. Ignore the manifest file in the case of
        # since it will be uploaded individually
        if use_manifest:
            if len(to_upload) == 1 and os.path.isdir(to_upload[0]):
                manifest_path_prefix = Path(to_upload[0])
            else:
                manifest_path_prefix = Path('.')
            manifest_file_name = manifest_path_prefix / utils.DATA_MANIFEST_FILE_NAME
        else:
            manifest_path_prefix = None
            manifest_file_name = None
        resource_base = utils.with_trailing_slash(self.resource) or PosixPath('')
        upload_object_options = {'header': headers or []}
        swift_upload_objects.extend([
            OBSUploadObject(f,
                            object_name=resource_base / utils.file_name_to_object_name(f),
                            options=upload_object_options)
            for f in all_files_to_upload if f != manifest_file_name
        ])

        if use_manifest:
            # Generate the data manifest and save it remotely
            object_names = [o.object_name for o in swift_upload_objects]
            utils.generate_and_save_data_manifest(manifest_path_prefix, object_names)
            manifest_obj_name = resource_base / utils.file_name_to_object_name(manifest_file_name)
            manifest_obj = OBSUploadObject(manifest_file_name,
                                           object_name=manifest_obj_name,
                                           options=upload_object_options)
            self._swift_service_call('upload', self.container, [manifest_obj])

            # Make a condition for validating the upload
            manifest_cond = partial(_validate_manifest_upload, object_names)
            condition = (utils.join_conditions(condition, manifest_cond)
                         if condition else manifest_cond)

        options = settings.get()['swift:upload']
        service_options = {
            'object_uu_threads': options['object_threads'],
            'segment_threads': options['segment_threads']
        }
        upload_options = {
            'segment_size': options['segment_size'],
            'use_slo': options['use_slo'],
            'segment_container': '.segments_%s' % self.container,
            'leave_segments': options['leave_segments'],
            'changed': options['changed'],
            'skip_identical': options['skip_identical'],
            'checksum': options['checksum']
        }
        with SwiftUploadLogger(len(swift_upload_objects), all_files_to_upload) as ul:
            results = self._swift_service_call('upload',
                                               self.container,
                                               swift_upload_objects,
                                               options=upload_options,
                                               _progress_logger=ul,
                                               _service_options=service_options)

        utils.check_condition(condition, results)
        return results

    @_swift_retry(exceptions=(UnavailableError, UnauthorizedError))
    def remove(self):
        """Removes a single object.

        This method retries ``num_retries`` times if swift is unavailable.
        View `module-level documentation <swiftretry>` for more
        information about configuring retry logic at the module or method
        level.

        Raises:
            ValueError: The path is invalid.
            SwiftError: A swift client error occurred.
        """
        if not self.container or not self.resource:
            raise ValueError('path must contain a container and resource to '
                             'remove a single file')

        return self._swift_service_call('delete',
                                        self.container,
                                        [self.resource])

    @_swift_retry(exceptions=(UnavailableError, ConflictError,
                              ConditionNotMetError, UnauthorizedError))
    def rmtree(self):
        """Removes a resource and all of its contents.
        This method retries ``num_retries`` times if swift is unavailable.
        View `module-level documentation <stor.swift>` for more
        information about configuring retry logic at the module or method
        level.

        Note that when removing a container, the associated segment container
        will also be removed if it exists. So, if one removes
        ``swift://tenant/container``, ``swift://tenant/container_segments``
        will also be deleted.

        Note:
            Calling rmtree on a directory marker will delete everything under the
            directory marker but not the marker itself.

        Raises:
            SwiftError: A swift client error occurred.
            ConditionNotMetError: Listing the objects after rmtree returns results,
                indicating something went wrong with the rmtree operation
        """
        if not self.container:
            raise ValueError('swift path must include container for rmtree')

        # Ensure that we treat this path as a dir
        to_delete = utils.with_trailing_slash(self)

        deleting_segments = 'segments' in self.container
        if deleting_segments:
            logger.warning('performing rmtree with segment container "%s". '
                           'This could cause issues when accessing objects '
                           'referencing those segments. Note that segments '
                           'and segment containers are automatically deleted '
                           'when their associated objects or containers are '
                           'deleted.', self.container)

        options = settings.get()

        service_options = {
            'object_dd_threads': options['swift:delete']['object_threads']
        }

        def _ignore_not_found(service_call):
            """Ignores 404 errors when performing a swift service call"""
            def wrapper(*args, **kwargs):
                try:
                    return service_call(*args, **kwargs)
                except NotFoundError:
                    return []
            return wrapper

        if not to_delete.resource:
            results = _ignore_not_found(self._swift_service_call)('delete',
                                                                  to_delete.container,
                                                                  _service_options=service_options)
            # Try to delete a segment container since swift client does not
            # do this automatically
            if not deleting_segments:
                segment_containers = ('%s_segments' % to_delete.container,
                                      '.segments_%s' % to_delete.container,
                                      '%s+segments' % to_delete.container)
                for segment_container in segment_containers:
                    _ignore_not_found(self._swift_service_call)('delete',
                                                                segment_container,
                                                                _service_options=service_options)
        else:
            objs_to_delete = [p.resource for p in to_delete.list()]
            results = _ignore_not_found(self._swift_service_call)('delete',
                                                                  self.container,
                                                                  objs_to_delete,
                                                                  _service_options=service_options)

        # Verify that all objects have been deleted before returning. Otherwise try deleting again
        with settings.use({'swift': {'num_retries': 0}}):
            _ignore_not_found(to_delete.list)(condition=lambda results: len(results) == 0)

        return results

    @_swift_retry(exceptions=(UnavailableError, UnauthorizedError))
    def remove_container(self):
        """
        Remove swift container if it's not empty.
        """
        if not self.container:
            raise ValueError('swift path must include container for remove_container')
        if self.resource:
            raise ValueError('swift path must not include resource for remove_container')

        return self._swift_connection_call('delete_container', self.container)

    @_swift_retry(exceptions=UnavailableError)
    def stat(self):
        """Performs a stat on the path.

        Note that the path can be a tenant, container, or
        object. Using ``stat`` on a directory of objects will
        produce a `NotFoundError`.

        This method retries ``num_retries`` times if swift is unavailable.
        View `module-level documentation <swiftretry>` for more information
        about configuring retry logic at the module or method level.

        The return value is dependent on if the path points to a tenant,
        container, or object.

        For tenants, an example return dictionary is the following::

            {
                'headers': {
                    'content-length': u'0',
                    'x-account-storage-policy-3xreplica-container-count': u'4655',
                    'x-account-meta-temp-url-key': 'temp_url_key',
                    'x-account-object-count': '428634',
                    'x-timestamp': '1450732238.21562',
                    'x-account-storage-policy-3xreplica-bytes-used': '175654877752',
                    'x-trans-id': u'transaction_id',
                    'date': u'Wed, 06 Apr 2016 18:30:28 GMT',
                    'x-account-bytes-used': '175654877752',
                    'x-account-container-count': '4655',
                    'content-type': 'text/plain; charset=utf-8',
                    'accept-ranges': 'bytes',
                    'x-account-storage-policy-3xreplica-object-count': '428634'
                },
                'Account': 'AUTH_seq_upload_prod',
                # The number of containers in the tenant
                'Containers': 31,
                # The number of objects in the tenant
                'Objects': '19955615',
                # The total bytes used in the tenant
                'Bytes': '24890576770484',
                'Containers-in-policy-"3xreplica"': '31',
                'Objects-in-policy-"3xreplica"': '19955615',
                'Bytes-in-policy-"3xreplica"': '24890576770484',
                # The tenant ACLs. An empty dict is returned if the user
                # does not have admin privileges on the tenant
                'Account-Access': {
                    'admin': ['swft_labprod_admin'],
                    'read-only': ['seq_upload_rnd','swft_labprod'],
                    'read-write': ['svc_svc_seq']
                }
            }

        For containers, an example return dictionary is the following::

            {
                'headers': {
                    'content-length': '0',
                    'x-container-object-count': '99',
                    'accept-ranges': 'bytes',
                    'x-storage-policy': '3xReplica',
                    'date': 'Wed, 06 Apr 2016 18:32:47 GMT',
                    'x-timestamp': '1457631707.95682',
                    'x-trans-id': 'transaction_id',
                    'x-container-bytes-used': '5389060',
                    'content-type': 'text/plain; charset=utf-8'
                },
                'Account': 'AUTH_seq_upload_prod',
                'Container': '2016-01',
                # The number of objects in the container
                'Objects': '43868',
                # The size of all objects in the container
                'Bytes': '55841489571',
                # Read and write ACLs for the container
                'Read-ACL': '',
                'Write-ACL': '',
                'Sync-To': '',
                'Sync-Key': ''
            }

        For objects, an example return dictionary is the following::

            {
                'headers': {
                    'content-length': '0',
                    'x-delete-at': '1459967915',
                    'accept-ranges': 'bytes',
                    'last-modified': 'Wed, 06 Apr 2016 18:21:56 GMT',
                    'etag': 'd41d8cd98f00b204e9800998ecf8427e',
                    'x-timestamp': '1459966915.96956',
                    'x-trans-id': 'transaction_id',
                    'date': 'Wed, 06 Apr 2016 18:33:48 GMT',
                    'content-type': 'text/plain',
                    'x-object-meta-mtime': '1459965986.000000'
                },
                'Account': 'AUTH_seq_upload_prod',
                'Container': '2016-01',
                'Object': PosixPath('object.txt'),
                'Content-Type': 'application/octet-stream',
                # The size of the object
                'Content-Length': '112',
                # The last time the object was modified
                'Last-Modified': 'Fri, 15 Jan 2016 05:22:46 GMT',
                # The MD5 checksum of the object. NOTE that if a large
                # object is uploaded in segments that this will be the
                # checksum of the *manifest* file of the object
                'ETag': '87f0b7f04557315e6d1e6db21742d31c',
                'Manifest': None
            }

        Raises:
            NotFoundError: When the tenant, container, or
                object can't be found.
        """
        stat_objects = [self.resource] if self.resource else None
        result = self._swift_service_call('stat',
                                          container=self.container,
                                          objects=stat_objects)[0]

        stat_values = {
            k.replace(' ', '-'): v
            for k, v in result['items']
        }
        stat_values['headers'] = result['headers']

        if result['action'] == 'stat_account':
            # Load account ACLs
            stat_values['Access-Control'] = json.loads(
                result['headers'].get('x-account-access-control', '{}'))

        return stat_values

    def getsize(self):
        """Returns content-length of object in Swift.

        Note that for containers / tenants, there will be no content-length, in
        which case this function returns 0 (``os.path.getsize`` has no
        contract)"""
        return int(self.stat().get('Content-Length', 0))

    @_swift_retry(exceptions=(UnavailableError, UnauthorizedError))
    def post(self, options=None):
        """Post operations on the path.

        This method retries ``num_retries`` times if swift is unavailable.
        View `module-level documentation <swiftretry>` for more
        information about configuring retry logic at the module or method
        level.

        Args:
            options (dict): A dictionary containing options to override the
                global options specified during the service object creation.
                These options are applied to all post operations performed by
                this call, unless overridden on a per object basis. Possible
                options are given below::

                    {
                        'meta': [],   # Meta values will be prefixed with X-Object-Meta
                                      # (or X-Container*  X-Account* depending on path type)
                        'header': [], # Header values will not be manipulated like meta values
                                      # when being posted.
                        'read_acl': None,   # For containers only
                        'write_acl': None,  # For containers only
                        'sync_to': None,    # For containers only
                        'sync_key': None    # For containers only
                    }

        Raises:
            SwiftError: A swift client error occurred.
        """
        return self._swift_service_call('post',
                                        container=self.container,
                                        objects=[self.resource] if self.resource else None,
                                        options=options)

    def _noop(attr_name):
        def wrapper(self):
            return type(self)(self)
        wrapper.__name__ = attr_name
        wrapper.__doc__ = 'No-op for %r' % attr_name
        return wrapper

    abspath = _noop('abspath')
    realpath = _noop('realpath')
    expanduser = _noop('expanduser')

    def isdir(self):
        if not self.resource:
            return self.exists()
        try:
            if utils.with_trailing_slash(self).first():
                return True
        except NotFoundError:
            pass
        try:
            return 'directory' in self.stat().get('Content-Type', '')
        except NotFoundError:
            pass
        return False

    def isfile(self):
        """Checks the object exists & is not a directory sentinel on Swift.
        """
        try:
            return self.resource and 'directory' not in self.stat().get('Content-Type', '')
        except NotFoundError:
            return False

    @_swift_retry(exceptions=UnavailableError)
    def walkfiles(self, pattern=None):
        """Iterates over listed files that match an optional pattern.

        Args:
            pattern (str, optional): Only return files that match this pattern

        Returns:
            Iter[SwiftPath]: All files that match the optional pattern. Swift directory
                markers are not returned.
        """
        with settings.use({'swift': {'num_retries': 0}}):
            for f in self.list(ignore_dir_markers=True):
                if pattern is None or f.fnmatch(pattern):
                    yield f

    def to_url(self):
        """Returns URI for object (based on storage URL)

        Returns:
            str: the HTTP url to the object
        Raises:
            UnauthorizedError: if we cannot authenticate to get a storage URL"""
        storage_url = _get_or_create_auth_credentials(self.tenant)['os_storage_url']
        return six.text_type(os.path.join(*filter(None,
                                                  [storage_url, self.container, self.resource])))
