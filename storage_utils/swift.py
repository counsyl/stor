"""
Provides utilities for accessing swift object storage.

Different configuration options are available at the module level, and
these variables are documented under their module declarations.

For swift authentication, the `auth_url`, `username`, and
`password` variables are used.

For methods that take conditions, the `initial_retry_sleep`,
`num_retries`, and `retry_sleep_function` variables are used to
configure the logic around retrying when the condition is not met.
Note that these variables can also be passed to the methods themselves.

Examples:

    A basic example of configuring the swift authentication parameters
    and downloading a directory::

    >>> from storage_utils import swift
    >>> swift.update_settings(auth_url='swift_auth_url.com',
    ...                       username='swift_user',
    ...                       password='swift_pass')
    >>> swift_path = swift.SwiftPath('swift://tenant/container/prefix')
    >>> swift_path.download('dest_dir')

More examples and documentations for swift methods can be found under
the `SwiftPath` class.
"""
from backoff.backoff import with_backoff
from cached_property import cached_property
import cStringIO
import copy
from functools import partial
from functools import wraps
import json
import logging
import os
import posixpath
import tempfile
import threading
import urlparse

import storage_utils
from storage_utils import is_swift_path
from storage_utils.base import Path
from storage_utils import utils
from storage_utils.posix import PosixPath
from swiftclient import exceptions as swift_exceptions
from swiftclient import service as swift_service
from swiftclient import client as swift_client
from swiftclient.service import SwiftUploadObject
from swiftclient.utils import generate_temp_url


logger = logging.getLogger(__name__)


# Default module-level settings for swift authentication.
# If None, the OS_AUTH_URL, OS_USERNAME, or OS_PASSWORD
# environment variables will be used
auth_url = os.environ.get('OS_AUTH_URL', 'https://swift.counsyl.com/auth/v2.0')
"""The swift authentication URL

If not set, the ``OS_AUTH_URL`` environment variable will be used. If
that is not set, the ``DEFAULT_AUTH_URL`` global constant will
be used.
"""

username = os.environ.get('OS_USERNAME')
"""The swift username

If not set, the ``OS_USERNAME`` environment variable will be used.
"""

password = os.environ.get('OS_PASSWORD')
"""The swift password

If not set, the ``OS_PASSWORD`` environment variable will be used.
"""

# singleton that collects together auth tokens for storage URLs
_cached_auth_token_map = {}
_singleton_lock = threading.Lock()

temp_url_key = os.environ.get('OS_TEMP_URL_KEY')
"""The key for generating temporary URLs

If not set, the ``OS_TEMP_URL_KEY environment variable will be used.
"""

# Make the default segment size for static large objects be 1GB
DEFAULT_SEGMENT_SIZE = 1024 * 1024 * 1024

# Name for the data manifest file when using the use_manifest option
# for upload/download
DATA_MANIFEST_FILE_NAME = '.data_manifest.csv'

# These variables are used to configure retry logic for swift.
# These variables can also be passed to the methods themselves
initial_retry_sleep = 1
"""The time to sleep before the first retry"""

num_retries = os.environ.get('OS_NUM_RETRIES', 0)
"""The number of times to retry

Uses the ``OS_NUM_RETRIES`` environment variable or defaults to 0
"""


def _default_retry_sleep_function(t, attempt):
    return t * 2

retry_sleep_function = _default_retry_sleep_function
"""The function that increases sleep time when retrying.

This function needs to take two integer
arguments (time slept last attempt, attempt number) and
return a time to sleep in seconds.
"""


def update_settings(**settings):
    """Updates swift module settings.

    All settings should be updated using this function.

    Args:
        **settings: keyword arguments for settings. Can
            include settings for auth_url, username,
            password, temp_url_key, initial_retry_sleep,
            num_retries, and retry_sleep_function.

    Examples:

        To update all authentication settings at once, do::

            from storage_utils import swift
            swift.update_settings(auth_url='swift_auth_url.com',
                                  username='swift_user',
                                  password='swift_pass')

        To update every retry setting at once, do::

            from storage_utils import swift
            swift.update_settings(initial_retry_sleep=5,
                                  num_retries=5,
                                  retry_sleep_function=custom_retry_func)

    """
    for setting, value in settings.items():
        if setting not in globals():
            raise ValueError('invalid setting "%s"' % setting)
        globals()[setting] = value

    _clear_cached_auth_credentials()


def _get_or_create_auth_credentials(tenant_name):
    try:
        return _cached_auth_token_map[tenant_name]
    except KeyError:
        storage_url, auth_token = swift_client.get_auth_keystone(
            auth_url, username, password,
            {'tenant_name': tenant_name},
        )
        creds = {
            'os_storage_url': storage_url,
            'os_auth_token': auth_token
        }

        # Note: we are intentionally ignoring the rare race condition where
        # authentication starts in one thread, then settings are updated, and
        # then authentication finishes in the other.
        with _singleton_lock:
            _cached_auth_token_map[tenant_name] = creds
        return creds


def _clear_cached_auth_credentials():
    with _singleton_lock:
        _cached_auth_token_map.clear()


class SwiftError(Exception):
    """The top-level exception thrown for any swift errors

    The 'caught_exception' attribute of the exception must
    be examined in order to inspect the swift exception that
    happened. A swift exception can either be a
    ``SwiftError`` (thrown by ``swiftclient.service``) or a
    ``ClientError`` (thrown by ``swiftclient.client``)
    """
    def __init__(self, message, caught_exception=None):
        super(SwiftError, self).__init__(message)
        self.caught_exception = caught_exception


class NotFoundError(SwiftError):
    """Thrown when a 404 response is returned from swift"""
    pass


class UnavailableError(SwiftError):
    """Thrown when a 503 response is returned from swift"""
    pass


class FailedUploadError(UnavailableError):
    """Thrown when an upload fails because of availability issues"""
    pass


class UnauthorizedError(SwiftError):
    """Thrown when a 403 response is returned from swift"""
    pass


class AuthenticationError(SwiftError):
    """Thrown when a client has improper authentication credentials.

    Swiftclient throws this error when trying to authenticate with
    the keystone client. This is similar to a 401 HTTP response.
    """
    pass


class ConflictError(SwiftError):
    """Thrown when a 409 response is returned from swift

    This error is thrown when deleting a container and
    some object storage nodes report that the container
    has objects while others don't.
    """
    pass


class ConfigurationError(SwiftError):
    """Thrown when swift is not configured properly.

    Swift needs either module-level or environment authentication
    variables set in order to be configured.
    """
    pass


class ConditionNotMetError(SwiftError):
    """Thrown when a condition is not met."""
    pass


def _validate_condition(condition):
    """Verifies condition is a function that takes one argument"""
    if condition is None:
        return
    if not (hasattr(condition, '__call__') and hasattr(condition, '__code__')):
        raise ValueError('condition must be callable')
    if condition.__code__.co_argcount != 1:
        raise ValueError('condition must take exactly one argument')


def _check_condition(condition, results):
    """Checks the results against the condition.

    Raises:
        ConditionNotMetError: If the condition returns False
    """
    if condition is None:
        return

    condition_met = condition(results)
    if not condition_met:
        raise ConditionNotMetError('condition not met')


def _swift_retry(exceptions=None):
    """Allows `SwiftPath` methods to take optional retry configuration
    parameters for doing retry logic
    """
    def decorated(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = kwargs.pop('num_retries', num_retries)
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


def _propagate_swift_exceptions(func):
    """Bubbles all swift exceptions as `SwiftError` classes
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (swift_service.SwiftError,
                swift_exceptions.ClientException) as e:
            # SwiftErrors catch Client exceptions and store them in the
            # 'exception' attribute. Try to get the client exception
            # here if there is one so that its http status can be
            # examined to throw specific exceptions.
            client_exception = getattr(e, 'exception', e)

            http_status = getattr(client_exception, 'http_status', None)
            if http_status == 403:
                logger.error('unauthorized error in swift operation - %s', str(e))
                raise UnauthorizedError(str(e), e)
            elif http_status == 404:
                raise NotFoundError(str(e), e)
            elif http_status == 409:
                raise ConflictError(str(e), e)
            elif http_status == 503:
                logger.error('unavailable error in swift operation - %s', str(e))
                raise UnavailableError(str(e), e)
            elif 'reset contents for reupload' in str(e):
                # When experiencing HA issues, we sometimes encounter a
                # ClientException from swiftclient during upload. The exception
                # is thrown here -
                # https://github.com/openstack/python-swiftclient/blob/84d110c63ecf671377d4b2338060e9b00da44a4f/swiftclient/client.py#L1625  # nopep8
                # Treat this as a FailedUploadError
                logger.error('upload error in swift put_object operation - %s', str(e))
                raise FailedUploadError(str(e), e)
            elif 'Unauthorized.' in str(e):
                # Swiftclient catches keystone auth errors at
                # https://github.com/openstack/python-swiftclient/blob/master/swiftclient/client.py#L536 # nopep8
                # Parse the message since they don't bubble the exception or
                # provide more information
                logger.warning('auth error in swift operation - %s', str(e))
                raise AuthenticationError(str(e), e)
            else:
                logger.error('unexpected swift error - %s', str(e))
                raise SwiftError(str(e), e)

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


def _delegate_to_buffer(attr_name, valid_modes=None):
    "Factory function that delegates file-like properties to underlying buffer"
    def wrapper(self, *args, **kwargs):
        if self.closed:
            raise ValueError('I/O operation on closed file')
        if valid_modes and self.mode not in valid_modes:
            raise TypeError('SwiftFile must be in modes %s to %r' %
                            (valid_modes, attr_name))
        func = getattr(self._buffer, attr_name)
        return func(*args, **kwargs)
    wrapper.__name__ = attr_name
    wrapper.__doc__ = getattr(cStringIO.StringIO(), attr_name).__doc__
    return wrapper


def _with_trailing_slash(p):
    "Returns a path with a single trailing slash or None if not a path"
    if not p:
        return p
    return type(p)(p.rstrip('/') + '/')


def file_name_to_object_name(p):
    """Given a file path, consruct its object name.

    Any relative or absolute directory markers at the beginning of
    the path will be stripped, for example::

        ../../my_file -> my_file
        ./my_dir -> my_dir
        .hidden_dir/file -> .hidden_dir/file
        /absolute_dir -> absolute_dir

    Note that windows paths will have their back slashes changed to
    forward slashes::

        C:\\my\\windows\\file -> my/windows/file

    Args:
        p (str): The input path

    Returns:
        PosixPath: The object name. An empty path will be returned in
            the case of the input path only consisting of absolute
            or relative directory markers (i.e. '/' -> '', './' -> '')
    """
    os_sep = os.path.sep
    p_parts = Path(p).expand().splitdrive()[1].split(os_sep)
    obj_start = next((i for i, part in enumerate(p_parts) if part not in ('', '..', '.')), None)
    parts_class = SwiftPath.parts_class
    return parts_class('') if obj_start is None else parts_class('/'.join(p_parts[obj_start:]))


def _generate_and_save_data_manifest(manifest_dir, data_manifest_contents):
    """Generates a data manifest for a given directory and saves it.

    Args:
        manifest_dir (str): The directory in which the manifest will be saved
        data_manifest_contents (List[str]): The list of all objects that will
            be part of the manifest.
    """
    manifest_file_name = Path(manifest_dir) / DATA_MANIFEST_FILE_NAME
    with storage_utils.open(manifest_file_name, 'w') as out_file:
        contents = '\n'.join(data_manifest_contents) + '\n'
        out_file.write(contents)


def _get_data_manifest_contents(manifest_dir):
    """Reads the manifest file and returns a set of expected files"""
    manifest = manifest_dir / DATA_MANIFEST_FILE_NAME
    with storage_utils.open(manifest, 'r') as manifest_file:
        return [
            f.strip() for f in manifest_file.readlines() if f.strip()
        ]


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


def _validate_manifest_list(expected_objs, list_results):
    """
    Given a list of expected object names and `SwiftPath.list` results,
    verify all expected objects are in the listed results
    """
    listed_objs = {r.resource for r in list_results}
    return set(expected_objs).issubset(listed_objs)


def join_conditions(*conditions):
    def wrapper(results):
        return all(f(results) for f in conditions)
    return wrapper


class SwiftFile(object):
    """Provides methods for reading and writing swift objects returned by
    `SwiftPath.open`.

    Objects are retrieved from `SwiftPath.open`. For example::

        obj = path('swift://tenant/container/object').open(mode='r')
        contents = obj.read()

    The above opens an object and reads its contents. To write to an
    object::

        obj = path('swift://tenant/container/object').open(mode='w')
        obj.write('hello ')
        obj.write('world')
        obj.close()

    Note that the writes will not be commited until the object has been
    closed. It is recommended to use `SwiftPath.open` as a context manager
    to avoid forgetting to close the resource::

        with path('swift://tenant/container/object').open(mode='r') as obj:
            obj.write('Hello world!')

    One can modify which parameters are use for swift upload when writing
    by passing them to ``open`` like so::

        with path('..').open(mode='r', swift_upload_options={'use_slo': True}) as obj:
            obj.write('Hello world!')

    In the above, `SwiftPath.upload` will be passed ``use_slo=False`` when
    the upload happens
    """
    closed = False
    _READ_MODES = ('r', 'rb')
    _WRITE_MODES = ('w', 'wb')
    _VALID_MODES = _READ_MODES + _WRITE_MODES

    def __init__(self, swift_path, mode='r', **swift_upload_kwargs):
        """Initializes a swift object

        Args:
            swift_path (SwiftPath): The path that represents an individual
                object
            mode (str): The mode of the resource. Can be "r" and "rb" for
                reading the resource and "w" and "wb" for writing the
                resource.
            **swift_upload_kwargs: The arguments that will be passed to
                `SwiftPath.upload` if writes occur on the object
        """
        if mode not in self._VALID_MODES:
            raise ValueError('invalid mode for swift file: %r' % mode)
        self._swift_path = swift_path
        self.mode = mode
        self._swift_upload_kwargs = swift_upload_kwargs

    def __enter__(self):
        if self.closed:
            raise ValueError('I/O operation on closed file.')
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    @cached_property
    def _buffer(self):
        "Cached buffer of data read from or to be written to Object Storage"
        if self.mode in ('r', 'rb'):
            return cStringIO.StringIO(self._swift_path._read_object())
        elif self.mode in ('w', 'wb'):
            return cStringIO.StringIO()
        else:
            raise ValueError('cannot obtain buffer in mode: %r' % self.mode)

    seek = _delegate_to_buffer('seek', valid_modes=_VALID_MODES)
    tell = _delegate_to_buffer('tell', valid_modes=_VALID_MODES)

    read = _delegate_to_buffer('read', valid_modes=_READ_MODES)
    readlines = _delegate_to_buffer('readlines', valid_modes=_READ_MODES)
    readline = _delegate_to_buffer('readline', valid_modes=_READ_MODES)

    write = _delegate_to_buffer('write', valid_modes=_WRITE_MODES)
    writelines = _delegate_to_buffer('writelines', valid_modes=_WRITE_MODES)
    truncate = _delegate_to_buffer('truncate', valid_modes=_WRITE_MODES)

    @property
    def name(self):
        return self._swift_path

    def close(self):
        if self.mode in self._WRITE_MODES:
            self.flush()
        self._buffer.close()
        self.closed = True
        del self.__dict__['_buffer']

    def flush(self):
        """Flushes the write buffer to swift (if it exists)"""
        if self.mode not in self._WRITE_MODES:
            raise TypeError("SwiftFile must be in modes %s to 'flush'" %
                            (self._WRITE_MODES,))
        if self._buffer.tell():
            self._swift_path.write_object(self._buffer.getvalue(),
                                          **self._swift_upload_kwargs)


class SwiftPath(Path):
    """
    Provides the ability to manipulate and access resources on swift
    with a similar interface to the path library.
    """
    swift_drive = 'swift://'
    path_module = posixpath
    # Parts of a swift path are returned using this class
    parts_class = PosixPath

    def __init__(self, swift):
        """Validates swift path is in the proper format.

        Args:
            swift (str): A path that matches the format of
                "swift://{tenant_name}/{container_name}/{rest_of_path}".
                The "swift://" prefix is required in the path.
        """
        if not hasattr(swift, 'startswith') or not swift.startswith(self.swift_drive):
            raise ValueError('path must have %s (got %r)' % (self.swift_drive, swift))
        return super(SwiftPath, self).__init__(swift)

    def __repr__(self):
        return '%s("%s")' % (type(self).__name__, self)

    def is_ambiguous(self):
        """Returns true if it cannot be determined if the path is a
        file or directory
        """
        return not self.endswith('/') and not self.ext

    def is_segment_container(self):
        """True if this path is a segment container"""
        container = self.container
        if not self.resource and container:
            return container.startswith('.segments_') or container.endswith('_segments')
        else:
            return False

    @property
    def name(self):
        """The name of the path, mimicking path.py's name property"""
        return self.parts_class(super(SwiftPath, self).name)

    @property
    def parent(self):
        """The parent of the path, mimicking path.py's parent property"""
        return self.path_class(super(SwiftPath, self).parent)

    def _get_parts(self):
        """Returns the path parts (excluding swift://) as a list of strings."""
        if len(self) > len(self.swift_drive):
            return self[len(self.swift_drive):].split('/')
        else:
            return []

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
        global username, password, auth_url

        if not username or not password or not auth_url:
            raise ConfigurationError((
                'OS_AUTH_URL, OS_USERNAME, and OS_PASSWORD environment vars '
                'must be set for swift authentication. The username, password '
                'and auth_url settings variables may also be set with update_settings.'
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
        """Instantiates a ``Connection`` object and runs ``method_name``."""
        connection = self._get_swift_connection()
        method = getattr(connection, method_name)
        return method(*args, **kwargs)

    @_retry_on_cached_auth_err
    @_propagate_swift_exceptions
    def _swift_service_call(self, method_name, *args, **kwargs):
        """Instantiates a ``SwiftService`` object and runs ``method_name``."""
        method_options = copy.deepcopy(kwargs)
        service_options = method_options.pop('_service_options', {})
        service = self._get_swift_service(**service_options)
        method = getattr(service, method_name)
        results = method(*args, **method_options)

        results = [results] if isinstance(results, dict) else list(results)
        for r in results:
            if 'error' in r:
                http_status = getattr(r['error'], 'http_status', None)
                if not http_status or http_status >= 400:
                    raise r['error']

        return results

    @_swift_retry(exceptions=(NotFoundError, UnavailableError))
    def _read_object(self):
        """Reads an individual object.

        This method retries `num_retries` times if swift is unavailable or if
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
        global temp_url_key, auth_url

        if not self.resource:
            raise ValueError('can only create temporary URL on object')
        if not temp_url_key:
            raise ValueError(
                'a temporary url key must be set with update_settings(temp_url_key=<KEY> '
                'or by setting the OS_TEMP_URL_KEY environment variable')
        if not auth_url:
            raise ValueError(
                'an auth url must be set with update_settings(auth_url=<AUTH_URL> '
                'or by setting the OS_AUTH_URL environment variable')

        obj_path = '/v1/%s' % self[len(self.swift_drive):]
        obj_url = generate_temp_url(obj_path, lifetime, temp_url_key, method)
        obj_url_parts = urlparse.urlparse(obj_url)
        query = obj_url_parts.query.split('&')
        if inline:
            query.append('inline')
        if filename:
            query.append('filename=%s' % filename)

        auth_url_parts = urlparse.urlparse(auth_url)
        return urlparse.urlunparse((auth_url_parts.scheme,
                                    auth_url_parts.netloc,
                                    obj_url_parts.path,
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
            content (str): The content of the object
            **swift_upload_args: Keyword arguments to pass to
                `SwiftPath.upload`
        """
        with tempfile.NamedTemporaryFile() as fp:
            fp.write(content)
            fp.flush()
            suo = SwiftUploadObject(fp.name, object_name=self.resource)
            return self.upload([suo], **swift_upload_args)

    def open(self, mode='r', swift_upload_options=None):
        """Opens a `SwiftFile` that can be read or written to.

        For examples of reading and writing opened objects, view
        `SwiftFile`.

        Args:
            mode (str): The mode of object IO. Currently supports reading
                ("r" or "rb") and writing ("w", "wb")
            swift_upload_options (dict): A dictionary of arguments that will be
                passed as keyword args to `SwiftPath.upload` if any writes
                occur on the opened resource.

        Returns:
            SwiftFile: The swift object.

        Raises:
            SwiftError: A swift client error occurred.
        """
        swift_upload_options = swift_upload_options or {}
        return SwiftFile(self, mode=mode, **swift_upload_options)

    @_swift_retry(exceptions=(ConditionNotMetError, UnavailableError))
    def list(self,
             starts_with=None,
             limit=None,
             condition=None,
             use_manifest=False,
             # intentionally not documented
             list_as_dir=False,
             ignore_segment_containers=True):
        """List contents using the resource of the path as a prefix.

        This method retries `num_retries` times if swift is unavailable
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
        _validate_condition(condition)

        if use_manifest:
            object_names = _get_data_manifest_contents(self)
            manifest_cond = partial(_validate_manifest_list, object_names)
            condition = join_conditions(condition, manifest_cond) if condition else manifest_cond

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
            list_kwargs['prefix'] = _with_trailing_slash(list_kwargs['prefix'])

        if self.container:
            results = self._swift_connection_call('get_container',
                                                  self.container,
                                                  **list_kwargs)
        else:
            results = self._swift_connection_call('get_account',
                                                  **list_kwargs)

        path_pre = SwiftPath('%s%s' % (self.swift_drive, tenant)) / (self.container or '')
        paths = list({
            path_pre / (r.get('name') or r['subdir'].rstrip('/'))
            for r in results[1]
        })

        if ignore_segment_containers:
            paths = [p for p in paths if not p.is_segment_container()]

        _check_condition(condition, paths)
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

        This method retries `num_retries` times if swift is unavailable or if
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
        _validate_condition(condition)

        paths = self.list(starts_with=pattern.replace('*', ''), num_retries=0)

        _check_condition(condition, paths)
        return paths

    @_swift_retry(exceptions=UnavailableError)
    def first(self):
        """Returns the first result from the list results of the path

        See `module-level retry <swiftretry>` documentation for more.

        Raises:
            SwiftError: A swift client error occurred.
        """
        results = self.list(limit=1, num_retries=0)
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
            self.stat(num_retries=0)
            return True
        except NotFoundError:
            pass
        try:
            # otherwise we could be a directory, so try to grab first
            # file/subfolder
            return bool(_with_trailing_slash(self).first(num_retries=0))
        except NotFoundError:
            return False

    @_swift_retry(exceptions=(UnavailableError))
    def _download_object(self, out_file):
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

    @_swift_retry(exceptions=(UnavailableError))
    def download_objects(self,
                         dest,
                         objects,
                         object_threads=10,
                         container_threads=10,
                         skip_identical=False,
                         shuffle=True):
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
            object_threads (int, default 10): The amount of threads to use for downloading
                objects.
            container_threads (int, default 10): The amount of threads to use for
                downloading containers.
            skip_identical (bool, default False): Skip downloading files that are identical
                on both sides. Note this incurs reading the contents of all pre-existing local
                files.
            shuffle (bool, default True): When downloading a complete container,
                download order is randomised in order to reduce the load on individual drives
                when doing threaded downloads. Disable this option to submit download jobs to
                the thread pool in the order they are listed in the object store.

        Returns:
            dict: A mapping of all requested ``objs`` to their location on
                disk

        Raises:
            ValueError: This method was called on a path that has no
                container

        Examples:

            To download a objects to a ``dest/folder`` destination::

                from storage_utils import path
                p = path('swift://tenant/container/dir/')
                results = p.download_objects('dest/folder', ['subdir/f1.txt',
                                                             'subdir/f2.txt'])
                print results
                {
                    'subdir/f1.txt': 'dest/folder/subdir/f1.txt',
                    'subdir/f2.txt': 'dest/folder/subdir/f2.txt'
                }

            To download full swift paths relative to a download path::

                from storage_utils import path
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
            if is_swift_path(obj) and not obj.startswith(_with_trailing_slash(self)):
                raise ValueError(
                    '"%s" must be child of download path "%s"' % (obj, self))

        service_options = {
            'object_dd_threads': object_threads,
            'container_threads': container_threads
        }
        download_options = {
            'prefix': _with_trailing_slash(self.resource),
            'out_directory': dest,
            'remove_prefix': True,
            'skip_identical': skip_identical,
            'shuffle': shuffle
        }
        results = self._swift_service_call('download',
                                           _service_options=service_options,
                                           container=self.container,
                                           objects=objs_to_download.values(),
                                           options=download_options)
        results = {r['object']: r['path'] for r in results}

        # Return results mapped back to their input name
        return {obj: results[objs_to_download[obj]] for obj in objects}

    @_swift_retry(exceptions=(ConditionNotMetError, UnavailableError))
    def download(self,
                 dest,
                 object_threads=10,
                 container_threads=10,
                 skip_identical=False,
                 shuffle=True,
                 condition=None,
                 use_manifest=False):
        """Downloads a directory to a destination.

        This method retries `num_retries` times if swift is unavailable or if
        the returned download result does not match the ``condition``
        condition. View `module-level documentation <storage_utils.swift>`
        for more information about configuring retry logic at the module or
        method level.

        Note that the destintation directory will be created automatically if
        it doesn't exist.

        Args:
            dest (str): The destination directory to download results to.
                The directory will be created if it doesn't exist.
            object_threads (int): The amount of threads to use for downloading
                objects.
            container_threads (int): The amount of threads to use for
                downloading containers.
            skip_identical (bool, default False): Skip downloading files that are identical
                on both sides. Note this incurs reading the contents of all pre-existing local
                files.
            shuffle (bool, default True): When downloading a complete container,
                download order is randomised in order to reduce the load on individual drives
                when doing threaded downloads. Disable this option to submit download jobs to
                the thread pool in the order they are listed in the object store.
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
        _validate_condition(condition)

        if use_manifest:
            # Do a full list with the manifest before the download. This will retry until
            # all results in the manifest can be listed, which helps ensure the download
            # can be performed without having to be retried
            self.list(use_manifest=True)
            object_names = _get_data_manifest_contents(self)
            manifest_cond = partial(_validate_manifest_download, object_names)
            condition = join_conditions(condition, manifest_cond) if condition else manifest_cond

        service_options = {
            'object_dd_threads': object_threads,
            'container_threads': container_threads
        }
        download_options = {
            'prefix': _with_trailing_slash(self.resource),
            'out_directory': dest,
            'remove_prefix': True,
            'skip_identical': skip_identical,
            'shuffle': shuffle
        }
        results = self._swift_service_call('download',
                                           self.container,
                                           _service_options=service_options,
                                           options=download_options)

        _check_condition(condition, results)
        return results

    @_swift_retry(exceptions=(ConditionNotMetError, UnavailableError))
    def upload(self,
               to_upload,
               segment_size=DEFAULT_SEGMENT_SIZE,
               object_threads=10,
               segment_threads=10,
               use_slo=True,
               leave_segments=False,
               changed=False,
               skip_identical=False,
               checksum=True,
               condition=None,
               use_manifest=False):
        """Uploads a list of files and directories to swift.

        This method retries `num_retries` times if swift is unavailable or if
        the returned upload result does not match the ``condition``
        condition. View `module-level documentation <storage_utils.swift>`
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
            to_upload (list): A list of file names, directory names, or
                `SwiftUploadObject` objects to upload.
            object_threads (int, default 10): The number of threads to use when uploading
                full objects.
            segment_threads (int, default 10): The number of threads to use when uploading
                object segments.
            segment_size (int|str, default 1GB): Upload files in segments no larger than
                <segment_size> (in bytes) and then create a "manifest" file
                that will download all the segments as if it were the original
                file. Sizes may also be expressed as bytes with the B suffix,
                kilobytes with the K suffix, megabytes with the M suffix or
                gigabytes with the G suffix.'
            use_slo (bool, default True): When used in conjunction with segment_size, it
                will create a Static Large Object instead of the default
                Dynamic Large Object.
            leave_segments (bool, default False): Indicates that you want the older segments
                of manifest objects left alone (in the case of overwrites).
            changed (bool, default False): Upload only files that have changed since last
                upload.
            skip_identical (bool, default False): Skip uploading files that are identical
                on both sides. Note this incurs reading the contents of all pre-existing local
                files.
            checksum (bool, default True): Peform checksum validation of upload.
            condition (function(results) -> bool): The method will only return
                when the results of upload matches the condition. In the event of the
                condition never matching after retries, partially uploaded
                results will not be deleted. Note that users are not expected to write
                conditions for upload without an understanding of the structure of the results.
            use_manifest (bool): Generate a data manifest and validate the upload results
                are in the manifest.

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
        if use_manifest and not (len(to_upload) == 1 and os.path.isdir(to_upload[0])):
            raise ValueError('can only upload one directory with use_manifest=True')
        _validate_condition(condition)

        swift_upload_objects = [
            name for name in to_upload
            if isinstance(name, SwiftUploadObject)
        ]
        all_files_to_upload = utils.walk_files_and_dirs([
            name for name in to_upload
            if not isinstance(name, SwiftUploadObject)
        ])

        # Convert everything to swift upload objects and prepend the relative
        # resource directory to uploaded results. Ignore the manifest file in the case of
        # since it will be uploaded individually
        manifest_file_name = Path(to_upload[0]) / DATA_MANIFEST_FILE_NAME if use_manifest else None
        resource_base = _with_trailing_slash(self.resource) or PosixPath('')
        swift_upload_objects.extend([
            SwiftUploadObject(f, object_name=resource_base / file_name_to_object_name(f))
            for f in all_files_to_upload if f != manifest_file_name
        ])

        if use_manifest:
            # Generate the data manifest and save it remotely
            object_names = [o.object_name for o in swift_upload_objects]
            _generate_and_save_data_manifest(to_upload[0], object_names)
            manifest_obj_name = resource_base / file_name_to_object_name(manifest_file_name)
            manifest_obj = SwiftUploadObject(manifest_file_name, object_name=manifest_obj_name)
            self._swift_service_call('upload', self.container, [manifest_obj])

            # Make a condition for validating the upload
            manifest_cond = partial(_validate_manifest_upload, object_names)
            condition = join_conditions(condition, manifest_cond) if condition else manifest_cond

        service_options = {
            'object_uu_threads': object_threads,
            'segment_threads': segment_threads
        }
        upload_options = {
            'segment_size': segment_size,
            'use_slo': use_slo,
            'segment_container': '.segments_%s' % self.container,
            'leave_segments': leave_segments,
            'changed': changed,
            'skip_identical': skip_identical,
            'checksum': checksum
        }
        results = self._swift_service_call('upload',
                                           self.container,
                                           swift_upload_objects,
                                           _service_options=service_options,
                                           options=upload_options)

        _check_condition(condition, results)
        return results

    @_swift_retry(exceptions=UnavailableError)
    def remove(self):
        """Removes a single object.

        This method retries `num_retries` times if swift is unavailable.
        View `module-level documentation <swiftretry>` for more
        information about configuring retry logic at the module or method
        level.

        Raises:
            ValueError: The path is invalid.
            SwiftError: A swift client error occurred.
        """
        if not self.container or not self.resource:
            raise ValueError('path must contain a container and resource to '
                             'remove')

        return self._swift_service_call('delete',
                                        self.container,
                                        [self.resource])

    @_swift_retry(exceptions=(UnavailableError, ConflictError, ConditionNotMetError))
    def rmtree(self, object_threads=10):
        """Removes a resource and all of its contents.

        This method retries `num_retries` times if swift is unavailable.
        View `module-level documentation <storage_utils.swift>` for more
        information about configuring retry logic at the module or method
        level.

        Note that when removing a container, the associated segment container
        will also be removed if it exists. So, if one removes
        ``swift://tenant/container``, ``swift://tenant/container_segments``
        will also be deleted.

        Note:
            Calling rmtree on a directory marker will delete everything under the
            directory marker but not the marker itself.

        Args:
            object_threads (int, default 10): The number of threads to use when deleting
                objects

        Raises:
            SwiftError: A swift client error occurred.
            ConditionNotMetError: Listing the objects after rmtree returns results,
                indicating something went wrong with the rmtree operation
        """
        if not self.container:
            raise ValueError('swift path must include container for rmtree')

        # Ensure that we treat this path as a dir
        to_delete = _with_trailing_slash(self)

        deleting_segments = 'segments' in self.container
        if deleting_segments:
            logger.warning('performing rmtree with segment container "%s". '
                           'This could cause issues when accessing objects '
                           'referencing those segments. Note that segments '
                           'and segment containers are automatically deleted '
                           'when their associated objects or containers are '
                           'deleted.', self.container)

        service_options = {
            'object_dd_threads': object_threads
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
                                      '.segments_%s' % to_delete.container)
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
        _ignore_not_found(to_delete.list)(condition=lambda results: len(results) == 0,
                                          num_retries=0)

        return results

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

    @_swift_retry(exceptions=UnavailableError)
    def post(self, options=None):
        """Post operations on the path.

        This method retries `num_retries` times if swift is unavailable.
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
                        'meta': [],
                        'headers': [],
                        'read_acl': None,   # For containers only
                        'write_acl': None,  # For containers only
                        'sync_to': None,    # For containers only
                        'sync_key': None    # For containers only
                    }

        Raises:
            SwiftError: A swift client error occurred.
        """
        if not self.container or self.resource:
            raise ValueError('post only works on container paths')

        return self._swift_service_call('post',
                                        container=self.container,
                                        options=options)

    def _noop(attr_name):
        def wrapper(self):
            return type(self)(self)
        wrapper.__name__ = attr_name
        wrapper.__doc__ = 'No-op for %r' % attr_name
        return wrapper

    abspath = _noop('abspath')

    def normpath(self):
        "Normalize path following linux conventions (keeps swift:// prefix)"
        normed = posixpath.normpath('/' + str(self)[len(self.swift_drive):])[1:]
        return self.path_class(self.swift_drive + normed)

    realpath = _noop('realpath')
    expanduser = _noop('expanduser')

    def isabs(self):
        return True

    def isdir(self):
        if not self.resource:
            return self.exists()
        try:
            if _with_trailing_slash(self).first():
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

    def islink(self):
        return False

    def ismount(self):
        return True
