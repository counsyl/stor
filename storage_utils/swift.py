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
from functools import wraps
import json
import logging
import operator
import os
import tempfile

from storage_utils import is_swift_path
from storage_utils import path
from storage_utils import utils
from storage_utils.third_party.path import Path
from swiftclient import exceptions as swift_exceptions
from swiftclient import service as swift_service
from swiftclient.service import SwiftUploadObject


logger = logging.getLogger(__name__)


# Default module-level settings for swift authentication.
# If None, the OS_AUTH_URL, OS_USERNAME, or OS_PASSWORD
# environment variables will be used
auth_url = os.environ.get('OS_AUTH_URL', 'http://swift.counsyl.com/auth/v2.0')
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

# Make the default segment size for static large objects be 1GB
DEFAULT_SEGMENT_SIZE = 1024 * 1024 * 1024

# When a swift method takes a condition, these variables are used to
# configure retry logic. These variables can also be passed
# to the methods themselves
initial_retry_sleep = 1
"""The time to sleep before the first retry"""

num_retries = os.environ.get('OS_NUM_RETRIES', 0)
"""The number of times to retry

Uses the ``OS_NUM_RETRIES`` environemnt variable or defaults to 0
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
    """Updates swift settings.

    Args:
        **settings: keyword arguments for settings. Can
            include settings for auth_url, username,
            password, initial_retry_sleep, num_retries,
            and retry_sleep_function.
    """
    for setting, value in settings.items():
        if setting not in globals():
            raise ValueError('invalid setting "%s"' % setting)
        globals()[setting] = value


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


class UnauthorizedError(SwiftError):
    """Thrown when a 403 response is returned from swift"""
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


class _Condition(object):
    """A conditional expression that can be applied to some swift methods

    Condition objects take an operator and a right operand. Left operands
    can be checked against the condition. If the condition is not met,
    ConditionNotMetError exceptions are thrown.

    The operators that are supported are ``==``, ``!=``, ``<``, ``>``, ``<=``,
    and ``>=``.

    """
    operators = {
        '==': operator.eq,
        '!=': operator.ne,
        '<': operator.lt,
        '>': operator.gt,
        '<=': operator.le,
        '>=': operator.ge
    }

    def __init__(self, operator, right_operand):
        if operator not in self.operators:
            raise ValueError('invalid operator: "%s"' % operator)
        self.operator = operator
        self.right_operand = right_operand

    def assert_is_met_by(self, left_operand, left_operand_name):
        """Asserts that a condition is met by a left operand

        Args:
            left_operand: The left operand checked against the condition
            left_operand_name: The name of the left operand. Used when
                creating an error message

        Raises:
            ConditionNotMetError: When the condition is not met
        """
        if not self.operators[self.operator](left_operand, self.right_operand):
            raise ConditionNotMetError(
                'condition not met: %s is not %s' % (left_operand_name, self)
            )

    def __str__(self):
        return '%s %s' % (self.operator, self.right_operand)

    def __repr__(self):
        return '%s("%s", %s)' % (type(self).__name__,
                                 self.operator,
                                 self.right_operand)


def make_condition(operator, right_operand):
    """Creates a condition that can be applied to various swift methods.

    Args:
        operator (str): The operator for the condition, can be
            one of ``==``, ``!=``, ``<``, ``>``, ``<=``,
            and ``>=``.
        right_operand: The operand on the right side of the
            condition.

    Examples:
        To make a condition and use it in `SwiftPath.list`::

            from storage_utils.swift import make_condition
            from storage_utils.swift import SwiftPath
            p = SwiftPath('swift://tenant/container/resource')
            # Ensure that the amount of listed objects are greater than 6
            cond = make_condition('>', 6)
            objs = p.list(num_objs_cond=cond)

        An illustration of what it looks like when a condition doesn't pass::

            cond = make_condition('>', 100)
            # ConditionNotMetError is thrown when the condition is not met
            objs = p.list(num_objs_cond=cond)
            Traceback (most recent call last):
            ...
            storage_utils.swift.ConditionNotMetError: condition not met: num
            listed objects is not > 100
    """
    return _Condition(operator, right_operand)


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
            elif http_status == 503:
                logger.error('unavailable error in swift operation - %s', str(e))
                raise UnavailableError(str(e), e)
            elif 'reset contents for reupload' in str(e):
                # When experiencing HA issues, we sometimes encounter a
                # ClientException from swiftclient during upload. The exception
                # is thrown here -
                # https://github.com/openstack/python-swiftclient/blob/84d110c63ecf671377d4b2338060e9b00da44a4f/swiftclient/client.py#L1625  # nopep8
                # Treat this as an UnavailableError
                logger.error('unavailable error in swift put_object operation - %s', str(e))
                raise UnavailableError(str(e), e)
            else:
                logger.error('unexpected swift error - %s', str(e))
                raise SwiftError(str(e), e)

    return wrapper


def _delegate_to_buffer(attr_name, valid_modes=None):
    "Factory function that delegates file-like properties to underlying buffer"
    def wrapper(self, *args, **kwargs):
        if self.closed:
            raise ValueError('I/O operation on closed file')
        if valid_modes and self.mode not in valid_modes:
            raise TypeError('SwiftFile must be in modes %s to %r' %
                            (valid_modes, attr_name))
        try:
            func = getattr(self._buffer, attr_name)
            return func(*args, **kwargs)
        except AttributeError:
            raise AttributeError("'%s' object has no attribute '%s'" %
                                 (self, attr_name))
    wrapper.__name__ = attr_name
    return wrapper


def _with_slash(path):
    "Appends a trailing slash to a path if it doesn't have one"
    return path if not path or path.endswith('/') else path + '/'


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
    newlines = _delegate_to_buffer('newlines', valid_modes=_VALID_MODES)

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


class SwiftPath(str):
    """
    Provides the ability to manipulate and access resources on swift
    with a similar interface to the path library.
    """
    swift_drive = drive = 'swift://'

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
        return 'SwiftPath("%s")' % self

    def __add__(self, more):
        return SwiftPath(super(SwiftPath, self).__add__(more))

    def __div__(self, rel):
        """Join two path components, adding a separator character if needed."""
        return SwiftPath(os.path.join(self, rel))

    # Make the / operator work even when true division is enabled.
    __truediv__ = __div__

    def is_ambiguous(self):
        """Returns true if it cannot be determined if the path is a
        file or directory
        """
        f = self.resource
        return f and not f.endswith('/') and not f.ext

    @property
    def name(self):
        """The name of the path, mimicking path.py's name property"""
        return Path(self).name

    @property
    def parent(self):
        """The parent of the path, mimicking path.py's parent property"""
        return self.__class__(Path(self).parent)

    @property
    def ext(self):
        """The extension of the file"""
        return Path(self).ext

    def dirname(self):
        """The directory name of the path, mimicking path.py's dirname()"""
        return self.__class__(Path(self).dirname())

    def basename(self):
        """The base name name of the path, mimicking path.py's basename()"""
        return Path(self).basename()

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
        """Returns the resource as a ``path.Path`` object or None.

        A resource can be a single object or a prefix to objects.
        Note that it's important to keep the trailing slash in a resource
        name for prefix queries.
        """
        parts = self._get_parts()
        joined_resource = '/'.join(parts[2:]) if len(parts) > 2 else None

        return path(joined_resource) if joined_resource else None

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

    @_propagate_swift_exceptions
    def _swift_connection_call(self, method, *args, **kwargs):
        """Runs a method call on a ``Connection`` object."""
        return method(*args, **kwargs)

    @_propagate_swift_exceptions
    def _swift_service_call(self, method, *args, **kwargs):
        """Runs a method call on a ``SwiftService`` object."""
        results = method(*args, **kwargs)

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
        connection = self._get_swift_connection()
        headers, content = self._swift_connection_call(connection.get_object,
                                                       self.container,
                                                       self.resource)
        return content

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
             num_objs_cond=None,
             # intentionally not documented
             list_as_dir=False):
        """List contents using the resource of the path as a prefix.

        This method retries `num_retries` times if swift is unavailable
        or if the number of objects returned does not match the
        ``num_objs_cond`` condition. View
        `module-level documentation <swiftretry>` for more
        information about configuring retry logic at the module or method
        level.

        Args:
            starts_with (str): Allows for an additional search path to
                be appended to the resource of the swift path. Note that the
                current resource path is treated as a directory
            limit (int): Limit the amount of results returned
            num_objs_cond (SwiftCondition): The method will only return
                results when the number of objects returned meets this
                condition.

        Returns:
            List[SwiftPath]: Every path in the listing.

        Raises:
            SwiftError: A swift client error occurred.
            ConditionNotMetError: Results were returned, but they did not
                meet the num_objs_cond condition.
        """
        connection = self._get_swift_connection()
        tenant = self.tenant
        prefix = self.resource
        full_listing = limit is None

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
            list_kwargs['prefix'] = _with_slash(list_kwargs['prefix'])

        if self.container:
            results = self._swift_connection_call(connection.get_container,
                                                  self.container,
                                                  **list_kwargs)
        else:
            results = self._swift_connection_call(connection.get_account,
                                                  **list_kwargs)

        path_pre = SwiftPath('swift://%s/%s' % (tenant, self.container or ''))
        paths = list({
            path_pre / (r.get('name') or r['subdir'].rstrip('/')) for r in results[1]
        })

        if num_objs_cond:
            num_objs_cond.assert_is_met_by(len(paths), 'num listed objects')

        return paths

    def listdir(self):
        """Lists the path as a dir, returning top-level directories and files

        For information about retry logic on this method, see
        `SwiftPath.list`
        """
        return self.list(list_as_dir=True)

    @_swift_retry(exceptions=(ConditionNotMetError, UnavailableError))
    def glob(self, pattern, num_objs_cond=None):
        """Globs all objects in the path with the pattern.

        This glob is only compatible with patterns that end in * because of
        swift's inability to do searches other than prefix queries.

        Note that this method assumes the current resource is a directory path
        and treats it as such. For example, if the user has a swift path of
        swift://tenant/container/my_dir (without the trailing slash), this
        method will perform a swift query with a prefix of mydir/pattern.

        This method retries `num_retries` times if swift is unavailable or if
        the number of globbed patterns does not match the ``num_objs_cond``
        condition. View `module-level documentation <swiftretry>`
        for more information about configuring retry logic at the module or
        method level.

        Args:
            pattern (str): The pattern to match. The pattern can only have
                up to one '*' at the end.
            num_objs_cond (SwiftCondition): The method will only return
                results when the number of objects returned meets this
                condition.

        Returns:
            List[SwiftPath]: Every matching path.

        Raises:
            SwiftError: A swift client error occurred.
            ConditionNotMetError: Results were returned, but they did not
                meet the num_objs_cond condition.
        """
        if pattern.count('*') > 1:
            raise ValueError('multiple pattern globs not supported')
        if '*' in pattern and not pattern.endswith('*'):
            raise ValueError('only prefix queries are supported')

        paths = self.list(starts_with=pattern.replace('*', ''), num_retries=0)

        if num_objs_cond:
            num_objs_cond.assert_is_met_by(len(paths), 'num globbed objects')

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
            return bool(self.first(num_retries=0))
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

        service = self._get_swift_service()
        self._swift_service_call(service.download,
                                 container=self.container,
                                 objects=[self.resource],
                                 options={'out_file': out_file})

    @_swift_retry(exceptions=(UnavailableError))
    def download_objects(self,
                         dest,
                         objects,
                         object_threads=10,
                         container_threads=10,):
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
            object_threads (int): The amount of threads to use for downloading
                objects.
            container_threads (int): The amount of threads to use for
                downloading containers.
            objs (List[str|PosixPath|SwiftPath]): The list of objects to
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
        obj_base = self.resource or path('')
        objs_to_download = {
            obj: path(obj).resource if is_swift_path(obj) else obj_base / obj
            for obj in objects
        }

        for obj in objs_to_download:
            if is_swift_path(obj) and not obj.startswith(_with_slash(self)):
                raise ValueError(
                    '"%s" must be child of download path "%s"' % (obj, self))

        service = self._get_swift_service(object_dd_threads=object_threads,
                                          container_threads=container_threads)
        download_options = {
            'prefix': _with_slash(self.resource),
            'out_directory': dest,
            'remove_prefix': True
        }
        results = self._swift_service_call(service.download,
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
                 num_objs_cond=None):
        """Downloads a directory to a destination.

        This method retries `num_retries` times if swift is unavailable or if
        the number of downloaded objects does not match the ``num_objs_cond``
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
            num_objs_cond (SwiftCondition): The method will only return
                results when the number of objects downloaded meets this
                condition. Partially downloaded results will not be deleted.

        Raises:
            SwiftError: A swift client error occurred.
            ConditionNotMetError: Results were returned, but they did not
                meet the ``num_objs_cond`` condition.

        Returns:
            dict: A mapping of the destination to the download path
        """
        if not self.container:
            raise ValueError('cannot call download on tenant with no container')

        service = self._get_swift_service(object_dd_threads=object_threads,
                                          container_threads=container_threads)
        download_options = {
            'prefix': _with_slash(self.resource),
            'out_directory': dest,
            'remove_prefix': True
        }
        results = self._swift_service_call(service.download,
                                           self.container,
                                           options=download_options)

        if num_objs_cond:
            num_objs_cond.assert_is_met_by(len(results),
                                           'num downloaded objects')

        return {self: dest}

    @_swift_retry(exceptions=UnavailableError)
    def upload(self,
               to_upload,
               segment_size=DEFAULT_SEGMENT_SIZE,
               use_slo=True,
               segment_container=None,
               leave_segments=False,
               changed=False,
               object_name=None,
               object_threads=10,
               segment_threads=10):
        """Uploads a list of files and directories to swift.

        This method retries `num_retries` times if swift is unavailable.
        View `module-level documentation <storage_utils.swift>` for more
        information about configuring retry logic at the module or method
        level.

        Note that this method upload on paths relative to the current
        directory. In order to load files relative to a target directory,
        use path as a context manager to change the directory.

        For example::

            with path('/path/to/upload/dir'):
                path('swift://tenant/container').upload(['.'])

        Args:
            to_upload (list): A list of file names, directory names, or
                `SwiftUploadObject` objects to upload.
            segment_size (int|str): Upload files in segments no larger than
                <segment_size> (in bytes) and then create a "manifest" file
                that will download all the segments as if it were the original
                file. Sizes may also be expressed as bytes with the B suffix,
                kilobytes with the K suffix, megabytes with the M suffix or
                gigabytes with the G suffix.'
            use_slo (bool): When used in conjunction with segment_size, it
                will create a Static Large Object instead of the default
                Dynamic Large Object.
            segment_container (str): Upload the segments into the specified
                container. If not specified, the segments will be uploaded to
                a <container>_segments container to not pollute the main
                <container> listings.
            leave_segments (bool): Indicates that you want the older segments
                of manifest objects left alone (in the case of overwrites).
            changed (bool): Upload only files that have changed since last
                upload.
            object_threads (int): The number of threads to use when uploading
                full objects.
            segment_threads (int): The number of threads to use when uploading
                object segments.

            Raises:
                SwiftError: A swift client error occurred.
        """
        service = self._get_swift_service(object_uu_threads=object_threads,
                                          segment_threads=segment_threads)
        swift_upload_objects = [
            name for name in to_upload
            if isinstance(name, SwiftUploadObject)
        ]
        all_files_to_upload = utils.walk_files_and_dirs([
            name for name in to_upload
            if not isinstance(name, SwiftUploadObject)
        ])

        # Verify no absolute paths are being uploaded
        for f in all_files_to_upload:
            if f.startswith('/'):
                raise ValueError('cannot upload absolute path %s' % f)

        # Convert everything to swift upload objects and prepend the relative
        # resource directory to uploaded results
        resource_base = _with_slash(self.resource) or path('')
        swift_upload_objects.extend([
            SwiftUploadObject(f, object_name=resource_base / f)
            for f in all_files_to_upload
        ])

        upload_options = {
            'segment_size': segment_size,
            'use_slo': use_slo,
            'segment_container': segment_container,
            'leave_segments': leave_segments,
            'changed': changed
        }
        print 'upload objects', swift_upload_objects
        return self._swift_service_call(service.upload,
                                        self.container,
                                        swift_upload_objects,
                                        options=upload_options)

    copy = utils.copy
    copytree = utils.copytree

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

        service = self._get_swift_service()
        return self._swift_service_call(service.delete,
                                        self.container,
                                        [self.resource])

    @_swift_retry(exceptions=UnavailableError)
    def rmtree(self):
        """Removes a resource and all of its contents.

        This method retries `num_retries` times if swift is unavailable.
        View `module-level documentation <storage_utils.swift>` for more
        information about configuring retry logic at the module or method
        level.

        Raises:
            SwiftError: A swift client error occurred.
        """
        service = self._get_swift_service()
        if not self.resource:
            return self._swift_service_call(service.delete,
                                            self.container)
        else:
            to_delete = [p.resource for p in self.list()]
            return self._swift_service_call(service.delete,
                                            self.container,
                                            to_delete)

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
                'Object': Path('object.txt'),
                'Content-Type': 'application/octet-stream',
                # The size of the object
                'Content-Length': '112',
                # The last time the object was modified
                'Last-Modified': 'Fri, 15 Jan 2016 05:22:46 GMT',
                # The MD5 checksum of the object. NOTE that if a large
                # object is uploaded in segments that this will be the
                # checksum of a segment
                'ETag': '87f0b7f04557315e6d1e6db21742d31c',
                'Manifest': None
            }

        Raises:
            NotFoundError: When the tenant, container, or
                object can't be found.
        """
        stat_objects = [self.resource] if self.resource else None
        service = self._get_swift_service()
        result = self._swift_service_call(service.stat,
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

        service = self._get_swift_service()
        return self._swift_service_call(service.post,
                                        container=self.container,
                                        options=options)

    def _noop(attr_name):
        def wrapper(self):
            return type(self)(self)
        wrapper.__name__ = attr_name
        wrapper.__doc__ = 'No-op for %r' % attr_name
        return wrapper

    abspath = _noop('abspath')
    expanduser = _noop('expanduser')

    def expandvars(self):
        "Expand system environment variables in path"
        return type(self)(os.path.expandvars(self))

    def expand(self):
        "Expand variables and normalize path"
        return self.expandvars().normpath()

    def normpath(self):
        "Normalize path following linux conventions"
        normed = os.path.normpath('/' + str(self)[len(self.swift_drive):])[1:]
        return type(self)(self.swift_drive + normed)
