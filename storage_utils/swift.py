"""
Provides utilities for accessing swift object storage.

Different configuration options are available at the module level, and
these variables are documented under their module declarations.

For swift authentication, the ``auth_url``, ``username``, and
``password`` variables are used.

For methods that take conditions, the ``initial_retry_sleep``,
``num_retries``, and ``retry_sleep_function`` variables are used to
configure the logic around retrying when the condition is not met.
Note that these variables can also be passed to the methods themselves.

Examples:

    Basic usage of swift is shown in the following with an example of how
    to download a swift path to the current working directory.

    >>> from storage_utils import swift
    >>> swift.auth_url = 'swift_auth_url.com'
    >>> swift.username = 'swift_user'
    >>> swift.password = 'swift_pass'
    >>>
    >>> swift_path = swift.SwiftPath('swift://tenant/container/prefix')
    >>> swift_path.download()
"""
from backoff.backoff import with_backoff
import cStringIO
from functools import wraps
import operator
import os
from path import Path
from storage_utils import utils
from swiftclient import exceptions as swift_exceptions
from swiftclient import service as swift_service


# Default module-level settings for swift authentication.
# If None, the OS_AUTH_URL, OS_USERNAME, or OS_PASSWORD
# environment variables will be used
auth_url = None
"""The swift authentication URL

If not set, the ``OS_AUTH_URL`` environment variable will be used. If
that is not set, the ``DEFAULT_AUTH_URL`` global constant will
be used.
"""

username = None
"""The swift username

If not set, the ``OS_USERNAME`` environment variable will be used.
"""

password = None
"""The swift password

If not set, the ``OS_PASSWORD`` environment variable will be used.
"""

# The default auth url used if the module setting or env variable
# isn't set
DEFAULT_AUTH_URL = 'http://swift.counsyl.com/auth/v2.0'

# When a swift method takes a condition, these variables are used to
# configure retry logic. These variables can also be passed
# to the methods themselves
initial_retry_sleep = 1
"""The time to sleep before the first retry"""

num_retries = 0
"""The number of times to retry"""

retry_sleep_function = lambda t, attempt: t * 2
"""The function that increases sleep time when retrying.

This function needs to take two integer
arguments (time slept last attempt, attempt number) and
return a time to sleep in seconds.
"""


class SwiftError(Exception):
    """The top-level exception thrown for any swift errors

    The 'caught_exception' attribute of the exception must
    be examined in order to inspect the swift exception that
    happened. A swift exception can either be a
    SwiftError (thrown by swiftclient.service) or a
    ClientError (thrown by swiftclient.client)
    """
    def __init__(self, message, caught_exception=None):
        super(SwiftError, self).__init__(message)
        self.caught_exception = caught_exception


class NotFoundError(SwiftError):
    """Thrown when a 404 response is returned from swift
    """
    pass


class UnavailableError(SwiftError):
    """Thrown when a 503 response is returned from swift
    """
    pass


class ConfigurationError(SwiftError):
    """Thrown when swift is not configured properly.

    Swift needs either module-level or environment authentication
    variables set in order to be configured.
    """
    pass


class ConditionNotMetError(SwiftError):
    """Thrown when a condition is not met.
    """
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
        >>> from storage_utils.swift import make_condition
        >>> from storage_utils.swift import SwiftPath
        >>> p = SwiftPath('swift://tenant/container/resource')
        >>> # Ensure that the amount of listed objects are greater than 6
        >>> cond = make_condition('>', 6)
        >>> objs = p.list(num_objs_cond=cond)

        >>> cond = make_condition('>', 100)
        >>> # ConditionNotMetError is thrown when the condition is not met
        >>> objs = p.list(num_objs_cond=cond)
        >>> Traceback (most recent call last):
        >>> ...
        >>> storage_utils.swift.ConditionNotMetError: condition not met: num
        >>> listed objects is not > 100
    """
    return _Condition(operator, right_operand)


def _swift_retry(exceptions=None):
    """Allows SwiftPath methods to take optional retry configuration parameters
    for doing retry logic
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
    """Bubbles all swift exceptions as SwiftErrors
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
            if http_status == 404:
                raise NotFoundError(str(e), e)
            elif http_status == 503:
                raise UnavailableError(str(e), e)
            else:
                raise SwiftError(str(e), e)

    return wrapper


class SwiftPath(str):
    """
    Provides the ability to manipulate and access resources on swift
    with a similar interface to the path library.
    """
    swift_drive = 'swift://'

    def __init__(self, swift):
        """Validates swift path is in the proper format.

        Args:
            swift (str): A path that matches the format of
                "swift://{tenant_name}/{container_name}/{rest_of_path}".
                The "swift://" prefix is required in the path.
        """
        if not swift.startswith(self.swift_drive):
            raise ValueError('path must have %s' % self.swift_drive)
        return super(SwiftPath, self).__init__(swift)

    def __repr__(self):
        return 'SwiftPath("%s")' % self

    def __add__(self, more):
        return SwiftPath(super(SwiftPath, self).__add__(more))

    def __div__(self, rel):
        """Join two path components, adding a separator character if needed.
        """
        return SwiftPath(os.path.join(self, rel))

    # Make the / operator work even when true division is enabled.
    __truediv__ = __div__

    def _get_parts(self):
        """Returns the path parts (excluding swift://) as a list of strings.
        """
        if len(self) > len(self.swift_drive):
            return self[len(self.swift_drive):].split('/')
        else:
            return []

    @property
    def tenant(self):
        """Returns the tenant name from the path or return None
        """
        parts = self._get_parts()
        return parts[0] if len(parts) > 0 and parts[0] else None

    @property
    def container(self):
        """Returns the container name from the path or None.
        """
        parts = self._get_parts()
        return parts[1] if len(parts) > 1 and parts[1] else None

    @property
    def resource(self):
        """Returns the resource as a Path object or None.

        A resource can be a single object or a prefix to objects.
        Note that it's important to keep the trailing slash in a resource
        name for prefix queries.
        """
        parts = self._get_parts()
        joined_resource = '/'.join(parts[2:]) if len(parts) > 2 else None

        return Path(joined_resource) if joined_resource else None

    def _get_swift_connection_options(self, **options):
        """Returns options for constructing SwiftServices and Connections.

        Args:
            options: Additional options that are directly passed
                into connection options.

        Raises:
            ConfigurationError: The needed swift environment variables
                aren't set.
        """
        os_auth_url = (
            auth_url or os.environ.get('OS_AUTH_URL') or DEFAULT_AUTH_URL)
        os_username = username or os.environ.get('OS_USERNAME')
        os_password = password or os.environ.get('OS_PASSWORD')

        if not os_username or not os_password:
            raise ConfigurationError((
                'OS_AUTH_URL, OS_USERNAME, and OS_PASSWORD environment vars '
                'must be set for swift authentication. The username, password '
                'and auth_url module-level variables may also be set.'
            ))

        # Set additional options on top of what was passed in
        options['os_tenant_name'] = self.tenant
        options['os_auth_url'] = os_auth_url
        options['os_username'] = os_username
        options['os_password'] = os_password

        # Merge options with global and local ones
        options = dict(swift_service._default_global_options,
                       **dict(swift_service._default_local_options,
                              **options))
        swift_service.process_options(options)
        return options

    def _get_swift_service(self, **options):
        """Initialize a swift service based on the path.

        Uses the tenant name of the path and an auth url to instantiate
        the swift service. The OS_AUTH_URL environment variable is used
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
        """Runs a method call on a Connection object.
        """
        return method(*args, **kwargs)

    @_propagate_swift_exceptions
    def _swift_service_call(self, method, *args, **kwargs):
        """Runs a method call on a SwiftService object.
        """
        results = method(*args, **kwargs)

        results = [results] if isinstance(results, dict) else list(results)
        for r in results:
            if 'error' in r:
                http_status = getattr(r['error'], 'http_status', None)
                if not http_status or http_status >= 400:
                    raise r['error']

        return results

    @_swift_retry(exceptions=(NotFoundError, UnavailableError))
    def open(self, mode='r'):
        """Opens a single resource using swift's get_object.

        This method is configured to retry by default when the object is
        not found or swift is unavailable. View module-level documentation
        about how to configure retry logic at the module or method level.

        Args:
            mode (str): The mode of file IO. Reading is the only supported
                mode.

        Returns:
            cStringIO: The contents of the object.

        Raises:
            SwiftError: A swift client error occurred.
        """
        if mode not in ('r', 'rb'):
            raise ValueError('only read-only mode ("r" and "rb") is supported')

        connection = self._get_swift_connection()
        headers, content = self._swift_connection_call(connection.get_object,
                                                       self.container,
                                                       self.resource)
        return cStringIO.StringIO(content)

    @_swift_retry(exceptions=(ConditionNotMetError, UnavailableError))
    def list(self, starts_with=None, limit=None, num_objs_cond=None):
        """List contents using the resource of the path as a prefix.

        This method retries ``num_retries`` times if swift is unavailable
        or if the number of objects returned does not match the
        num_objs_cond condition. View module-level documentation for more
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

        if self.container:
            results = self._swift_connection_call(connection.get_container,
                                                  self.container,
                                                  full_listing=full_listing,
                                                  prefix=prefix,
                                                  limit=limit)
        else:
            results = self._swift_connection_call(connection.get_account,
                                                  full_listing=full_listing,
                                                  prefix=prefix,
                                                  limit=limit)

        path_pre = SwiftPath('swift://%s/%s' % (tenant, self.container or ''))
        paths = [
            path_pre / r['name'] for r in results[1]
        ]

        if num_objs_cond:
            num_objs_cond.assert_is_met_by(len(paths), 'num listed objects')

        return paths

    @_swift_retry(exceptions=(ConditionNotMetError, UnavailableError))
    def glob(self, pattern, num_objs_cond=None):
        """Globs all objects in the path with the pattern.

        This glob is only compatible with patterns that end in * because of
        swift's inability to do searches other than prefix queries.

        Note that this method assumes the current resource is a directory path
        and treats it as such. For example, if the user has a swift path of
        swift://tenant/container/my_dir (without the trailing slash), this
        method will perform a swift query with a prefix of mydir/pattern.

        This method retries ``num_retries`` times if swift is unavailable or if
        the number of globbed patterns does not match the num_objs_cond
        condition. View module-level documentation for more
        information about configuring retry logic at the module or method
        level.

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

    @_swift_retry(exceptions=(UnavailableError))
    def first(self):
        """Returns the first result from the list results of the path

        Raises:
            SwiftError: A swift client error occurred.
        """
        results = self.list(limit=1, num_retries=0)
        return results[0] if results else None

    @_swift_retry(exceptions=(UnavailableError))
    def exists(self):
        """Checks existence of the path.

        Returns True if the path exists, False otherwise.

        Returns:
            bool: True if the path exists, False otherwise.

        Raises:
            SwiftError: A non-404 swift client error occurred.
        """
        try:
            return bool(self.first(num_retries=0))
        except NotFoundError:
            return False

    @_swift_retry(exceptions=(ConditionNotMetError, UnavailableError))
    def download(self,
                 output_dir=None,
                 remove_prefix=False,
                 object_threads=10,
                 container_threads=10,
                 num_objs_cond=None):
        """Downloads a path.

        This method retries ``num_retries`` times if swift is unavailable or if
        the number of downloaded objects does not match the num_objs_cond
        condition. View module-level documentation for more
        information about configuring retry logic at the module or method
        level.

        Args:
            output_dir (str): The output directory to download results to.
                If None, results are downloaded to the working directory.
            remove_prefix (bool): Removes the prefix in the path from the
                downloaded results. For example, if our swift path is
                swift://tenant/container/my_prefix, all results under my_prefix
                will be downloaded without my_prefix attached to them if
                remove_prefix is true.
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
                meet the num_objs_cond condition.
        """
        service = self._get_swift_service(object_dd_threads=object_threads,
                                          container_threads=container_threads)
        download_options = {
            'prefix': self.resource,
            'out_directory': output_dir,
            'remove_prefix': remove_prefix,
            'skip_identical': True,
        }
        results = self._swift_service_call(service.download,
                                           self.container,
                                           options=download_options)

        if num_objs_cond:
            num_objs_cond.assert_is_met_by(len(results),
                                           'num downloaded objects')

    @_swift_retry(exceptions=(UnavailableError))
    def upload(self,
               to_upload,
               segment_size=None,
               use_slo=False,
               segment_container=None,
               leave_segments=False,
               changed=False,
               object_name=None,
               object_threads=10,
               segment_threads=10):
        """Uploads a list of files and directories to swift.

        This method retries ``num_retries`` times if swift is unavailable.
        View module-level documentation for more
        information about configuring retry logic at the module or method
        level.

        Note that this method uploads based on paths relative to the current
        directory. In order to load files relative to a target directory,
        use path as a context manager to change the directory.

        For example::

            with path('/path/to/upload/dir'):
                path('swift://tenant/container').upload(['.'])

        Args:
            to_upload (list): A list of file and directory names to upload.
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
            object_name (str): Upload file and name object to <object_name>.
                If uploading dir, use <object_name> as object prefix instead
                of the folder name.
            object_threads (int): The number of threads to use when uploading
                full objects.
            segment_threads (int): The number of threads to use when uploading
                object segments.

            Raises:
                SwiftError: A swift client error occurred.
        """
        service = self._get_swift_service(object_uu_threads=object_threads,
                                          segment_threads=segment_threads)
        all_files_to_upload = utils.walk_files_and_dirs(to_upload)
        upload_options = {
            'segment_size': segment_size,
            'use_slo': use_slo,
            'segment_container': segment_container,
            'leave_segments': leave_segments,
            'changed': True,
            'object_name': object_name
        }
        return self._swift_service_call(service.upload,
                                        self.container,
                                        all_files_to_upload,
                                        options=upload_options)

    @_swift_retry(exceptions=(UnavailableError))
    def remove(self):
        """Removes a single object.

        This method retries ``num_retries`` times if swift is unavailable.
        View module-level documentation for more
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

    @_swift_retry(exceptions=(UnavailableError))
    def rmtree(self):
        """Removes a resource and all of its contents.

        This method retries ``num_retries`` times if swift is unavailable.
        View module-level documentation for more
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

    @_swift_retry(exceptions=(UnavailableError))
    def post(self, options=None):
        """Post operations on the path.

        This method retries ``num_retries`` times if swift is unavailable.
        View module-level documentation for more
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
