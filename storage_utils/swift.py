from backoff.backoff import with_backoff
import cStringIO
from functools import wraps
import operator
import os
from path import Path
from storage_utils import utils
from swiftclient import exceptions as swift_exceptions
from swiftclient import service as swift_service


# Settings for swift retry logic
initial_retry_sleep = 1
num_retries = 5
retry_sleep_function = lambda t, attempt: t * 2


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


def _swift_propagate_exceptions(func):
    """Bubbles all swift exceptions as SwiftClientErrors
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
                raise SwiftNotFoundError(str(e), e)
            else:
                raise SwiftClientError(str(e), e)

    return wrapper


class SwiftClientError(Exception):
    """The top-level exception thrown for any swift errors

    The 'caught_exception' attribute of the exception must
    be examined in order to inspect the swift exception that
    happened. A swift exception can either be a
    SwiftError (thrown by swiftclient.service) or a
    ClientError (thrown by swiftclient.client)
    """
    def __init__(self, message, caught_exception=None):
        super(SwiftClientError, self).__init__(message)
        self.caught_exception = caught_exception


class SwiftNotFoundError(SwiftClientError):
    """Thrown when a 404 response is returned from swift
    """
    pass


class SwiftConfigurationError(SwiftClientError):
    """Thrown when swift is not configured properly.

    Swift needs the OS_USERNAME and OS_PASSWORD env
    variables configured in order to operate.
    """
    pass


class SwiftConditionError(SwiftClientError):
    """Thrown when a swift command does not meet a condition.

    Some swift commands (such as list) can have a condition
    attached to them that will cause the command to fail if
    the condition criteria is not met. This exception is
    thrown in those cases.
    """
    pass


class SwiftCondition(object):
    """A condition on a swift call

    SwiftCondition objects are passed to swift calls that take conditions
    (e.g. list). A swift condition is constructed with an operator and
    right operand of the condition, for example:

        >>> cond = SwiftCondition('>=', 3)

    The above makes a "greater than or equal to three" condition. The
    ``is_met_by`` method is used to test the condition:

        >>> print cond.is_met_by(3)
        True
        >>> print cond.is_met_by(4)
        True
        >>> print cond.is_met_by(2)
        False

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

    def is_met_by(self, left_operand):
        """Returns True if the left operand meets the condition
        """
        return self.operators[self.operator](left_operand, self.right_operand)

    def __str__(self):
        return '%s %s' % (self.operator, self.right_operand)

    def __repr__(self):
        return 'SwiftCondition("%s", %s)' % (self.operator, self.right_operand)


class SwiftPath(str):
    """
    Provides the ability to manipulate and access resources on swift
    with a similar interface to the path library.
    """
    swift_drive = 'swift://'
    default_auth_url = 'http://sfo1-prd-osn01.counsyl.com/auth/v2.0'

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

    def get_parts(self):
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
        parts = self.get_parts()
        return parts[0] if len(parts) > 0 and parts[0] else None

    @property
    def container(self):
        """Returns the container name from the path or None.
        """
        parts = self.get_parts()
        return parts[1] if len(parts) > 1 and parts[1] else None

    @property
    def resource(self):
        """Returns the resource or None.

        A resource can be a single object or a prefix to objects.
        Note that it's important to keep the trailing slash in a resource
        name for prefix queries.
        """
        parts = self.get_parts()
        joined_resource = '/'.join(parts[2:]) if len(parts) > 2 else None

        return Path(joined_resource) if joined_resource else None

    def _get_swift_connection_options(self, **options):
        """Returns options for constructing SwiftServices and Connections.

        Args:
            options: Additional options that are directly passed
                into connection options.

        Raises:
            SwiftConfigurationError: The needed swift environment variables
                aren't set.
        """
        if 'OS_PASSWORD' not in os.environ or 'OS_USERNAME' not in os.environ:
            raise SwiftConfigurationError('OS_USERNAME and OS_PASSWORD '
                                          'environment vars must be set for '
                                          'Swift authentication')

        # Set additional options on top of what was passed in
        options['os_tenant_name'] = self.tenant
        options['os_auth_url'] = os.environ.get('OS_AUTH_URL',
                                                self.default_auth_url)

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

    @_swift_propagate_exceptions
    def _swift_connection_call(self, method, *args, **kwargs):
        """Runs a method call on a Connection object.
        """
        return method(*args, **kwargs)

    @_swift_propagate_exceptions
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

    @_swift_retry(exceptions=SwiftClientError)
    def open(self, mode='r'):
        """Opens a single resource using swift's get_object.

        Args:
            mode (str): The mode of file IO. Reading is the only supported
                mode.

        Returns:
            cStringIO: The contents of the object.

        Raises:
            SwiftClientError: A swift client error occurred.
        """
        if mode not in ('r', 'rb'):
            raise ValueError('only read-only mode ("r" and "rb") is supported')

        connection = self._get_swift_connection()
        headers, content = self._swift_connection_call(connection.get_object,
                                                       self.container,
                                                       self.resource)
        return cStringIO.StringIO(content)

    @_swift_retry(exceptions=SwiftConditionError)
    def list(self, starts_with=None, limit=None, num_objs_cond=None):
        """List contents using the resource of the path as a prefix.

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
            SwiftClientError: A swift client error occurred.
            SwiftConditionError: Results were returned, but they did not
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

        if num_objs_cond and not num_objs_cond.is_met_by(len(paths)):
            raise SwiftConditionError('swift condition not met: '
                                      'num listed objects %s' % num_objs_cond)

        return paths

    def glob(self, pattern, num_objs_cond=None):
        """Globs all objects in the path with the pattern.

        This glob is only compatible with patterns that end in * because of
        swift's inability to do searches other than prefix queries.

        Note that this method assumes the current resource is a directory path
        and treats it as such. For example, if the user has a swift path of
        swift://tenant/container/my_dir (without the trailing slash), this
        method will perform a swift query with a prefix of mydir/pattern

        Args:
            pattern (str): The pattern to match. The pattern can only have
                up to one '*' at the end.
            num_objs_cond (SwiftCondition): The method will only return
                results when the number of objects returned meets this
                condition.

        Returns:
            List[SwiftPath]: Every matching path.

        Raises:
            SwiftClientError: A swift client error occurred.
            SwiftConditionError: Results were returned, but they did not
                meet the num_objs_cond condition.
        """
        if pattern.count('*') > 1:
            raise ValueError('multiple pattern globs not supported')
        if '*' in pattern and not pattern.endswith('*'):
            raise ValueError('only prefix queries are supported')

        return self.list(starts_with=pattern.replace('*', ''),
                         num_objs_cond=num_objs_cond)

    def first(self):
        """Returns the first result from the list results of the path.

        Note that this method does not perform any retry logic.

        Raises:
            SwiftClientError: A swift client error occurred.
        """
        results = self.list(limit=1)
        return results[0] if results else None

    def exists(self):
        """Checks existence of the path.

        Returns True if the path exists, False otherwise. This method
        performs no retry logic.

        Returns:
            bool: True if the path exists, False otherwise.

        Raises:
            SwiftClientError: A non-404 swift client error occurred.
        """
        try:
            return bool(self.first())
        except SwiftNotFoundError:
            return False

    @_swift_retry(exceptions=SwiftConditionError)
    def download(self,
                 output_dir=None,
                 remove_prefix=False,
                 object_threads=10,
                 container_threads=10,
                 num_objs_cond=None):
        """Downloads a path.

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
            SwiftClientError: A swift client error occurred.
            SwiftConditionError: Results were returned, but they did not
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

        if num_objs_cond and not num_objs_cond.is_met_by(len(results)):
            raise SwiftConditionError('swift condition not met: num '
                                      'downloaded objects %s' % num_objs_cond)

    def upload(self,
               upload_names,
               segment_size=None,
               use_slo=False,
               segment_container=None,
               leave_segments=False,
               changed=False,
               object_name=None,
               object_threads=10,
               segment_threads=10):
        """Uploads a list of files and directories to swift.

        Note that this method uploads based on paths relative to the current
        directory. In order to load files relative to a target directory,
        use path as a context manager to change the directory.

        For example::

            with path('/path/to/upload/dir'):
                path('swift://tenant/container').upload(['.'])

        Args:
            upload_names (list): A list of file and directory names to upload.
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
                SwiftClientError: A swift client error occurred.
        """
        service = self._get_swift_service(object_uu_threads=object_threads,
                                          segment_threads=segment_threads)
        upload_names = utils.walk_files_and_dirs(upload_names)
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
                                        upload_names,
                                        options=upload_options)

    def remove(self):
        """Removes a single object.

        Raises:
            ValueError: The path is invalid.
            SwiftClientError: A swift client error occurred.
        """
        if not self.container or not self.resource:
            raise ValueError('path must contain a container and resource to '
                             'remove')

        service = self._get_swift_service()
        return self._swift_service_call(service.delete,
                                        self.container,
                                        [self.resource])

    def rmtree(self):
        """Removes a resource and all of its contents.

        Raises:
            SwiftClientError: A swift client error occurred.
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

    def post(self, options=None):
        """Post operations on the path.

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
            SwiftClientError: A swift client error occurred.
        """
        if not self.container or self.resource:
            raise ValueError('post only works on container paths')

        service = self._get_swift_service()
        return self._swift_service_call(service.post,
                                        container=self.container,
                                        options=options)
