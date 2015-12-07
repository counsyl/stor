from backoff.backoff import with_backoff
import cStringIO
import os
from path import Path
from storage_utils import utils
import swiftclient


class SwiftConfigurationError(Exception):
    """Thrown when swift is not configured properly.

    Swift needs the OS_USERNAME and OS_PASSWORD env
    variables configured in order to operate.
    """
    pass


class SwiftConditionError(Exception):
    """Thrown when a swift command does not meet a condition.

    Some swift commands (such as list) can have a condition
    attached to them that will cause the command to fail if
    the condition criteria is not met. This exception is
    thrown in those cases.
    """
    pass


class SwiftPath(str):
    """
    Provides the ability to manipulate and access resources on swift
    with a similar interface to the path library.
    """
    swift_drive = 'swift://'
    default_auth_url = 'https://oak1-prd-oslb01.counsyl.com/auth/v2.0'

    def __init__(self, swift_path):
        """Validates swift path is in the proper format.

        Args:
            swift_path (str): A path that matches the format of
                "swift://{tenant_name}/{container_name}/{rest_of_path}".
                The "swift://" prefix is required in the path.
        """
        if not swift_path.startswith(self.swift_drive):
            raise ValueError('path must have {}'.format(self.swift_drive))
        return super(SwiftPath, self).__init__(swift_path)

    def __repr__(self):
        return 'SwiftPath("{}")'.format(self)

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
        from swiftclient import service

        if 'OS_PASSWORD' not in os.environ or 'OS_USERNAME' not in os.environ:
            raise SwiftConfigurationError('OS_USERNAME and OS_PASSWORD '
                                          'environment vars must be set for '
                                          'Swift authentication')

        # Set additional options on top of what was passed in
        options['os_tenant_name'] = self.tenant
        options['os_auth_url'] = os.environ.get('OS_AUTH_URL',
                                                self.default_auth_url)

        # Merge options with global and local ones
        options = dict(service._default_global_options,
                       **dict(service._default_local_options, **options))
        service.process_options(options)
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
        from swiftclient import service

        conn_opts = self._get_swift_connection_options(**options)
        return service.SwiftService(conn_opts)

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
        from swiftclient import service

        conn_opts = self._get_swift_connection_options(**options)
        return service.get_conn(conn_opts)

    @with_backoff(exceptions=swiftclient.exceptions.ClientException)
    def open(self, mode='r'):
        """Opens a single resource using swift's get_object.

        Args:
            mode (str): The mode of file IO. Reading is the only supported
                mode.

        Returns:
            cStringIO: The contents of the object.

        Raises:
            swiftclient.exceptions.ClientException: The swift request is
                invalid.
        """
        if mode not in ('r', 'rb'):
            raise ValueError('only read-only mode ("r" and "rb") is supported')

        connection = self._get_swift_connection()
        headers, content = connection.get_object(self.container, self.resource)
        return cStringIO.StringIO(content)

    @with_backoff(exceptions=SwiftConditionError)
    def list(self, starts_with=None, num_objs_eq=None):
        """List contents using the resource of the path as a prefix.

        Args:
            starts_with (str): Allows for an additional search path to
                be appended to the resource of the swift path. Note that the
                current resource path is treated as a directory
            num_objs_eq (int): If specified, this call
                returns results only when num_objs_eq matches the number
                of returned objects

        Returns:
            Generator[SwiftPath]: Every path in the listing.

        Raises:
            Exception: An error was found in the returned results.
        """
        service = self._get_swift_service()
        tenant = self.tenant
        prefix = self.resource

        # When starts_with is provided, treat the resource as a
        # directory that has the starts_with parameter after it. This allows
        # the user to specify a path like tenant/container/mydir
        # and do an additional glob on a directory-like structure
        if starts_with:
            prefix = prefix / starts_with if prefix else starts_with

        batched_results = service.list(container=self.container, options={
            'prefix': prefix
        })

        batched_results = self._eval_swift_results_or_error(batched_results)
        results = []
        for batched_result in batched_results:
            # If there is no container present in the results, the object name
            # is the container name
            c = batched_result['container'] or ''
            results.extend([
                SwiftPath('swift://{}'.format(tenant)) / c / obj['name']
                for obj in batched_result['listing']
            ])

        if num_objs_eq is not None and len(results) != num_objs_eq:
            raise SwiftConditionError('num objects returned from list '
                                      '!= {}'.format(num_objs_eq))

        return results

    def glob(self, pattern, num_objs_eq=None):
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
            num_objs_eq (int): If specified, this call
                returns results only when num_objs_eq matches the number
                of returned objects

        Returns:
            Generator[SwiftPath]: Every matching path.
        """
        if pattern.count('*') > 1:
            raise ValueError('multiple pattern globs not supported')
        if '*' in pattern and not pattern.endswith('*'):
            raise ValueError('only prefix queries are supported')

        return self.list(starts_with=pattern.replace('*', ''))

    def first(self):
        """Returns the first result from the list results of the path.

        Note that this method does not perform any retry logic.

        Raises:
            swiftclient.service.SwiftError: A swift error happened.
        """
        results = self.list()
        return results[0] if results else None

    def exists(self):
        """Checks existence of the path.

        Returns True if the path exists, False otherwise. This method
        performs no retry logic.

        Returns:
            bool: True if the path exists, False otherwise.

        Raises:
            swiftclient.service.SwiftError: A non-404 swift error happened.
        """
        from swiftclient.service import SwiftError

        try:
            return bool(self.first())
        except SwiftError as e:
            # Return false if the container doesnt exist
            if e.exception.http_status == 404:
                return False
            raise

    def _eval_swift_results_or_error(self, results, ignore_http_codes=None):
        """Evaluate iterable results from swift or error.

        Args:
            results (list|dict): Results returned from a swift command
                (such as "download" or "upload").
            ignore_http_codes (list): A list of integers of http status codes
                to ignore if they are found.

        Raises:
            swiftclient.service.SwiftError: A swift error happened.

        Returns:
            list: The swift results as a list. If the swift results were
                a single dictionary, a single-element list is returned
        """
        ignore_http_codes = ignore_http_codes or {}
        results = [results] if isinstance(results, dict) else list(results)
        for r in results:
            if 'error' in r:
                http_status = getattr(r['error'], 'http_status', None)
                if http_status not in ignore_http_codes:
                    raise r['error']

        return results

    @with_backoff(exceptions=SwiftConditionError)
    def download(self,
                 output_dir=None,
                 remove_prefix=False,
                 object_threads=10,
                 container_threads=10,
                 num_objs_eq=None):
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
            num_objs_eq (int): If specified, this call
                returns results only when num_objs_eq matches the number
                of downloaded objects

        Raises:
            swiftclient.service.SwiftError: A swift error happened.
        """
        service = self._get_swift_service(object_dd_threads=object_threads,
                                          container_threads=container_threads)
        results = service.download(self.container, options={
            'prefix': self.resource,
            'out_directory': output_dir,
            'remove_prefix': remove_prefix,
            'skip_identical': True,
        })
        results = self._eval_swift_results_or_error(results,
                                                    ignore_http_codes=[304])

        if num_objs_eq is not None and len(results) != num_objs_eq:
            raise SwiftConditionError('num objects downloaded '
                                      '!= {}'.format(num_objs_eq))

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

        For example:
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
            changed (bool): Only upload files that have changed since the last
                upload.
            object_name (str): Upload file and name object to <object_name>.
                If uploading dir, use <object_name> as object prefix instead
                of the folder name.
            object_threads (int): The number of threads to use when uploding
                full objects.
            segment_threads (int): The number of threads to use when uploading
                object segments.

            Raises:
                swiftclient.service.SwiftError: A swift upload failed.
        """
        service = self._get_swift_service(object_uu_threads=object_threads,
                                          segment_threads=segment_threads)
        upload_names = utils.walk_files_and_dirs(upload_names)
        results = service.upload(
            self.container, upload_names, options={
                'segment_size': segment_size,
                'use_slo': use_slo,
                'segment_container': segment_container,
                'leave_segments': leave_segments,
                'changed': changed,
                'object_name': object_name
            })
        self._eval_swift_results_or_error(results)

    def remove(self):
        """Removes a single object.

        Raises:
            ValueError: The path is invalid.
            swiftclient.service.SwiftError: A swift deletion failed.
        """
        if not self.container or not self.resource:
            raise ValueError('path must contain a container and resource to '
                             'remove')

        service = self._get_swift_service()
        results = service.delete(self.container, [self.resource])
        self._eval_swift_results_or_error(results)

    def rmtree(self):
        """Removes a resource and all of its contents.

        Raises:
            swiftclient.service.SwiftError: The deletion fails.
        """
        service = self._get_swift_service()
        if not self.resource:
            results = service.delete(self.container)
        else:
            to_delete = [p.resource for p in self.list()]
            results = service.delete(self.container, to_delete)
        self._eval_swift_results_or_error(results)

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
            swiftclient.service.SwiftError: The post fails.
        """
        if not self.container or self.resource:
            raise ValueError('post only works on container paths')

        service = self._get_swift_service()
        results = service.post(container=self.container, options=options)
        self._eval_swift_results_or_error(results)
