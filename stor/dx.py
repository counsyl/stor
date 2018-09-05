from cached_property import cached_property
import six

import dxpy
import dxpy.exceptions as dx_exceptions
from dxpy.exceptions import DXError

from stor import exceptions as stor_exceptions
from stor import Path
from stor import utils
from stor.obs import OBSPath


DNAnexusError = stor_exceptions.RemoteError
NotFoundError = stor_exceptions.NotFoundError


def _parse_dx_error(exc, **kwargs):
    """
    Parses DXError exceptions to throw a more informative exception.
    """
    msg = exc.message
    exc_type = type(exc)

    if exc_type is dx_exceptions.DXSearchError:
        if msg and 'found more' in msg.lower():
            return DuplicateError(msg, exc)
        elif msg and 'found none' in msg.lower():
            return NotFoundError(msg, exc)


class DuplicateError(DNAnexusError):
    """Thrown when multiple objects exist with the same name

    Currently, we throw this when trying to get the canonical project
    from virtual path and two or more projects were found with same name
    """
    pass


class DuplicateProjectError(DuplicateError):
    """Thrown when multiple projects exist with the same name

    Currently, we throw this when trying to get the canonical project
    from virtual path and two or more projects were found with same name
    """
    pass


class ProjectNotFoundError(NotFoundError):
    """Thrown when no project exists with the given name

    Currently, we throw this when trying to get the canonical project
    from virtual path and no project was found with same name
    """
    pass


class DXPath(OBSPath):
    """
        Provides the ability to manipulate and access resources on DNAnexus
        with a similar interface to the path library.
        """

    def __new__(cls, path):
        """Custom __new__ method so that the validation checks happen during creation

        This ensures invalid dx paths like DXPath('dx://) are never initialized
        """
        return super(DXPath, cls).__new__(Path, path)

    drive = 'dx://'

    __stat = None

    def _get_parts(self):
        """Returns the path parts (excluding the drive) as a list of strings."""
        parts = super(DXPath, self)._get_parts()
        if len(parts) > 0 and parts[0]:
            # first part can be 'proj:file' or 'proj:' or 'proj'
            parts_first = parts[0].split(':')
            parts[0] = parts_first[0]
            if len(parts_first) > 1 and parts_first[1]:
                parts.insert(1, parts_first[1])
        return parts

    def _is_folder(self):
        return self.resource and not self.resource.ext and \
               not utils.is_valid_dxid(self.resource.lstrip('/'), 'file')

    def _noop(attr_name):
        def wrapper(self):
            return type(self)(self)
        wrapper.__name__ = attr_name
        wrapper.__doc__ = 'No-op for %r' % attr_name
        return wrapper

    abspath = _noop('abspath')
    realpath = _noop('realpath')
    expanduser = _noop('expanduser')

    @property
    def project(self):
        """Returns the project name from the path or None"""
        parts = self._get_parts()
        return parts[0] if len(parts) > 0 and parts[0] else None

    def temp_url(self, lifetime=300, filename=None):
        """Obtains a temporary URL to a DNAnexus data-object.

        Args:
            lifetime (int): The time (in seconds) the temporary
                URL will be valid
            filename (str, optional): A urlencoded filename to use for
                attachment, otherwise defaults to object name
        """
        try:
            if self.canonical_resource:
                file_handler = dxpy.DXFile(self.canonical_resource)
                return file_handler.get_download_url(
                    duration=lifetime,
                    preauthenticated=True,
                    filename=filename,
                    project=self.canonical_project
                )[0]
            else:
                raise DXError('DX Projects cannot have a temporary download url')
        except ValueError:
            raise DXError('DXPaths ending in folders cannot have a temporary download url')

    @property
    def resource(self):
        parts = self._get_parts()
        joined_resource = '/'.join(parts[1:]) if len(parts) > 1 else None
        return self.parts_class('/'+joined_resource) if joined_resource else None

    def dirname(self):
        if not self.resource:
            return self
        else:
            return super(DXPath, self).dirname()

    def download_objects(self):  # may not need it
        raise NotImplementedError

    def remove(self):
        raise NotImplementedError

    def rmtree(self):
        raise NotImplementedError

    def isdir(self):
        if not self.resource or self._is_folder():
            return self.exists()
        return False

    def isfile(self):
        try:
            return self.resource and not self._is_folder() and self.exists()
        except NotFoundError:
            return False

    def getsize(self):
        if not self.resource:
            return self.stat()['dataUsage']*1e9
        elif self._is_folder():
            return 0
        else:
            return self.stat()['size']

    def download_object(self, dest):
        """Download a single path or object to file."""
        raise NotImplementedError

    def download(self, dest):
        """Download a directory."""
        raise NotImplementedError

    def upload(self, source):
        """Upload a list of files and directories to a directory."""
        raise NotImplementedError

    def open(self, mode='r', encoding=None):
        """
        Opens a OBSFile that can be read or written to and is uploaded to
        the remote service.
        """
        raise NotImplementedError

    def list(self,
             canonicalize=False,
             starts_with=None,
             limit=None,
             category=None,
             condition=None
             ):
        """List contents using the resource of the path as a prefix.

        Args:
            canonicalize (boolean): whether to return canonicalized paths
            starts_with (str): Allows for an additional search path to
                be appended to the resource of the dx path. Note that this
                resource path is treated as a directory
            limit (int): Limit the amount of results returned
            category (str): Restricting class : One of 'record', 'file', 'gtable,
                'applet', 'workflow'
            condition (function(results) -> bool): The method will only return
                when the results matches the condition.

        Returns:
             List[DXPath]: Iterates over listed files that match an optional pattern.
        """
        results = list(self.walkfiles(
            canonicalize=canonicalize,
            starts_with=starts_with,
            limit=limit,
            category=category
        ))
        if not results or not results[0]:  # when results == [[]]
            results = []
        utils.validate_condition(condition)
        utils.check_condition(condition, results)
        return results

    def list_iter(self,
                  canonicalize=False,
                  starts_with=None,
                  limit=None,
                  category=None,
                  ):
        """Iterate over contents using the resource of the path as a prefix.

        Args:
            canonicalize (boolean): whether to return canonicalized paths
            starts_with (str): Allows for an additional search path to
                be appended to the resource of the dx path. Note that this
                resource path is treated as a directory
            limit (int): Limit the amount of results returned
            category (str): Restricting class : One of 'record', 'file', 'gtable,
                'applet', 'workflow'

        Returns:
             Iter[DXPath]: Iterates over listed files that match an optional pattern.
        """
        return self.walkfiles(
            canonicalize=canonicalize,
            starts_with=starts_with,
            limit=limit,
            category=category
        )

    def listdir(self, only='all', canonicalize=False):
        """List the path as a dir, returning top-level directories and files.

        Args:
            canonicalize (boolean): whether to return canonicalized paths
            only (str): "objects" for only objects, "folders" for only folder,
                    "all" for both

        Returns:
            List[DXPath]: Iterates over listed files directly within the resource

        Raises:
            NotFoundError: When resource folder is not present on DX platform
        """
        proj_id = self.canonical_project
        proj_name = self.virtual_project
        ans_list = []
        kwargs = {
            'only': only,
            'describe': {'fields': {'name': True, 'folder': True}}
        }
        if self._is_folder():
            kwargs.update({'folder': self.resource})
        elif self.resource:
            return ans_list
        try:
            obj_dict = dxpy.DXProject(dxid=proj_id).list_folder(**kwargs)
        except dxpy.exceptions.ResourceNotFound:
            raise NotFoundError('The specified folder ({}) was not found'.format(
                self.resource))
        for key, values in obj_dict.items():
            for entry in values:
                if canonicalize:
                    ans_list.append(DXCanonicalPath('dx://{}:/{}'.format(
                        proj_id, (entry.lstrip('/') if key == 'folders' else entry['id']))))
                else:
                    if key == 'folders':
                        ans_list.append(DXVirtualPath(self.drive + proj_name + ':' + entry))
                    else:
                        ans_list.append(DXVirtualPath(self.drive + proj_name + ':' +
                                                      entry['describe']['folder'])
                                        / entry['describe']['name'])
        return ans_list

    def listdir_iter(self, canonicalize=False):
        """Iterate the path as a dir, returning top-level directories and files.

        Args:
            canonicalize (boolean): whether to return canonicalized paths

        Returns:
            Iter[DXPath]: Iterates over listed files directly within the resource
        """
        folders = self.listdir(only='folders', canonicalize=canonicalize)
        for folder in folders:
            yield folder
        for data in self.walkfiles(
                        canonicalize=canonicalize,
                        recurse=False
                    ):
            yield data

    def walkfiles(self,
                  pattern=None,
                  canonicalize=False,
                  recurse=True,
                  starts_with=None,
                  limit=None,
                  category=None):
        """Iterates over listed files that match an optional pattern.

        Args:
            pattern (str): glob pattern to match the filenames against.
            canonicalize (boolean): whether to return canonicalized paths
            recurse (boolean): whether to look in subfolders of folder as well
            starts_with (str): Allows for an additional search path to
                be appended to the resource of the dx path. Note that this
                resource path is treated as a directory
            limit (int): Limit the amount of results returned
            category (str): Restricting class : One of 'record', 'file', 'gtable,
                'applet', 'workflow'

        Returns:
             Iter[DXPath]: Iterates over listed files that match an optional pattern.
        """
        proj_id = self.canonical_project
        proj_name = self.virtual_project
        kwargs = {
            'project': proj_id,
            'name': pattern,
            'name_mode': 'glob',
            # the query performance is similar w/wo describe field,
            # hence no need to customize query based on canonicalize flag
            'describe': {'fields': {'name': True, 'folder': True}},
            'recurse' : recurse,
            'classname': category,
            'limit': limit,
            'folder': (self.resource or '/') + (starts_with or '')
        }
        if self.resource and not self._is_folder():  # if path is a file path
            yield []
            return
        list_gen = dxpy.find_data_objects(**kwargs)
        for obj in list_gen:
            if canonicalize:
                yield DXCanonicalPath('dx://{}:/{}'.format(obj['project'], obj['id']))
            else:
                dx_p = DXVirtualPath(self.drive + proj_name + ':' + obj['describe']['folder'])
                dx_p = dx_p / obj['describe']['name']
                yield dx_p

    def glob(self, pattern, condition=None, canonicalize=False):
        """ Glob for pattern relative to this directory."""

        results = list(self.walkfiles(
            canonicalize=canonicalize,
            pattern=pattern
        ))
        if not results or not results[0]:  # when results == [[]]
            results = []
        utils.validate_condition(condition)
        utils.check_condition(condition, results)
        return results

    def exists(self):
        """Checks existence of the path.

        Returns True if the path exists, False otherwise.

        Returns:
            bool: True if the path exists, False otherwise.
        """
        try:
            # first see if there is a specific corresponding object
            self.stat()
            return True
        except (NotFoundError, ValueError):
            pass
        # otherwise we could be a directory, so try to grab first
        # file/subfolder
        if self._is_folder():
            try:
                self.list(limit=1)
                return True
            except NotFoundError:
                return False
        return False

    def stat(self):
        if not self.__stat:

            if self.canonical_resource:
                self.__stat = dxpy.DXFile(dxid=self.canonical_resource,
                                          project=self.canonical_project).describe()
            else:
                self.__stat = dxpy.DXProject(dxid=self.canonical_project).describe()
        return self.__stat


class DXVirtualPath(DXPath):
    """Class Handler for DXPath of form 'dx://project-{ID}:/a/b/c' or 'dx://a/b/c'"""

    @property
    def virtual_project(self):
        if utils.is_valid_dxid(self.project, 'project'):
            return dxpy.DXProject(dxid=self.project).name
        return self.project

    @property
    def virtual_resource(self):
        return self.resource

    @property
    def virtual_path(self):
        return self

    @cached_property
    def canonical_project(self):
        """Returns the unique project that matches the name that user has view access to.
        If no match is found, returns None

        Raises:
            DuplicateProjectError - if project name is not unique on DX platform
            NotFoundError - If project name doesn't exist on DNAnexus
        """
        if utils.is_valid_dxid(self.project, 'project'):
            return self.project
        else:
            try:
                proj_dict = dxpy.find_one_project(
                    name=self.project, level='VIEW', zero_ok=True, more_ok=False)
                if proj_dict is None:
                    raise ProjectNotFoundError('No projects were found with given name ({})'
                                               .format(self.project))
                return proj_dict['id']
            except DXError as e:
                raise DuplicateProjectError('Duplicate projects were found with given name ({})'
                                            .format(self.project), e)

    @cached_property
    def canonical_resource(self):
        """Returns the dx file-ID of the uniquely matched filename

        Raises:
            DuplicateError: if filename is not unique
            NotFoundError: if resource is not found on DX platform
        """
        if not self.resource:
            return None
        if self._is_folder():
            raise ValueError('DXPath ({}) ending in folders cannot be canonicalized'.format(self))
        objects = [{
            'name': self.name,
            'folder': self.resource.parent,
            'project': self.canonical_project
        }]
        results = dxpy.resolve_data_objects(objects=objects)[0]
        if len(results) > 1:
            raise DuplicateError('The virtual resource is not unique on DNAnexus')
        elif len(results) == 1:
            return results[0]['id']
        else:
            raise NotFoundError('The virtual resource does not exist on DNAnexus')

    @property
    def canonical_path(self):
        """Returns the unique file that matches the given path"""
        return DXCanonicalPath(self.drive + self.canonical_project +
                               ':') / (self.canonical_resource or '')


class DXCanonicalPath(DXPath):
    """Class Handler for DXPath of form 'dx://project-{ID}:/file-{ID}' or 'dx://project-{ID}:'"""

    @property
    def virtual_project(self):
        return self.virtual_path.project

    @property
    def virtual_resource(self):
        return self.virtual_path.resource

    @cached_property
    def virtual_path(self):
        proj = dxpy.DXProject(dxid=self.project)
        virtual_p = DXVirtualPath(self.drive + proj.name + ':/')
        if self.resource:
            file_h = dxpy.DXFile(dxid=self.canonical_resource)
            virtual_p = virtual_p / file_h.folder[1:] / file_h.name
        return virtual_p

    @property
    def canonical_project(self):
        return self.project

    @property
    def canonical_resource(self):
        return self.resource.lstrip('/') if self.resource else None

    @property
    def canonical_path(self):
        return self
