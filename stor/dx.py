from cached_property import cached_property
import six

import dxpy.bindings as dxb
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
            return DuplicateProjectError(msg, exc)
        elif msg and 'found none' in msg.lower():
            return NotFoundError(msg, exc)


class DuplicateProjectError(DNAnexusError):
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
        Provides the ability to manipulate and access resources on swift
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

    @property
    def project(self):
        """Returns the project name from the path or None"""
        parts = self._get_parts()
        return parts[0] if len(parts) > 0 and parts[0] else None

    # TODO(akumar) make sure DX_LOGIN_TOKEN is set up here
    def _get_dx_connection_vars(self):
        raise NotImplementedError

    #TODO(akumar)
    def _dx_api_call(self):
        raise NotImplementedError

    def temp_url(self):
        raise NotImplementedError

    @property
    def resource(self):
        parts = self._get_parts()
        joined_resource = '/'.join(parts[1:]) if len(parts) > 1 else None
        return self.parts_class('/'+joined_resource) if joined_resource else None

    def first(self): # may not need it
        raise NotImplementedError

    def download_objects(self): # may not need it
        raise NotImplementedError
    
    def remove(self):
        raise NotImplementedError
    
    def rmtree(self):
        raise NotImplementedError
    
    def isdir(self):
        raise NotImplementedError
    
    def isfile(self):
        raise NotImplementedError

    def getsize(self):
        raise NotImplementedError

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

    def list(self, canonicalize=False):
        """List contents using the resource of the path as a prefix."""
        return list(self.walkfiles(canonicalize=canonicalize))

    def listdir(self, canonicalize=False):
        """list the path as a dir, returning top-level directories and files."""
        proj_id = self.canonical_project
        proj_name = self.virtual_project
        ans_list = []
        if not self.resource:
            obj_dict = dxb.DXProject(dxid=proj_id).list_folder(
                describe={'fields': {'name': True, 'folder': True}}
            )
        elif self.endswith('/'):
            obj_dict = dxb.DXProject(dxid=proj_id).list_folder(
                folder=self.resource,
                describe={'fields': {'name': True, 'folder': True}}
            )
        else:
            return ans_list
        for key, values in obj_dict:
            for entry in values:
                if key == 'folders':
                    ans_list.append(entry)
                elif canonicalize:
                    ans_list.append(DXCanonicalPath('dx://{}:/{}'.format(proj_id, entry['id'])))
                else:
                    ans_list.append(DXVirtualPath(self.drive + proj_name + ':/')
                                    / entry['describe']['folder'] / entry['describe']['name'])

    def walkfiles(self, pattern=None, canonicalize=False):
        """Iterates over listed files that match an optional pattern.

        :param pattern: pattern to match the filenames against.
        :param canonicalize: boolean indicating whether to return canonicalized paths
        :return: Iter[DXPath] Iterates over listed files that match an optional pattern.
        """
        proj_id = self.canonical_project
        proj_name = self.virtual_project
        if not self.resource:
            # the query performance is similar w/wo describe field,
            # hence no need to customize query on canonicalize flag
            list_gen = dxb.search.find_data_objects(
                project=proj_id,
                name=pattern,
                name_mode='glob',
                describe={'fields': {'name': True, 'folder': True}}
            )
        elif self.endswith('/'):
            list_gen = dxb.search.find_data_objects(
                project=proj_id,
                name=pattern,
                name_mode='glob',
                folder=self.resource,
                describe={'fields': {'name': True, 'folder': True}}
            )
        else:
            list_gen = dxb.search.find_data_objects(
                project=proj_id,
                name=self.virtual_resource.name,
                folder=self.resource.parent,
                describe={'fields': {'name': True, 'folder': True}}
            )
        for obj in list_gen:
            if canonicalize:
                yield DXCanonicalPath('dx://{}:/{}'.format(obj['project'], obj['id']))
            else:
                dx_p = DXVirtualPath(self.drive + proj_name + ':/')
                dx_p = dx_p / obj['describe']['folder'] / obj['describe']['name']
                yield dx_p

    def glob(self, pattern):
        """ Glob for pattern relative to this directory.

        Note that Swift currently only supports a single trailing *"""
        raise NotImplementedError

    def exists(self):
        """Checks whether path exists on local filesystem or on swift.

        For directories on swift, checks whether directory sentinel exists or
        at least one subdirectory exists"""
        raise NotImplementedError

    def stat(self):
        if not self.__stat:

            if self.canonical_resource:
                self.__stat = dxb.DXFile(dxid=self.canonical_resource,
                                         project=self.canonical_project).describe()
            else:
                self.__stat = dxb.DXProject(dxid=self.canonical_project).describe()
        return self.__stat


class DXVirtualPath(DXPath):
    """Class Handler for DXPath of form 'dx://project-{ID}:/a/b/c' or 'dx://a/b/c'"""

    @property
    def virtual_project(self):
        if utils.is_valid_dxid(self.project, 'project'):
            return dxb.DXProject(dxid=self.project).name
        return self.project

    @property
    def virtual_resource(self):
        return self.resource

    @property
    def virtual_path(self):
        return self

    @cached_property
    def canonical_project(self):
        """Returns the first project that matches the name that user has view access to.
        If no match is found, returns None

        Raises:
            DuplicateProjectError - if project name is not unique on DX platform
            NotFoundError - If project name doesn't exist on DNAnexus
        """
        if utils.is_valid_dxid(self.project, 'project'):
            return self.project
        else:
            try:
                proj_dict = dxb.search.find_one_project(
                    name=self.project, level='VIEW', zero_ok=True, more_ok=False)
                if proj_dict is None:
                    raise ProjectNotFoundError('No projects were found with given name ({})'
                                               .format(self.project))
                return proj_dict['id']
            except DXError as e:
                six.raise_from(_parse_dx_error(e), e)

    @cached_property
    def canonical_resource(self):
        """Returns the dx file-ID of the first matched filename"""
        if not self.resource:
            return None
        if self.endswith('/'):
            raise ValueError('DXPath ({}) ending in folders cannot be canonicalized'.format(self))
        objects = [{
            'name': self.name,
            'folder': self.resource.parent,
            'project': self.canonical_project
        }]
        object_d = next(iter(dxb.search.resolve_data_objects(objects=objects)[0]), None)
        if object_d:
            return object_d['id']
        else:
            raise NotFoundError('The virtual resource does not exist on DNAnexus')

    @property
    def canonical_path(self):
        """returns the first file that matches the given path"""
        return DXCanonicalPath(self.drive + self.canonical_project + ':') / (self.canonical_resource or '')


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
        proj = dxb.DXProject(dxid=self.project)
        virtual_p = DXVirtualPath(self.drive + proj.name + ':/')
        if self.resource:
            file_h = dxb.DXFile(dxid=self.resource)
            virtual_p = virtual_p / file_h.folder[1:] / file_h.name
        return virtual_p

    @property
    def canonical_project(self):
        return self.project

    @property
    def canonical_resource(self):
        return self.resource

    @property
    def canonical_path(self):
        return self
