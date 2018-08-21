from cached_property import cached_property

import dxpy.bindings as dxb
from dxpy.exceptions import DXError, DXSearchError

from stor import Path
from stor.obs import OBSPath


class DXPath(OBSPath):
    """
        Provides the ability to manipulate and access resources on swift
        with a similar interface to the path library.
        """

    def __new__(cls, path):
        return super(DXPath, cls).__new__(Path, path)

    drive = 'dx://'

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

    #TODO(akumar)
    def _get_dx_connection_vars(self):
        raise NotImplementedError

    #TODO(akumar)
    def _dx_api_call(self):
        raise NotImplementedError

    def temp_url(self):
        raise NotImplementedError

    @property
    def resource(self):
        res = super(DXPath, self).resource
        return self.parts_class('/'+res) if res else None

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

    def walkfiles(self, pattern=None):
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

    def list(self):
        """List contents using the resource of the path as a prefix."""
        raise NotImplementedError

    def listdir(self):
        """list the path as a dir, returning top-level directories and files."""
        raise NotImplementedError

    def glob(self, pattern):
        """ Glob for pattern relative to this directory.

        Note that Swift currently only supports a single trailing *"""
        raise NotImplementedError

    def exists(self):
        """Checks whether path exists on local filesystem or on swift.

        For directories on swift, checks whether directory sentinel exists or
        at least one subdirectory exists"""
        raise NotImplementedError


class DXVirtualPath(DXPath):
    """Class Handler for DXPath of form 'dx://project-{ID}:/a/b/c' or 'dx://a/b/c'"""

    @property
    def virtual_project(self):
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
        """
        try:
            if not dxb.verify_string_dxid(self.project, 'project'):
                return self.project
        except DXError:
            try:
                proj_dict = dxb.search.find_one_project(
                    name=self.project, level='VIEW', zero_ok=True, more_ok=False)
                if proj_dict:
                    return proj_dict['id']
            except DXSearchError as e:
                raise e('Did not find exactly 1 matching project name to ID')

    @cached_property
    def canonical_resource(self):
        """Returns the dx file-ID of the first matched filename"""
        if not self.ext:
            raise ValueError('DXPath must be file with extension')
        if not self.resource:
            return None
        objects = [{
            'name': self.name,
            'folder': '/' + self.resource.parent,
            'project': self.canonical_project
        }]
        object_d = next(iter(dxb.search.resolve_data_objects(objects=objects)[0]), None)
        if object_d:
            return object_d['id']

    @property
    def canonical_path(self):
        """returns the first file that matches the given path"""
        return DXCanonicalPath(self.drive + self.canonical_project + ':') / (self.canonical_resource or '')

    @cached_property
    def stat(self):
        return self.canonical_path().stat()


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
        proj = dxb.dxproject.DXProject(dxid=self.project)
        virtual_p = DXVirtualPath(self.drive + proj.name + ':/')
        if self.resource:
            file_h = dxb.DXFile(dxid=self.resource)
            virtual_p = virtual_p / file_h.folder[1:] / file_h.name
        print(virtual_p)
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

    @cached_property
    def stat(self):
        if self.resource:
            return dxb.dxdataobject_functions.describe(self.resource)
        elif self.project:
            return dxb.dxdataobject_functions.describe(self.project)
