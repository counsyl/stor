from cached_property import cached_property

import dxpy.bindings as db

from stor.obs import OBSPath


class DXPath(OBSPath):
    """
        Provides the ability to manipulate and access resources on swift
        with a similar interface to the path library.
        """
    drive = 'dx://'

    @property
    def project(self):
        """Returns the project name from the path or None"""
        parts = self._get_parts()
        return parts[0][:-1] if len(parts) > 0 and parts[0] else None

    #TODO(akumar)
    def _get_dx_connection_vars(self):
        raise NotImplementedError

    #TODO(akumar)
    def _dx_api_call(self):
        raise NotImplementedError

    def temp_url(self):
        raise NotImplementedError

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

    @cached_property
    def canonical_project(self):
        """Returns the first project that matches the name that user has view access to.
        If no match is found, returns None
        """
        # we need the DX project-ID to get the file_ID and full path
        proj_dict = db.search.find_one_project(
            name=self.project, level='VIEW', zero_ok=True, more_ok=False)
        if proj_dict:
            return proj_dict['id']

    @cached_property
    def canonical_resource(self):
        """Returns the dx file-ID of the first matched filename"""
        if not self.resource:
            return None
        objects = [{
            'name': self.name,
            'folder': '/' + self.resource.parent,
            'project': self.canonical_project
        }]
        object_d = next(iter(db.search.resolve_data_objects(objects=objects)[0]), None)
        if object_d:
            return object_d['id']

    def canonical_path(self):
        """returns the first file that matches the given path"""
        return DXCanonicalPath(self.drive + self.canonical_project + ':') / (self.canonical_resource or '')

    @cached_property
    def stat(self):
        return self.canonical_path().stat()


class DXCanonicalPath(DXPath):

    def virtual_project(self):
        return self.virtual_path.project

    def virtual_resource(self):
        return self.virtual_path.resource

    @cached_property
    def virtual_path(self):
        proj = db.dxproject.DXProject(dxid=self.project)
        virtual_p = DXVirtualPath(self.drive + proj.name + ':/')
        if self.resource:
            file_h = db.DXFile(dxid=self.resource)
            virtual_p = virtual_p / file_h.folder[1:] / file_h.name
        print(virtual_p)
        return virtual_p

    @cached_property
    def stat(self):
        if self.resource:
            return db.dxdataobject_functions.describe(self.resource)
        elif self.project:
            return db.dxdataobject_functions.describe(self.project)
