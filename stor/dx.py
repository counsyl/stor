from stor.obs import OBSPath
import dxpy

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
        return parts[0] if len(parts) > 0 and parts[0] else None

    #TODO(akumar)
    def _get_dx_connection_vars(self):
        raise NotImplementedError

    #TODO(akumar)
    def _dx_api_call(self):
        raise NotImplementedError

    def canonicalize(self):
        raise NotImplementedError

    def humanize(self):
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

    def stat(self):
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