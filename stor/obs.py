import cStringIO
import posixpath

from cached_property import cached_property
from swiftclient.service import SwiftError
from swiftclient.service import SwiftUploadObject

from stor.base import Path
from stor.posix import PosixPath
from stor import utils


def _delegate_to_buffer(attr_name, valid_modes=None):
    """Factory function that delegates file-like properties to underlying buffer"""
    def wrapper(self, *args, **kwargs):
        if self.closed:
            raise ValueError('I/O operation on closed file')
        if valid_modes and self.mode not in valid_modes:
            raise TypeError('File must be in modes %s to %r' %
                            (valid_modes, attr_name))
        func = getattr(self._buffer, attr_name)
        return func(*args, **kwargs)
    wrapper.__name__ = attr_name
    wrapper.__doc__ = getattr(cStringIO.StringIO(), attr_name).__doc__
    return wrapper


class OBSUploadObject(SwiftUploadObject):
    """
    An upload object similar to swiftclient's SwiftUploadObject that allows the user
    to specify a destination file name (full key) and upload options.
    """
    def __init__(self, source, object_name, options=None):
        """
        An OBSUploadObject must be initialized with a source and destination path.

        Args:
            source (str): A path that specifies a source file.
            dest (str): A path that specifies a destination file name (full key)
        """
        try:
            super(OBSUploadObject, self).__init__(source, object_name=object_name, options=options)
        except SwiftError as exc:
            if 'SwiftUploadObject' in exc.value:
                msg = exc.value.replace('SwiftUploadObject', 'OBSUploadObject', 1)
            else:
                msg = exc.value
            raise ValueError(msg)


class OBSPath(Path):
    """
    A base class that defines all methods available from Path objects that
    interface with remote services, such as Swift and S3.
    """
    path_module = posixpath
    parts_class = PosixPath

    def __init__(self, pth):
        """
        Validates OBS path is in the proper format.

        Args:
            pth (str): A path that matches the format of
                ``(drive)://{bucket_name}/{rest_of_path}`` where (drive)
                can be any of s3 or swift.
                The ``(drive)://`` prefix is required in the path.
        """
        if not hasattr(pth, 'startswith') or not pth.startswith(self.drive):
            raise ValueError('path must have %s (got %r)' % (self.drive, pth))
        return super(OBSPath, self).__init__(pth)

    copy = utils.copy
    copytree = utils.copytree

    def __repr__(self):
        return '%s("%s")' % (type(self).__name__, self)

    def is_ambiguous(self):
        """Returns true if it cannot be determined if the path is a
        file or directory
        """
        return not self.endswith('/') and not self.ext

    def _get_parts(self):
        """Returns the path parts (excluding the drive) as a list of strings."""
        if len(self) > len(self.drive):
            return self[len(self.drive):].split('/')
        else:
            return []

    @property
    def name(self):
        """The name of the path, mimicking path.py's name property"""
        return self.parts_class(super(OBSPath, self).name)

    @property
    def parent(self):
        """The parent of the path, mimicking path.py's parent property"""
        return self.path_class(super(OBSPath, self).parent)

    @property
    def resource(self):
        """Returns the resource as a ``PosixPath`` object or None.

        A resource can be a single object or a prefix to objects.
        Note that it's important to keep the trailing slash in a resource
        name for prefix queries.
        """
        parts = self._get_parts()
        joined_resource = '/'.join(parts[1:]) if len(parts) > 1 else None

        return self.parts_class(joined_resource) if joined_resource else None

    def normpath(self):
        """Normalize path following linux conventions (keeps drive prefix)"""
        normed = posixpath.normpath('/' + str(self)[len(self.drive):])[1:]
        return self.path_class(self.drive + normed)

    def read_object(self):
        raise NotImplementedError

    def write_object(self, content):
        """Writes an individual object."""
        raise NotImplementedError

    def open(self, mode='r'):
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

    def isabs(self):
        return True

    def isdir(self):
        raise NotImplementedError

    def isfile(self):
        raise NotImplementedError

    def islink(self):
        return False

    def ismount(self):
        return True

    def getsize(self):
        """Returns size, in bytes of path."""
        raise NotImplementedError

    def remove(self):
        """ Delete single path or object """
        raise NotImplementedError

    def rmtree(self):
        """Delete entire directory (or all paths starting with prefix).

        See shutil.rmtree"""
        raise NotImplementedError

    def stat(self):
        raise NotImplementedError

    def walkfiles(self, pattern=None):
        """Iterate over files recursively.

        Args:
            pattern (str, optional): Limits the results to files
                with names that match the pattern.  For example,
                ``mydir.walkfiles('*.tmp')`` yields only files with the ``.tmp``
                extension.

        Returns:
            Iter[Path]: Files recursively under the path
        """
        for f in self.list(ignore_dir_markers=True):
            if pattern is None or f.fnmatch(pattern):
                yield f

    def download_object(self, dest):
        """Download a single path or object to file."""
        raise NotImplementedError

    def download(self, dest, condition=None, use_manifest=False, **kwargs):
        """Download a directory."""
        raise NotImplementedError

    def upload(self, source, condition=None, use_manifest=False, headers=None):
        """Upload a list of files and directories to a directory."""
        raise NotImplementedError


class OBSFile(object):
    """
    Provides methods for reading and writing OBS objects returned by
    `OBSPath.open`.

    Objects are retrieved from `OBSPath.open`. For example::

        obj = Path('swift://tenant/container/object').open(mode='r')
        contents = obj.read()

    The above opens an object and reads its contents. To write to an
    object::

        obj = Path('s3://bucket/object').open(mode='w')
        obj.write('hello ')
        obj.write('world')
        obj.close()

    Note that the writes will not be commited until the object has been
    closed. It is recommended to use `OBSPath.open` as a context manager
    to avoid forgetting to close the resource::

        with Path('swift://tenant/container/object').open(mode='r') as obj:
            obj.write('Hello world!')
    """
    closed = False
    _READ_MODES = ('r', 'rb')
    _WRITE_MODES = ('w', 'wb')
    _VALID_MODES = _READ_MODES + _WRITE_MODES

    def __init__(self, pth, mode='r', **kwargs):
        """Initializes a file object

        Args:
            pth (Path): The path that represents an individual
                object
            mode (str): The mode of the resource. Can be "r" and "rb" for
                reading the resource and "w" and "wb" for writing the
                resource.
        """
        if mode not in self._VALID_MODES:
            raise ValueError('invalid mode for file: %r' % mode)
        self._path = pth
        self.mode = mode
        self._kwargs = kwargs

    def __enter__(self):
        if self.closed:
            raise ValueError('I/O operation on closed file.')
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def __iter__(self):
        # ape file object behavior by returning self (and attempting *not* to
        # give users access to underlying buffer)
        return self

    @cached_property
    def _buffer(self):
        "Cached buffer of data read from or to be written to Object Storage"
        if self.mode in ('r', 'rb'):
            return cStringIO.StringIO(self._path.read_object())
        elif self.mode in ('w', 'wb'):
            return cStringIO.StringIO()
        else:
            raise ValueError('cannot obtain buffer in mode: %r' % self.mode)

    seek = _delegate_to_buffer('seek', valid_modes=_VALID_MODES)
    tell = _delegate_to_buffer('tell', valid_modes=_VALID_MODES)

    read = _delegate_to_buffer('read', valid_modes=_READ_MODES)
    readlines = _delegate_to_buffer('readlines', valid_modes=_READ_MODES)
    readline = _delegate_to_buffer('readline', valid_modes=_READ_MODES)
    # In Python 3 it's __next__, in Python 2 it's next()
    # __next__ = _delegate_to_buffer('__next__', valid_modes=_READ_MODES)
    # TODO: Only use in Python 2 context
    next = _delegate_to_buffer('next', valid_modes=_READ_MODES)

    write = _delegate_to_buffer('write', valid_modes=_WRITE_MODES)
    writelines = _delegate_to_buffer('writelines', valid_modes=_WRITE_MODES)
    truncate = _delegate_to_buffer('truncate', valid_modes=_WRITE_MODES)

    @property
    def name(self):
        return self._path

    def close(self):
        if self.mode in self._WRITE_MODES:
            self.flush()
        self._buffer.close()
        self.closed = True
        del self.__dict__['_buffer']

    def flush(self):
        """Flushes the write buffer to the OBS path (if it exists)"""
        if self.mode not in self._WRITE_MODES:
            raise TypeError("File must be in modes %s to 'flush'" %
                            (self._WRITE_MODES,))
        if self._buffer.tell():
            self._path.write_object(self._buffer.getvalue(),
                                    **self._kwargs)
