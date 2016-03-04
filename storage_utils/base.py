from storage_utils import utils
from storage_utils.third_party.path import ClassProperty

import glob
import os
import ntpath
import posixpath
import sys

from six.moves import builtins
from six import text_type
from six import string_types
from six import PY3

class Path(text_type):
    """
    Wraps path operations with an object-oriented API that makes it easier to
    combine and also to work with OBS and local paths via a single API. Methods
    on this class will be implemented by all subclasses of path.

    Using the class-level constructor returns a concrete subclass based on
    prefix and current environment.

    Examples::

        >>> from storage_utils import Path
        >>> Path('/some/path')
        PosixPath('/some/path')
        >>> Path('swift://AUTH_something/cont/blah')
        SwiftPath('swift://AUTH_something/cont/blah')
    """

    def __new__(cls, path):
        if cls is not Path:
            return cls(path)
        if utils.is_swift_path(path):
            from storage_utils.swift import SwiftPath

            return SwiftPath(path)
        elif os.path == ntpath:
            from storage_utils.windows import WindowsPath

            return WindowsPath(path)
        elif os.path == posixpath:
            from storage_utils.posix import PosixPath

            return PosixPath(path)
        else:  # pragma: no cover
            assert False, 'path is not compatible with storage utils'

    @ClassProperty
    @classmethod
    def path_module(cls):
        """The path module used for path manipulation functions"""
        raise NotImplementedError('must implement path_module')

    @ClassProperty
    @classmethod
    def path_class(cls):
        """What class should be used to construct new instances from this class"""
        return cls

    _next_class = path_class

    def _has_incompatible_path_module(self, other):
        """Returns true if the other path is a storage utils path and has a
        compatible path module for path operations"""
        return isinstance(other, Path) and other.path_module != self.path_module

    copy = utils.copy
    copytree = utils.copytree

    def __repr__(self):
        return '%s(%s)' % (type(self).__name__, super(Path, self).__repr__())

    def __div__(self, rel):
        """Join two path components (self / rel), adding a separator character if needed."""
        if self._has_incompatible_path_module(rel):
            return NotImplemented
        return self.path_class(self.path_module.join(self, rel))

    def __rdiv__(self, rel):
        """Join two path components (rel / self), adding a separator character if needed."""
        if self._has_incompatible_path_module(rel):
            return NotImplemented
        return self.path_class(self.path_module.join(rel, self))

    # Make the / operator work even when true division is enabled.
    __truediv__ = __div__
    __rtruediv__ = __rdiv__

    def __add__(self, more):
        if self._has_incompatible_path_module(more):
            return NotImplemented
        return self.path_class(super(Path, self).__add__(more))

    def __radd__(self, other):
        if self._has_incompatible_path_module(other):
            return NotImplemented
        if not isinstance(other, string_types):
            return NotImplemented
        return self.path_class(other.__add__(self))

    #
    # --- Operations on Path strings.

    def abspath(self):
        """ .. seealso:: :func:`os.path.abspath` """
        return self._next_class(self.module.abspath(self))

    def normcase(self):
        """ .. seealso:: :func:`os.path.normcase` """
        return self._next_class(self.module.normcase(self))

    def normpath(self):
        """ .. seealso:: :func:`os.path.normpath` """
        return self._next_class(self.module.normpath(self))

    def realpath(self):
        """ .. seealso:: :func:`os.path.realpath` """
        return self._next_class(self.module.realpath(self))

    def expanduser(self):
        """ .. seealso:: :func:`os.path.expanduser` """
        return self._next_class(self.module.expanduser(self))

    def expandvars(self):
        """ .. seealso:: :func:`os.path.expandvars` """
        return self._next_class(self.module.expandvars(self))

    def dirname(self):
        """ .. seealso:: :attr:`parent`, :func:`os.path.dirname` """
        return self._next_class(self.module.dirname(self))

    def basename(self):
        """ .. seealso:: :attr:`name`, :func:`os.path.basename` """
        return self._next_class(self.module.basename(self))

    def expand(self):
        """ Clean up a filename by calling :meth:`expandvars()`,
        :meth:`expanduser()`, and :meth:`normpath()` on it.

        This is commonly everything needed to clean up a filename
        read from a configuration file, for example.
        """
        return self.expandvars().expanduser().normpath()

    @property
    def parent(self):
        return self.dirname()

    @property
    def name(self):
        return self.basename()

    def splitpath(self):
        """ p.splitpath() -> Return ``(p.parent, p.name)``.

        (naming is to avoid colliding with str.split)

        .. seealso:: :attr:`parent`, :attr:`name`, :func:`os.path.split`
        """
        parent, child = self.module.split(self)
        return self._next_class(parent), child

    def splitext(self):
        """ p.splitext() -> Return ``(p.stripext(), p.ext)``.

        Split the filename extension from this path and return
        the two parts.  Either part may be empty.

        The extension is everything from ``'.'`` to the end of the
        last path segment.  This has the property that if
        ``(a, b) == p.splitext()``, then ``a + b == p``.

        .. seealso:: :func:`os.path.splitext`
        """
        filename, ext = self.module.splitext(self)
        return self._next_class(filename), ext

    def joinpath(self, *others):
        """
        Join first to zero or more :class:`Path` components, adding a separator
        character (:samp:`{first}.module.sep`) if needed.  Returns a new
        instance of :samp:`{first}._next_class`.

        .. seealso:: :func:`os.path.join`
        """
        return self._next_class(self.module.join(self, *others))

    def open(self, **kwargs):
        raise NotImplementedError

    def listdir(self):
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
        """ .. seealso:: :func:`os.path.isabs`

        Always True with SwiftPath"""
        raise NotImplementedError

    def isdir(self):
        """ .. seealso:: :func:`os.path.isdir` """
        raise NotImplementedError

    def isfile(self):
        """ .. seealso:: :func:`os.path.isfile` """
        raise NotImplementedError

    def islink(self):
        """ .. seealso:: :func:`os.path.islink`

        Always False on Swift."""
        raise NotImplementedError

    def ismount(self):
        """ .. seealso:: :func:`os.path.ismount`

        Always True on Swift.
        """
        raise NotImplementedError


class FileSystemPath(Path):
    """'Abstract' class implementing file-system specific operations.

    In particular: allows changing directory when used as contextmanager and
    wraps Python's builtin open() to be compatible with swift_upload_args.
    """
    def open(self, *args, **kwargs):
        """
        Opens a path and retains interface compatibility with
        `SwiftPath` by popping the unused ``swift_upload_args`` keyword
        argument.
        """
        kwargs.pop('swift_upload_kwargs', None)
        return builtins.open(*args, **kwargs)

    def __enter__(self):
        self._old_dir = os.getcwd()
        os.chdir(self)
        return self

    def __exit__(self, *_):
        os.chdir(self._old_dir)

    def stat(self):
        """Performs os.stat on object.

        Not part of cross-compatible API because SwiftPath.stat() returns a
        dictionary of headers (and most attributes would not map anyways).
        """
        return self.module.stat(self)

    @staticmethod
    def _always_unicode(path):
        """
        Ensure the path as retrieved from a Python API, such as
        :func:`os.listdir`, is a proper Unicode string.
        """
        if PY3 or isinstance(path, text_type):
            return path
        return path.decode(sys.getfilesystemencoding(), 'surrogateescape')

    def listdir(self):
        """D.listdir() -> List of items in this directory.

        The elements of the list are Path objects.
        """
        return [self / child
                for child in map(self._always_unicode, os.listdir(self))]

    def glob(self, pattern):
        cls = self._next_class
        return [cls(s) for s in glob.glob(self / pattern)]

    def exists(self):
        """ .. seealso:: :func:`os.path.exists` """
        return self.module.exists(self)

    def isabs(self):
        """ .. seealso:: :func:`os.path.isabs` """
        return self.module.isabs(self)

    def isdir(self):
        """ .. seealso:: :func:`os.path.isdir` """
        return self.module.isdir(self)

    def isfile(self):
        """ .. seealso:: :func:`os.path.isfile` """
        return self.module.isfile(self)

    def islink(self):
        """ .. seealso:: :func:`os.path.islink` """
        return self.module.islink(self)

    def ismount(self):
        """ .. seealso:: :func:`os.path.ismount` """
        return self.module.ismount(self)
