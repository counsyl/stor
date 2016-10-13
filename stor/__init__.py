"""
stor
====

Stor is a library that aims to make it easy to write code that works
with both local posix filesystems and Swift Object Based Storage. In general,
you should be able to replace most uses of ``os.path`` and ``open`` with::

    import stor as path
    from stor import open

And your code will work either with posix paths or swift paths (defined as
strings in the format ``swift://<TENANT>/<CONTAINER>/<OBJECT>``).  Stor
also provides an object-oriented API similar to Python 3's new pathlib,
accessible via ``stor.Path``.

stor is heavily inspired by / based on the path.py library, but
modified to avoid the need to know whether you have a Path or a string for most
functions.

See `stor.swift` for more information on Swift-specific functionality.
"""
import pkg_resources

from stor.utils import copy
from stor.utils import copytree
from stor.utils import is_filesystem_path
from stor.utils import is_swift_path
from stor.utils import NamedTemporaryDirectory
from stor.base import Path
from stor import settings


# TODO: Compile this - costs ~700us to do this on import
try:
    __version__ = pkg_resources.get_distribution('stor').version
except pkg_resources.DistributionNotFound:  # pragma: no cover
    # we are not pip installed in environment
    __version__ = None


settings._initialize()


def _delegate_to_path(name):
    def wrapper(path, *args, **kwargs):
        f = getattr(Path(path), name)
        return f(*args, **kwargs)
    wrapper.__doc__ = getattr(Path, name).__doc__
    wrapper.__name__ = name
    return wrapper

# extra compat!
open = _delegate_to_path('open')
abspath = _delegate_to_path('abspath')
normcase = _delegate_to_path('normcase')
normpath = _delegate_to_path('normpath')
realpath = _delegate_to_path('realpath')
expanduser = _delegate_to_path('expanduser')
expandvars = _delegate_to_path('expandvars')
dirname = _delegate_to_path('dirname')
basename = _delegate_to_path('basename')
expand = _delegate_to_path('expand')
# these two have special names to not collide with builtins
join = _delegate_to_path('joinpath')
split = _delegate_to_path('splitpath')
splitext = _delegate_to_path('splitext')
list = _delegate_to_path('list')
listdir = _delegate_to_path('listdir')
exists = _delegate_to_path('exists')
isabs = _delegate_to_path('isabs')
isdir = _delegate_to_path('isdir')
isfile = _delegate_to_path('isfile')
islink = _delegate_to_path('islink')
ismount = _delegate_to_path('ismount')
getsize = _delegate_to_path('getsize')
remove = _delegate_to_path('remove')
rmtree = _delegate_to_path('rmtree')
walkfiles = _delegate_to_path('walkfiles')


def glob(pth, pattern):  # pragma: no cover
    """ Glob for pattern relative to ``pth``.

    Note that Swift currently only supports a single trailing *"""
    # TODO(jtratner): support single argument for glob, which is more
    # comprehensible.
    return Path(pth).glob(pattern)


def listpath(pth):  # pragma: no cover
    import warnings

    # DeprecationWarnings are hidden by default. We want to get rid of this
    # sooner rather than later.
    warnings.warn('Using the ``stor.listpath()`` function directly is'
                  ' deprecated, use ``stor.list()`` instead', UserWarning)
    return list(pth)


def path(pth):  # pragma: no cover
    import warnings

    # DeprecationWarnings are hidden by default. We want to get rid of this
    # sooner rather than later.
    warnings.warn('Using the ``path()`` function directly is deprecated -'
                  ' either use stor.Path or the functional API'
                  ' directly', UserWarning)
    return Path(pth)


__all__ = [
    'abspath',
    'normcase',
    'normpath',
    'realpath',
    'expanduser',
    'expandvars',
    'dirname',
    'basename',
    'expand',
    'join',
    'split',
    'splitext',
    'listdir',
    'glob',
    'exists',
    'isabs',
    'isdir',
    'isfile',
    'islink',
    'ismount',
    'getsize',
    'copy',
    'copytree',
    'remove',
    'rmtree',
    'walkfiles',
    'is_filesystem_path',
    'is_swift_path',
    'NamedTemporaryDirectory',
]
