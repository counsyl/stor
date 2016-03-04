"""
storage-utils
=============

Storage utils is a library that aims to make it easy to write code that works
with both local posix filesystems and Swift Object Based Storage. In general,
you should be able to replace most uses of ``os.path`` and ``open`` with::

    import storage_utils as path
    from storage_utils import open

And your code will work either with posix paths or swift paths (defined as
strings in the format ``swift://<TENANT>/<CONTAINER>/<OBJECT>``).  Storage
Utils also provides an object-oriented API similar to Python 3's new pathlib,
accessible via ``storage_utils.Path``.

storage_utils is heavily inspired by / based on the path.py library, but
modified to avoid the need to know whether you have a Path or a string for most
functions.

See `storage_utils.swift` for more information on Swift-specific functionality.
"""

from storage_utils.utils import copy
from storage_utils.utils import copytree
from storage_utils.utils import is_filesystem_path
from storage_utils.utils import is_swift_path
from storage_utils.utils import NamedTemporaryDirectory
from storage_utils.base import Path


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
listdir = _delegate_to_path('listdir')
glob = _delegate_to_path('glob')
exists = _delegate_to_path('exists')
isabs = _delegate_to_path('isabs')
isdir = _delegate_to_path('isdir')
isfile = _delegate_to_path('isfile')
islink = _delegate_to_path('islink')
ismount = _delegate_to_path('ismount')
remove = _delegate_to_path('remove')
rmtree = _delegate_to_path('rmtree')


def path(pth):
    import warnings

    # DeprecationWarnings are hidden by default. We want to get rid of this
    # sooner rather than later.
    warnings.warn('Using the ``path()`` function directly is deprecated -'
                  ' either use storage_utils.Path or the functional API'
                  ' directly', UserWarning)
    return Path(pth)


__all__ = [
    'copy',
    'copytree',
    'is_filesystem_path',
    'is_swift_path',
    'NamedTemporaryDirectory',
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
    'remove',
    'rmtree',
]
