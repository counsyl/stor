from path import Path
from storage_utils import swift_path


def path(p):
    """A factory function for returning a path based on its prefix.

    Examples:

        >>> from storage_utils import path
        >>> p = path('/my/local/path')
        >>> print type(p)
        <class 'storage_utils.os_path.OSPath'>
        >>> print p.exists()
        False

        >>> from storage_utils import path
        >>> p = path('swift://tenant/container/object')
        >>> print type(p)
        <class 'storage_utils.swift_path.SwiftPath'>
        >>> print p.exists()
        False
    """
    if p.startswith('swift://'):
        return swift_path.SwiftPath(p)
    else:
        return Path(p)
