from counsyl_io import os_path
from counsyl_io import swift_path


def path(*args):
    """A factory function for returning a path based on its prefix.

    Examples:

        >>> from counsyl_io import path
        >>> p = path('/my/local/path')
        >>> print type(p)
        <class 'counsyl_io.os_path.OSPath'>
        >>> print p.exists()
        False

        >>> from counsyl_io import path
        >>> p = path('swift://tenant/container/object')
        >>> print type(p)
        <class 'counsyl_io.swift_path.SwiftPath'>
        >>> print p.exists()
        False
    """
    if len(args) == 1 and args[0].startswith('swift://'):
        return swift_path.SwiftPath(args[0])
    else:
        return os_path.OSPath(*args)
