"""
Counsyl Storage Utils comes with the ability to create paths in a similar
manner to `path.py <https://pypi.python.org/pypi/path.py>`_. It is expected
that the main functions below are the only ones directly used.
(i.e. ``Path`` or ``SwiftPath`` objects should never be explicitly
instantiated).
"""

from storage_utils.utils import NamedTemporaryDirectory  # flake8: noqa


def is_swift_path(p):
    """Determines if the path is a Swift path.

    All Swift paths start with swift://

    Args:
        p (str): The path string

    Returns:
        bool: True if p is a Swift path, False otherwise.
    """
    from storage_utils.swift import SwiftPath
    return p.startswith(SwiftPath.swift_drive)


def is_posix_path(p):
    """Determines if the path is a posix path.

    This utility assumes that all paths that aren't swift paths
    are posix file system paths.

    Args:
        p (str): The path string

    Returns:
        bool: True if p is a posix path, False otherwise.
    """
    return not is_swift_path(p)


def path(p):
    """A factory function for returning a path based on its prefix.

    Args:
        p (str): The path string

    Examples:

        >>> from storage_utils import path
        >>> p = path('/my/local/path')
        >>> print type(p)
        <class 'path.Path'>
        >>> print p.exists()
        False

        >>> from storage_utils import path
        >>> p = path('swift://tenant/container/object')
        >>> print type(p)
        <class 'storage_utils.swift.SwiftPath'>
        >>> print p.exists()
        False

    Note that it's okay to repeatedly call ``path`` on any ``Path`` object
    since it will return itself.
    """
    if is_swift_path(p):
        from storage_utils.swift import SwiftPath
        return SwiftPath(p)
    else:
        from path import Path
        return Path(p)
