def is_swift_path(p):
    """Determines if the path is a Swift path.

    All Swift paths start with swift://

    Args:
        p (str): The path string

    Returns:
        bool: True if p is a Swift path, False otherwise.
    """
    from storage_utils.swift_path import SwiftPath
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
        <class 'storage_utils.swift_path.Path'>
        >>> print p.exists()
        False
    """
    if is_swift_path(p):
        from storage_utils.swift_path import SwiftPath
        return SwiftPath(p)
    else:
        from path import Path
        return Path(p)
