def path(p):
    """A factory function for returning a path based on its prefix.

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
    if p.startswith('swift://'):
        from storage_utils.swift_path import SwiftPath
        return SwiftPath(p)
    else:
        from path import Path
        return Path(p)
