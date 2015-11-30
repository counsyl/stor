import os
from path import Path


class OSPath(Path):
    """Represents a path with additional functionality.
    """
    @classmethod
    def _curdir_or_join_args(cls, *args):
        for arg in args:
            if not isinstance(arg, basestring):
                raise ValueError('%s() arguments must be Path, str or '
                                 'unicode' % cls.__name__)

        return os.curdir if not args else os.path.join(*args)

    def __new__(typ, *args):
        """Constructs a new path.

        Args:
            *args: If there are no args, os.curdir is used as the path.
                If there are single or multiple arguments, the paths are
                joined using os.path.join and used as the path.

        Examples:
            >>> p = OSPath()
            >>> print p
            .

            >>> p = OSPath('my/path')
            >>> print p
            my/path

            >>> p = OSPath('my', 'path')
            >>> print p
            my/path

        """
        p = typ._curdir_or_join_args(*args)
        return super(OSPath, typ).__new__(typ, p)

    def __init__(self, *args):
        """Initializes a new path. Uses the same process as __new__.
        """
        p = self._curdir_or_join_args(*args)
        return super(OSPath, self).__init__(p)

    def absexpand(self):
        """Make fully-expanded and absolute path.

        Examples:
            >>> p = OSPath('my/local/path')
            >>> print p.absexpand()
            /Users/wes/counsyl/counsyl-storage-utils/my/local/path
        """
        return self.expand().abspath()
