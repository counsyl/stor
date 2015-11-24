import os
from path import Path as BasePath


class OSPath(BasePath):
    """Represents a path with additional functionality.
    """
    @classmethod
    def _curdir_or_join_args(cls, *args):
        """Returns the current directory or joined args.

        If there are no args, return the current directory.
        Otherwise, join the args into one path.
        Verify all args are strings.

        Used by __init__ and __new__ when constructing a path.
        """
        for arg in args:
            if not isinstance(arg, basestring):
                raise ValueError('%s() arguments must be Path, str or '
                                 'unicode' % cls.__name__)

        return os.curdir if not args else os.path.join(*args)

    def __new__(typ, *args):
        p = typ._curdir_or_join_args(*args)
        return super(OSPath, typ).__new__(typ, p)

    def __init__(self, *args):
        p = self._curdir_or_join_args(*args)
        return super(OSPath, self).__init__(p)

    def absexpand(self):
        """Make fully-expanded and absolute path.
        """
        return self.expand().abspath()
