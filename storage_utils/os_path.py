from path import Path


class OSPath(Path):
    """Represents a path with additional functionality.
    """
    def absexpand(self):
        """Make fully-expanded and absolute path.

        Examples:
            >>> p = OSPath('my/local/path')
            >>> print p.absexpand()
            /Users/wes/counsyl/counsyl-storage-utils/my/local/path
        """
        return self.expand().abspath()
