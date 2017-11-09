"""
Provides functionality for accessing resources on Posix file systems.
"""
import os
import posixpath

from stor import base
from stor import utils


class PosixPath(base.FileSystemPath):
    """Represents a posix path.

    This class inherits a vendored path.py ``Path`` object and
    overrides methods for cross compatibility with swift.

    This class overrides ``Path`` object's
    ``module`` attribute and sets it to ``posixpath`` to ensure
    that it always represents a posix path.
    """
    path_module = posixpath

    def list(self):
        """
        List all files and directories under a path.

        Returns:
            List[str]: A list of all files and directories.
        """
        return list(utils.walk_files_and_dirs([self]).keys())

    def walkfiles(self, pattern=None):
        """Iterate over files recursively.

        Args:
            pattern (str, optional): Limits the results to files
                with names that match the pattern.  For example,
                ``mydir.walkfiles('*.tmp')`` yields only files with the ``.tmp``
                extension.

        Returns:
            Iter[Path]: Files recursively under the path
        Note:
            This may be much slower than list() because it checks twice that everything is a file.
        """
        for f in self.list():
            if os.path.isfile(f) and (pattern is None or f.fnmatch(pattern)):
                yield f
