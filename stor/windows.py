"""
Provides functionality for accessing resources on Windows file systems.
"""
import ntpath
from stor import base


class WindowsPath(base.FileSystemPath):
    """Represents a windows path.

    This class inherits a vendored path.py ``Path`` object and
    overrides methods for cross compatibility with swift.

    This class overrides ``Path`` object's
    ``module`` attribute and sets it to ``ntpath`` to ensure
    that it always represents a windows path.
    """
    path_module = ntpath
