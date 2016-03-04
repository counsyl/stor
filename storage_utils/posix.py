"""
Provides functionality for accessing resources on Posix file systems.
"""
import posixpath
from storage_utils import base


class PosixPath(base.StorageUtilsPath):
    """Represents a posix path.

    This class inherits a vendored path.py ``Path`` object and
    overrides methods for cross compatibility with swift.

    This class overrides ``Path`` object's
    ``module`` attribute and sets it to ``posixpath`` to ensure
    that it always represents a posix path.
    """
    module = posixpath
