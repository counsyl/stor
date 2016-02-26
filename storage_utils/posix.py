"""
Provides functionality for accessing resources on Posix file systems.
"""
import posixpath
from storage_utils import base


class PosixPath(base.StorageUtilsPath):
    """Represents a posix path.

    This class overrides ``Path`` object's
    ``module`` attribute to ensure that it always represents
    a posix path.
    """
    module = posixpath
