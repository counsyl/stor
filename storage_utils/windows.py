"""
Provides functionality for accessing resources on Windows file systems.
"""
import ntpath
from storage_utils import base


class WindowsPath(base.StorageUtilsPath):
    """Represents a windows path.

    This class overrides ``Path`` object's
    ``module`` attribute to ensure that it always represents
    a windows path.
    """
    module = ntpath
