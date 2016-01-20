"""
Provides functionality for accessing resources on Posix file systems.
"""
from storage_utils.third_party.path import Path
from storage_utils import utils


class PosixPath(Path):
    """Provides additional functionality on the Path class from path.py

    Full documentation for path.py can be viewed at
    http://pythonhosted.org/path.py/api.html.
    """
    copy = utils.copy
