"""
Provides functionality for accessing resources on Posix file systems.
"""
from storage_utils.third_party.path import Path
from storage_utils import utils


class PosixPath(Path):
    """
    Represents a filesystem path.

    For documentation on individual methods, consult their
    counterparts in :mod:`os.path`.

    Some methods are additionally included from :mod:`shutil`.
    The functions are linked directly into the class namespace
    such that they will be bound to the Path instance. For example,
    ``Path(src).samepath(target)`` is equivalent to
    ``os.path.samepath(src, target)``. Therefore, when referencing
    the docs for these methods, assume ``src`` references ``self``,
    the Path instance.

    Provides additional functionality on the Path class from path.py,
    vendored into ``storage_utils.third_party.path``, specifically around the
    ``copy`` command.
    """
    copy = utils.copy

    def open(self, *args, **kwargs):
        """
        Opens a posix path and retains interface compatibility with
        `SwiftPath` by popping the unused ``swift_upload_args`` keyword
        argument.
        """
        kwargs.pop('swift_upload_kwargs', None)
        return super(PosixPath, self).open(*args, **kwargs)
