from storage_utils import utils
from storage_utils.third_party.path import Path


class StorageUtilsPathMixin(object):
    """Provides basic cross compatibility between paths on different file
    systems.

    Objects that utilize this mixin are able to be cross compatible when
    copying between storage systems, for example, copying from a posix file
    system to swift or vice versa.


    """
    copy = utils.copy
    copytree = utils.copytree


class StorageUtilsPath(StorageUtilsPathMixin, Path):
    """
    A base Path object that utilizes a vendored path.py module
    and provides basic cross-compatibility on operations between
    paths on different file systems.

    For documentation on path.py, view
    https://pythonhosted.org/path.py/api.html

    For documentation on individual methods in the ``Path`` object,
    consult their counterparts in :mod:`os.path`.

    Some methods are additionally included from :mod:`shutil`.
    The functions are linked directly into the class namespace
    such that they will be bound to the Path instance. For example,
    ``Path(src).samepath(target)`` is equivalent to
    ``os.path.samepath(src, target)``. Therefore, when referencing
    the docs for these methods, assume ``src`` references ``self``,
    the Path instance.
    """
    def open(self, *args, **kwargs):
        """
        Opens a path and retains interface compatibility with
        `SwiftPath` by popping the unused ``swift_upload_args`` keyword
        argument.
        """
        kwargs.pop('swift_upload_kwargs', None)
        return super(StorageUtilsPath, self).open(*args, **kwargs)
