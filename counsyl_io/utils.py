from contextlib import contextmanager
from counsyl_io.os_path import OSPath
import os
import tempfile


@contextmanager
def chdir(dirname):
    curdir = os.getcwd()
    try:
        os.chdir(dirname)
        yield
    finally:
        os.chdir(curdir)


@contextmanager
def NamedTemporaryDirectory(suffix='', prefix='tmp', dir=None,
                            change_dir=False):
    """Context manager for creating and deleting temporary directory.

    if change_dir=True, cd to that directory.

    Name is CamelCase to match tempfile.NamedTemporaryFile.
    """
    tempdir = OSPath(tempfile.mkdtemp(suffix, prefix, dir))
    if change_dir:
        with chdir(str(tempdir)):
            yield tempdir
    else:
        yield tempdir
    tempdir.rmtree()
