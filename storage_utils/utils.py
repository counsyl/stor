from contextlib import contextmanager
import os
from subprocess import check_call
from swiftclient.service import SwiftUploadObject
import tempfile


def is_swift_path(p):
    """Determines if the path is a Swift path.

    All Swift paths start with swift://

    Args:
        p (str): The path string

    Returns:
        bool: True if p is a Swift path, False otherwise.
    """
    from storage_utils.swift import SwiftPath
    return p.startswith(SwiftPath.swift_drive)


def is_posix_path(p):
    """Determines if the path is a posix path.

    This utility assumes that all paths that aren't swift paths
    are posix file system paths.

    Args:
        p (str): The path string

    Returns:
        bool: True if p is a posix path, False otherwise.
    """
    return not is_swift_path(p)


def path(p):
    """A factory function for returning a path based on its prefix.

    Args:
        p (str): The path string

    Examples:

        >>> from storage_utils import path
        >>> p = path('/my/local/path')
        >>> print type(p)
        <class 'path.Path'>
        >>> print p.exists()
        False

        >>> from storage_utils import path
        >>> p = path('swift://tenant/container/object')
        >>> print type(p)
        <class 'storage_utils.swift.SwiftPath'>
        >>> print p.exists()
        False

    Note that it's okay to repeatedly call ``path`` on any ``Path`` object
    since it will return itself.
    """
    if is_swift_path(p):
        from storage_utils.swift import SwiftPath
        return SwiftPath(p)
    else:
        from storage_utils.posix import PosixPath
        return PosixPath(p)


def copy(source, dest, **retry_args):
    """Copies a source file to a destination file.

    Note that this utility can be called from either swift or posix
    paths created with `storage_utils.path`.

    Args:
        src (path|str): The source directory to copy from
        dest (path|str): The destination file. In contrast to
            ``shutil.copy``, the parent directory is created if it doesn't
            already exist.
        retry_args (dict): Optional retry arguments to use for swift upload
            or download. View the swift module-level documentation for more
            information on retry arguments

    Examples:
        Copying a swift file to a local path behaves as follows::

            from storage_utils import path
            swift_p = path('swift://tenant/container/dir/file.txt')
            # file.txt will be copied to other_dir/other_file.txt
            swift_p.copy('other_dir/other_file.txt')

        Copying from a local path to swift behaves as follows::

            from storage_utils import path
            local_p = path('my/local/file.txt')
            # File will be uploaded to swift://tenant/container/dir/my_file.txt
            local_p.copy('swift://tenant/container/dir/my_file.txt')
    """
    source = path(source)
    dest = path(dest)
    if is_swift_path(source) and is_swift_path(dest):
        raise ValueError('cannot copy one swift path to another swift path')
    if not source.isfile():
        raise ValueError('source must be a file that ends with an extension')
    if is_swift_path(dest) and not (dest.isfile() or dest.isdir()):
        raise ValueError('swift destination must be file with extension or directory with slash')

    dest_file = dest if dest.name else dest / source.name
    if is_posix_path(dest):
        dest_file.makedirs_p()
        if is_swift_path(source):
            source.download_object(dest_file)
        else:
            check_call(['cp', str(source), str(dest)])
    else:
        if not dest_file.parent.container:
            raise ValueError((
                'cannot copy to tenant "%s" and file '
                '"%s"' % (dest_file.parent, dest_file.name)
            ))
        else:
            print dest_file.parent, dest_file
            print 'hi'
        dest_obj_name = path(dest_file.parent.resource or '') / dest_file.name
        dest_file.parent.upload([SwiftUploadObject(source, object_name=dest_obj_name)])
        return dest_file.parent, source


def copytree(source, dest, copy_cmd='cp -r', object_threads=20,
             segment_threads=20, **retry_args):
    """Copies a source directory to a destination directory. Assumes that
    paths are capable of being copied to/from.

    Args:
        src (path|str): The source directory to copy from
        dest (path|str): The directory to copy to
        copy_cmd (str): If copying to / from posix, this command is
            used.
        object_threads (int): The amount of object threads to use
            for swift uploads / downloads.
        segment_threads (int): The amount of segment threads to use
            for swift uploads.
        retry_args (dict): Optional retry arguments to use for swift upload
            or download. View the swift module-level documentation for more
            information on retry arguments
    """
    source = path(source)
    dest = path(dest)
    if is_swift_path(source) and is_swift_path(dest):
        raise ValueError('cannot copy one swift path to another swift path')

    if is_posix_path(dest):
        dest.expand().abspath().parent.makedirs_p()
        if is_swift_path(source):
            source.download(dest, object_threads=object_threads)
        else:
            formatted_copy_cmd = copy_cmd.split()
            formatted_copy_cmd.extend([str(source), str(dest)])
            check_call(formatted_copy_cmd)
    else:
        with source:
            dest.upload(['.'],
                        object_threads=object_threads,
                        segment_threads=segment_threads)


def walk_files_and_dirs(files_and_dirs):
    """Walk all files and directories.

    Args:
        files_and_dirs (List[str]): All file or directory names to walk.

    Returns:
        List[str]: All files and empty directories under files_and_dirs.

    Raises:
        ValueError: The provided upload name is not a file or a directory.

    Examples:
        >>> from storage_utils.utils import walk_files_and_dirs
        >>> results = walk_files_and_dirs(['file_name', 'dir_name'])
        >>> print results
        ['file_name', 'dir_name/file1', 'dir_name/file2']
    """
    walked_upload_names = []
    for name in files_and_dirs:
        if os.path.isfile(name):
            walked_upload_names.append(name)
        elif os.path.isdir(name):
            for (_dir, _ds, _fs) in os.walk(name):
                if not (_ds + _fs):
                    # Ensure that empty directories are uploaded as well
                    walked_upload_names.append(_dir)
                else:
                    walked_upload_names.extend([
                        os.path.join(_dir, _f) for _f in _fs
                    ])
        else:
            raise ValueError('file "%s" not found' % name)

    return walked_upload_names


@contextmanager
def NamedTemporaryDirectory(suffix='', prefix='tmp', dir=None,
                            change_dir=False):
    """Context manager for creating and deleting temporary directory.

    Mimics the behavior of tempfile.NamedTemporaryFile.

    Arguments:
        suffix (str): If specified, the dir name will end with it.
        prefix (str): If specified, the dir name will start with it,
            otherwise 'tmp' is used.
        dir (str): If specified, the dir will be created in this
            directory.
        change_dir (bool): If specified, will change to the temporary
            directory.

    Yields:
        Path: The temporary directory.

    Note:
        Name is CamelCase to match tempfile.NamedTemporaryFile.

    Examples:
        >>> from storage_utils import NamedTemporaryDirectory
        >>> with NamedTemporaryDirectory() as d:
        >>>     # Do operations within "d", which will be deleted afterwards
    """
    tempdir = path(tempfile.mkdtemp(suffix, prefix, dir))
    if change_dir:
        with tempdir:
            yield tempdir
    else:
        yield tempdir
    tempdir.rmtree()
