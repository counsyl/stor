from contextlib import contextmanager
import datetime
import errno
import logging
import os
import shlex
import shutil
from subprocess import check_call
from swiftclient.service import SwiftUploadObject
import tempfile


logger = logging.getLogger(__name__)


def file_name_to_object_name(p):
    """Given a file path, construct its object name.

    Any relative or absolute directory markers at the beginning of
    the path will be stripped, for example::

        ../../my_file -> my_file
        ./my_dir -> my_dir
        .hidden_dir/file -> .hidden_dir/file
        /absolute_dir -> absolute_dir

    Note that windows paths will have their back slashes changed to
    forward slashes::

        C:\\my\\windows\\file -> my/windows/file

    Args:
        p (str): The input path

    Returns:
        PosixPath: The object name. An empty path will be returned in
            the case of the input path only consisting of absolute
            or relative directory markers (i.e. '/' -> '', './' -> '')
    """
    from storage_utils import Path
    from storage_utils.posix import PosixPath

    p_parts = Path(p).expand().splitdrive()[1].split(os.path.sep)
    obj_start = next((i for i, part in enumerate(p_parts) if part not in ('', '..', '.')), None)
    return PosixPath.parts_class('/'.join(p_parts[obj_start:]) if obj_start is not None else '')


def make_dest_dir(dest):
    """Make directories if they do not already exist.

    Raises:
        OSError: An error occurred while creating the directories.
            A specific exception is if a directory that is being created already exists as a file.
    """
    dest = os.path.abspath(dest)
    if not os.path.isdir(dest):
        try:
            os.makedirs(dest)
        except OSError as exc:
            if exc.errno == errno.ENOTDIR:
                raise OSError(errno.ENOTDIR,
                              'a parent directory of \'%s\' already exists as a file' % dest)
            else:
                raise


def with_trailing_slash(p):
    """Returns a path with a single trailing slash or None if not a path"""
    if not p:
        return p
    return type(p)(p.rstrip('/') + '/')


def is_swift_path(p):
    """Determines if the path is a Swift path.

    All Swift paths start with swift://

    Args:
        p (str): The path string

    Returns:
        bool: True if p is a Swift path, False otherwise.
    """
    from storage_utils.swift import SwiftPath
    return p.startswith(SwiftPath.drive)


def is_filesystem_path(p):
    """Determines if the path is posix or windows filesystem.

    Args:
        p (str): The path string

    Returns:
        bool: True if p is a Windows path, False otherwise.
    """
    return not (is_swift_path(p) or is_s3_path(p))


def is_s3_path(p):
    """Determines if the path is a S3 path.

    All S3 paths start with ``s3://``

    Args:
        p (str): The path string

    Returns
        bool: True if p is a S3 path, False otherwise.
    """
    from storage_utils.experimental.s3 import S3Path
    return p.startswith(S3Path.drive)


def copy(source, dest, swift_retry_options=None):
    """Copies a source file to a destination file.

    Note that this utility can be called from either swift, posix, or
    windows paths created with ``storage_utils.Path``.

    Args:
        source (path|str): The source directory to copy from
        dest (path|str): The destination file or directory.
        swift_retry_options (dict): Optional retry arguments to use for swift
            upload or download. View the
            `swift module-level documentation <swiftretry>` for more
            information on retry arguments

    Examples:
        Copying a swift file to a local path behaves as follows::

            >>> import storage_utils
            >>> swift_p = 'swift://tenant/container/dir/file.txt'
            >>> # file.txt will be copied to other_dir/other_file.txt
            >>> storage_utils.copy(swift_p, 'other_dir/other_file.txt')

        Copying from a local path to swift behaves as follows::

            >>> from storage_utils import Path
            >>> local_p = Path('my/local/file.txt')
            >>> # File will be uploaded to swift://tenant/container/dir/my_file.txt
            >>> local_p.copy('swift://tenant/container/dir/')

        Because of the ambiguity in whether a remote target is a file or directory, copy()
        will error on ambiguous paths.

            >>> local_p.copy('swift://tenant/container/dir')
            Traceback (most recent call last):
            ...
            ValueError: swift destination must be file with extension or directory with slash
    """
    from storage_utils import Path

    source = Path(source)
    dest = Path(dest)
    swift_retry_options = swift_retry_options or {}
    if is_swift_path(source) and is_swift_path(dest):
        raise ValueError('cannot copy one swift path to another swift path')
    if is_swift_path(dest) and dest.is_ambiguous():
        raise ValueError('swift destination must be file with extension or directory with slash')

    if is_filesystem_path(dest):
        if is_swift_path(source):
            dest_file = dest if not dest.isdir() else dest / source.name
            source.download_object(dest_file, **swift_retry_options)
        else:
            shutil.copy(source, dest)
    else:
        dest_file = dest if not dest.endswith('/') else dest / source.name
        if not dest_file.parent.container:
            raise ValueError((
                'cannot copy to tenant "%s" and file '
                '"%s"' % (dest_file.parent, dest_file.name)
            ))
        dest_obj_name = Path(dest_file.parent.resource or '') / dest_file.name
        dest_file.parent.upload([SwiftUploadObject(source, object_name=dest_obj_name)],
                                **swift_retry_options)


def copytree(source, dest, copy_cmd=None, use_manifest=False, headers=None,
             condition=None, **retry_args):
    """Copies a source directory to a destination directory. Assumes that
    paths are capable of being copied to/from.

    Note that this function uses shutil.copytree by default, meaning
    that a posix or windows destination must not exist beforehand.

    For example, assume the following file hierarchy::

        a/
        - b/
        - - 1.txt

    Doing a copytree from ``a`` to a new posix destination of ``c`` is
    performed with::

        path('a').copytree('c')

    The end result for c looks like::

        c/
        - b/
        - - 1.txt

    Note that the user can override which copy command is used for posix
    copies, and it is up to them to ensure that their code abides by the
    semantics of the provided copy command. This function has been tested
    in production using the default command of ``cp -r`` and using ``mcp -r``.

    Using swift source and destinations work in a similar manner. Assume
    the destination is a swift path and we upload the same ``a`` folder::

        path('a').copytree('swift://tenant/container/folder')

    The end swift result will have one object::

        path('swift://tenant/container/folder/b/1.txt')

    Similarly one can do::

        path('swift://tenant/container/folder/').copytree('c')

    The end result for c looks the same as the above posix example::

        c/
        - b/
        - - 1.txt

    Args:
        source (path|str): The source directory to copy from
        dest (path|str): The directory to copy to. Must not exist if
            its a posix directory
        copy_cmd (str): If copying to / from posix or windows, this command is
            used instead of shutil.copytree
        use_manifest (bool, default False): See `SwiftPath.upload` and
            `SwiftPath.download`.
        headers (List[str]): See `SwiftPath.upload`.

    Raises:
        ValueError: if two swift paths specified
        OSError: if destination is a posix path and it already exists
    """
    from storage_utils import Path

    source = Path(source)
    dest = Path(dest)
    if is_swift_path(source) and is_swift_path(dest):
        raise ValueError('cannot copy one swift path to another swift path')
    from storage_utils.windows import WindowsPath
    if is_swift_path(source) and isinstance(dest, WindowsPath):
        raise ValueError('swift copytree to windows is not supported')

    if is_filesystem_path(dest):
        dest.expand().abspath().parent.makedirs_p()
        if is_swift_path(source):
            source.download(dest, use_manifest=use_manifest,
                            condition=condition, **retry_args)
        else:
            if copy_cmd:
                copy_cmd = shlex.split(copy_cmd)
                copy_cmd.extend([str(source.abspath().expand()),
                                 str(dest.abspath().expand())])
                logger.info('performing copy with command - %s', copy_cmd)
                check_call(copy_cmd)
            else:
                shutil.copytree(source, dest)
    else:
        with source:
            dest.upload(['.'], use_manifest=use_manifest, headers=headers,
                        condition=condition, **retry_args)


def walk_files_and_dirs(files_and_dirs):
    """Walk all files and directories.

    Args:
        files_and_dirs (List[str]): All file or directory names to walk.

    Returns:
        dict: All files and empty directories under files_and_dirs keyed to
            their size. Dictionaries have a size of 0

    Raises:
        ValueError: The provided upload name is not a file or a directory.

    Examples:
        >>> from storage_utils.utils import walk_files_and_dirs
        >>> results = walk_files_and_dirs(['file_name', 'dir_name'])
        >>> print results
        ['file_name', 'dir_name/file1', 'dir_name/file2']
    """
    walked_upload_names_and_sizes = {}
    for name in files_and_dirs:
        if os.path.isfile(name):
            walked_upload_names_and_sizes[name] = os.path.getsize(name)
        elif os.path.isdir(name):
            for (root_dir, dir_names, file_names) in os.walk(name):
                if not (dir_names + file_names):
                    # Ensure that empty directories are uploaded as well
                    walked_upload_names_and_sizes[root_dir] = 0
                else:
                    for file_name in file_names:
                        full_name = os.path.join(root_dir, file_name)
                        walked_upload_names_and_sizes[full_name] = os.path.getsize(full_name)
        else:
            raise ValueError('file "%s" not found' % name)

    return walked_upload_names_and_sizes


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
    from storage_utils import Path

    tempdir = Path(tempfile.mkdtemp(suffix, prefix, dir))
    try:
        if change_dir:
            with tempdir:
                yield tempdir
        else:
            yield tempdir
    finally:
        tempdir.rmtree()


class ClassProperty(property):
    def __get__(self, cls, owner):
        return self.fget.__get__(None, owner)()


class BaseProgressLogger(object):
    """Base class and methods for logging progress.

    Progress loggers that inherit this base class have the ability to
    track information related to the progress of a group of results.

    Users instantiate the logger as a context manager::

        for MyProgressLogger() as l:
            for r in results:
                l.add_result(r)

    When the progress logger is instantiated, the message returned
    from ``get_start_message`` is logged. As results are added, logs
    are printed at each result interval. For example, if the
    ``result_interval`` is 3, progress will be logged for every third result
    added.

    When exiting the context manager, the log message from ``get_finish_message``
    is returned.

    To accumulate more results, implement the ``update_progress`` method. For
    example, to track the amount of bytes tracked so far::

        def update_progress(self, result):
            self.total_bytes += result['bytes']

    Any custom results can be printed when implementing ``get_progress_message``.
    """
    def __init__(self, logger, level=logging.INFO, result_interval=10):
        self.logger = logger
        self.level = level
        self.result_interval = result_interval
        self.num_results = 0
        self.start_time = datetime.datetime.utcnow()

    def __enter__(self):
        start_msg = self.get_start_message()
        if start_msg:  # pragma: no cover
            self.logger.log(self.level, start_msg)
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if exc_type is None:
            finish_msg = self.get_finish_message()
            if finish_msg:
                self.logger.log(self.level, finish_msg)

    def get_elapsed_time(self):
        return datetime.datetime.utcnow() - self.start_time

    def format_time(self, t):
        time_elapsed = datetime.datetime.utcnow() - self.start_time
        hours, remainder = divmod(time_elapsed.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        return '%d:%02d:%02d' % (hours, minutes, seconds)

    def get_start_message(self):
        return self.get_progress_message()

    def get_finish_message(self):
        return self.get_progress_message()

    def get_progress_message(self):
        raise NotImplementedError

    def update_progress(self, result):
        pass

    def add_result(self, result):
        """Adds a result to the progress logger and logs messages.

        Messages are logged every time the ``result_interval`` is met.
        """
        self.num_results += 1
        self.update_progress(result)
        if self.num_results % self.result_interval == 0:
            progress_msg = self.get_progress_message()
            if progress_msg:  # pragma: no cover
                self.logger.log(self.level, progress_msg)
