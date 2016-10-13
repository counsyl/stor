from contextlib import contextmanager
import datetime
import errno
import logging
import os
import shlex
import shutil
from subprocess import check_call
import tempfile
from stor import exceptions

logger = logging.getLogger(__name__)

# Name for the data manifest file when using the use_manifest option
# for upload/download
DATA_MANIFEST_FILE_NAME = '.data_manifest.csv'


def str_to_bytes(s):
    """
    Converts a given string into an integer representing bytes
    where G is gigabytes, M is megabytes, K is kilobytes, and B
    is bytes.
    """
    if type(s) is int:
        return s

    units = {'B': 1, 'K': 1024, 'M': 1024 ** 2, 'G': 1024 ** 3}
    if len(s) < 2:
        raise ValueError('invalid size')
    order = s[-1]
    try:
        return units[order] * int(s[:-1])
    except ValueError:
        raise ValueError('invalid size')
    except KeyError:
        raise ValueError('invalid units')


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
    from stor import Path
    from stor.posix import PosixPath

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
    try:
        os.makedirs(dest)
    except OSError as exc:
        if exc.errno == errno.ENOTDIR:
            raise OSError(errno.ENOTDIR,
                          'a parent directory of \'%s\' already exists as a file' % dest)
        elif exc.errno != errno.EEXIST or os.path.isfile(dest):
            raise


def with_trailing_slash(p):
    """Returns a path with a single trailing slash or None if not a path"""
    if not p:
        return p
    return type(p)(p.rstrip('/') + '/')


def has_trailing_slash(p):
    """Checks if a path has a single trailing slash"""
    if not p:
        return False
    return str(p)[-1] == '/'


def remove_trailing_slash(p):
    """Returns a path with trailing slashes removed or None if not a path"""
    if not p:
        return p
    return type(p)(p.rstrip('/'))


def validate_condition(condition):
    """Verifies condition is a function that takes one argument"""
    if condition is None:
        return
    if not (hasattr(condition, '__call__') and hasattr(condition, '__code__')):
        raise ValueError('condition must be callable')
    if condition.__code__.co_argcount != 1:
        raise ValueError('condition must take exactly one argument')


def check_condition(condition, results):
    """Checks the results against the condition.

    Raises:
        ConditionNotMetError: If the condition returns False
    """
    if condition is None:
        return

    condition_met = condition(results)
    if not condition_met:
        raise exceptions.ConditionNotMetError('condition not met')


def join_conditions(*conditions):
    def wrapper(results):
        return all(f(results) for f in conditions)
    return wrapper


def generate_and_save_data_manifest(manifest_dir, data_manifest_contents):
    """Generates a data manifest for a given directory and saves it.

    Args:
        manifest_dir (str): The directory in which the manifest will be saved
        data_manifest_contents (List[str]): The list of all objects that will
            be part of the manifest.
    """
    import stor
    from stor import Path

    manifest_file_name = Path(manifest_dir) / DATA_MANIFEST_FILE_NAME
    with stor.open(manifest_file_name, 'w') as out_file:
        contents = '\n'.join(data_manifest_contents) + '\n'
        out_file.write(contents)


def get_data_manifest_contents(manifest_dir):
    """Reads the manifest file and returns a set of expected files"""
    import stor

    manifest = manifest_dir / DATA_MANIFEST_FILE_NAME
    with stor.open(manifest, 'r') as manifest_file:
        return [
            f.strip() for f in manifest_file.readlines() if f.strip()
        ]


def validate_manifest_list(expected_objs, list_results):
    """
    Given a list of expected object names and results,
    verify all expected objects are in the listed results
    """
    listed_objs = {r.resource for r in list_results}
    return set(expected_objs).issubset(listed_objs)


def is_swift_path(p):
    """Determines if the path is a Swift path.

    All Swift paths start with swift://

    Args:
        p (str): The path string

    Returns:
        bool: True if p is a Swift path, False otherwise.
    """
    from stor.swift import SwiftPath
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
    from stor.s3 import S3Path
    return p.startswith(S3Path.drive)


def is_obs_path(p):
    """Determines if the path is an OBS path (an S3 or Swift path).

    Args:
        p (str): The path string

    Returns
        bool: True if p is an OBS path, False otherwise.
    """
    return is_s3_path(p) or is_swift_path(p)


def copy(source, dest, swift_retry_options=None):
    """Copies a source file to a destination file.

    Note that this utility can be called from either OBS, posix, or
    windows paths created with ``stor.Path``.

    Args:
        source (path|str): The source directory to copy from
        dest (path|str): The destination file or directory.
        swift_retry_options (dict): Optional retry arguments to use for swift
            upload or download. View the
            `swift module-level documentation <swiftretry>` for more
            information on retry arguments

    Examples:
        Copying a swift file to a local path behaves as follows::

            >>> import stor
            >>> swift_p = 'swift://tenant/container/dir/file.txt'
            >>> # file.txt will be copied to other_dir/other_file.txt
            >>> stor.copy(swift_p, 'other_dir/other_file.txt')

        Copying from a local path to swift behaves as follows::

            >>> from stor import Path
            >>> local_p = Path('my/local/file.txt')
            >>> # File will be uploaded to swift://tenant/container/dir/my_file.txt
            >>> local_p.copy('swift://tenant/container/dir/')

        Because of the ambiguity in whether a remote target is a file or directory, copy()
        will error on ambiguous paths.

            >>> local_p.copy('swift://tenant/container/dir')
            Traceback (most recent call last):
            ...
            ValueError: OBS destination must be file with extension or directory with slash
    """
    from stor import Path
    from stor.obs import OBSUploadObject

    source = Path(source)
    dest = Path(dest)
    swift_retry_options = swift_retry_options or {}
    if is_obs_path(source) and is_obs_path(dest):
        raise ValueError('cannot copy one OBS path to another OBS path')
    if is_obs_path(dest) and dest.is_ambiguous():
        raise ValueError('OBS destination must be file with extension or directory with slash')

    if is_filesystem_path(dest):
        if is_obs_path(source):
            dest_file = dest if not dest.isdir() else dest / source.name
            source.download_object(dest_file, **swift_retry_options)
        else:
            shutil.copy(source, dest)
    else:
        dest_file = dest if not dest.endswith('/') else dest / source.name
        if is_swift_path(dest) and not dest_file.parent.container:
            raise ValueError((
                'cannot copy to tenant "%s" and file '
                '"%s"' % (dest_file.parent, dest_file.name)
            ))
        dest_obj_name = Path(dest_file.parent.resource or '') / dest_file.name
        upload_obj = OBSUploadObject(source, dest_obj_name)
        dest_file.parent.upload([upload_obj],
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

        Path('a').copytree('c')

    The end result for c looks like::

        c/
        - b/
        - - 1.txt

    Note that the user can override which copy command is used for posix
    copies, and it is up to them to ensure that their code abides by the
    semantics of the provided copy command. This function has been tested
    in production using the default command of ``cp -r`` and using ``mcp -r``.

    Using OBS source and destinations work in a similar manner. Assume
    the destination is a swift path and we upload the same ``a`` folder::

        Path('a').copytree('swift://tenant/container/folder')

    The end swift result will have one object::

        Path('swift://tenant/container/folder/b/1.txt')

    Similarly one can do::

        Path('swift://tenant/container/folder/').copytree('c')

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
        ValueError: if two OBS paths are specified
        OSError: if destination is a posix path and it already exists
    """
    from stor import Path

    source = Path(source)
    dest = Path(dest)
    if is_obs_path(source) and is_obs_path(dest):
        raise ValueError('cannot copy one OBS path to another OBS path')
    from stor.windows import WindowsPath
    if is_obs_path(source) and isinstance(dest, WindowsPath):
        raise ValueError('OBS copytree to windows is not supported')

    if is_filesystem_path(dest):
        dest.expand().abspath().parent.makedirs_p()
        if is_obs_path(source):
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


def _safe_get_size(name):
    """Get the size of a file, handling weird edge cases like broken
    symlinks by returning None"""
    try:
        return os.path.getsize(name)
    except OSError as e:
        if e.errno == errno.ENOENT:
            return None
        else:  # pragma: no cover
            raise


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
        >>> from stor.utils import walk_files_and_dirs
        >>> results = walk_files_and_dirs(['file_name', 'dir_name'])
        >>> print results
        ['file_name', 'dir_name/file1', 'dir_name/file2']
    """
    walked_upload_names_and_sizes = {}
    non_existent_files = []
    for name in files_and_dirs:
        if os.path.isfile(name):
            walked_upload_names_and_sizes[name] = _safe_get_size(name)
        elif os.path.isdir(name):
            for (root_dir, dir_names, file_names) in os.walk(name):
                sizes = []
                for file_name in file_names:
                    full_name = os.path.join(root_dir, file_name)
                    sz = _safe_get_size(full_name)
                    if sz is not None:
                        sizes.append(sz)
                        walked_upload_names_and_sizes[full_name] = sz
                    else:
                        non_existent_files.append(full_name)
                if not sizes and not dir_names:
                    # we have an empty directory
                    walked_upload_names_and_sizes[root_dir] = 0
        else:
            raise ValueError('file "%s" not found' % name)

    if non_existent_files:
        file_list = ','.join(non_existent_files[:10])
        if len(file_list) > 50 or len(non_existent_files) > 10:  # pragma: no cover
            file_list = file_list[:50] + '...'
        logger.warn('Skipping %d non existent files in {!r}. Files: %s'.format(
                    ','.join(files_and_dirs)), len(non_existent_files),
                    file_list)

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
        >>> from stor import NamedTemporaryDirectory
        >>> with NamedTemporaryDirectory() as d:
        >>>     # Do operations within "d", which will be deleted afterwards
    """
    from stor import Path

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
