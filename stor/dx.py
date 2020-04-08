import logging
import sys
import tempfile
import urllib.parse
import warnings

from cached_property import cached_property
from contextlib import contextmanager
import dxpy
from dxpy.exceptions import DXError
from dxpy.exceptions import DXSearchError

from stor import exceptions as stor_exceptions
from stor import Path
from stor import settings
from stor import utils
from stor.obs import OBSPath
from stor.obs import OBSUploadObject
from stor.posix import PosixPath
import stor.settings


logger = logging.getLogger(__name__)
progress_logger = logging.getLogger('%s.progress' % __name__)


class DNAnexusError(stor_exceptions.RemoteError):
    """Base class for all remote errors thrown by this DX module"""
    pass


class MultipleObjectsSameNameError(DNAnexusError):
    """Thrown when multiple objects exist with the same name

    Currently, we throw this when trying to get the canonical project
    from virtual path and two or more projects were found with same name
    """
    pass


class ProjectNotFoundError(stor_exceptions.NotFoundError):
    """Thrown when no project exists with the given name

    Currently, we throw this when trying to get the canonical project
    from virtual path and no project was found with same name
    """
    pass


class InconsistentUploadDownloadError(DNAnexusError):
    """Thrown during checksum mismatch or part length mismatch.."""
    pass


def _dx_error_to_descriptive_exception(client_exception):
    """Converts dxpy errors to more descriptive exceptions with
    transaction ID"""
    http_status = getattr(client_exception, 'code', None)
    if isinstance(client_exception, dxpy.DXAPIError):
        exc_str = '{} - {}'.format(client_exception.name or '',
                                   client_exception.msg or client_exception.error_message())
    else:
        exc_str = str(client_exception)
    if http_status in [401, 403]:
        return stor_exceptions.UnauthorizedError(
            'Either use `dx login --token {{your_dx_token}} --save` or set DX_AUTH_TOKEN '
            'environment variable. {}'.format(exc_str), client_exception
        )
    status_to_exception = {
        404: stor_exceptions.NotFoundError,
        409: stor_exceptions.ConflictError
    }
    if http_status in status_to_exception:
        return status_to_exception[http_status](exc_str, client_exception)
    elif 'DXChecksumMismatchError' in exc_str or 'DXPartLengthMismatchError' in exc_str:
        return InconsistentUploadDownloadError(exc_str, client_exception)
    else:
        return DNAnexusError(exc_str, client_exception)


@contextmanager
def _wrap_dx_calls():
    """Updates the dx_auth_token from settings for dxpy
    Bubbles all dxpy exceptions as `DNAnexusError` classes
    """
    auth_token = settings.get()['dx']['auth_token']
    if auth_token:  # pragma: no cover
        dxpy.set_security_context({
            'auth_token_type': 'Bearer',
            'auth_token': auth_token
        })
    try:
        yield
    except DXError as e:
        raise _dx_error_to_descriptive_exception(e) from e


class DXPath(OBSPath):
    """
    Provides the ability to manipulate and access resources on DNAnexus
    servers with stor interfaces.
    """

    def __new__(cls, path):
        """Custom __new__ method so that the validation checks happen during creation

        This ensures invalid dx paths like DXPath('dx://') are never initialized
        """
        return super(DXPath, cls).__new__(Path, path)

    drive = 'dx://'

    def _get_parts(self):
        """Returns the path parts (excluding the drive) as a list of strings.
        Project is always the first part returned here.
        """
        colon_pieces = self[len(self.drive):].split(':', 1)
        project = colon_pieces[0]
        resource = (colon_pieces[1] if len(colon_pieces) == 2 else '').lstrip('/')
        parts = resource.split('/')
        parts.insert(0, project)
        return parts

    def _noop(attr_name):
        def wrapper(self):
            return type(self)(self)
        wrapper.__name__ = attr_name
        wrapper.__doc__ = 'No-op for %r' % attr_name
        return wrapper

    abspath = _noop('abspath')
    realpath = _noop('realpath')
    expanduser = _noop('expanduser')

    def clear_cached_properties(self):
        """Clears all cached properties in DXPath objects.

        The canonical and virtual forms of DXPath objects are cached to not hit
        the server for every transformation call. However, after copy/remove/rename,
        the cached information is outdated and needs to be cleared.
        """
        for prop in ('canonical_project', 'canonical_resource', 'virtual_path', 'virtual_project'):
            self.__dict__.pop(prop, None)

    def virtual_project(self):
        raise NotImplementedError

    def virtual_resource(self):
        raise NotImplementedError

    def virtual_path(self):
        raise NotImplementedError

    def canonical_project(self):
        raise NotImplementedError

    def canonical_resource(self):
        raise NotImplementedError

    def canonical_path(self):
        raise NotImplementedError

    def normpath(self):
        raise NotImplementedError

    def exists(self):
        raise NotImplementedError

    @property
    def project(self):
        """The project name from the path or None"""
        parts = self._get_parts()
        return parts[0] if len(parts) > 0 and parts[0] else None

    def joinpath(self, *others):
        """Wrapper around base joinpath function which converts the first part to normpath
        before joining with others.
        """
        return self.path_class(self.path_module.join(self.normpath(), *others))

    def to_url(self):
        """For compatibility with OBS - returns ``temp_url()``"""
        return self.temp_url()

    def temp_url(self, lifetime=300, filename=None):
        """Obtains a temporary URL to a DNAnexus data-object.

        If ``DX_FILE_PROXY_URL`` or ``[dx] file_proxy_url=`` is set, will use that to construct a
        path instead, e.g.::

            >>> stor.Path('dx://proj:/folder/mypath.csv').temp_url()
            'https://dl.dnanex.us/F/D/awe1323/mypath.csv'
            >>> with stor.settings.use({'dx': {'file_proxy_url':
            ...     'https://my-dnax-proxy.example.com/gateway'}):
            ... stor.Path('dx://proj:/folder/mypath.csv').temp_url()
            'https://my-dnax-proxy.example.com/gateway/proj/folder/mypath.csv'

        The file proxy is assumed to be a service that, when given DX path and project, will proxy
        through to DNAnexus to render content.

        Args:
            lifetime (int): The time (in seconds) the temporary
                URL will be valid (only for temp URL generation)
            filename (str, optional): A urlencoded filename to use for
                attachment, otherwise defaults to object name
                (to bypass, set to empty string).

        Raises:
            ValueError: The path points to a project
            ValueError: ``file_proxy_url`` is set and ``filename`` does not match object name
            ValueError: ``file_proxy_url`` does not look like a valid http(s) path
            NotFoundError: The path could not be resolved to a file (when ``file_proxy_url`` unset)
        """
        if not self.resource:
            raise ValueError('DX Projects cannot have a temporary download url')
        file_proxy_url = stor.settings.get()['dx']['file_proxy_url']
        if file_proxy_url:
            if not file_proxy_url.startswith('http'):
                raise ValueError("if set, ``file_proxy_url`` must be an http(s) path")
            if filename and filename != self.name:
                raise ValueError(
                    'filename MUST match object name when file_proxy_url is set'
                )
            return urllib.parse.urljoin(
                file_proxy_url,
                f'{self.virtual_project}/{self.virtual_resource}'
            )
        with _wrap_dx_calls():
            if filename is None:
                filename = self.virtual_path.name
            elif not filename:
                # e.g., set to empty string
                filename = None
            file_handler = dxpy.DXFile(dxid=self.canonical_resource,
                                       project=self.canonical_project)
            return file_handler.get_download_url(
                duration=lifetime,
                preauthenticated=True,
                filename=filename,
                project=self.canonical_project
            )[0]

    @property
    def resource(self):
        """The virtual or canonical path to the file within the project (as a POSIXPath).

        Examples:

           >>> Path('dx://project:dir/file').resource
           PosixPath('dir/file')
           >>> Path('dx://project-123:file-456').resource
           PosixPath('file-456')

        NOTE: to avoid making API requests, this operation only uses the local string
        """
        parts = self._get_parts()
        joined_resource = '/'.join(parts[1:]) if len(parts) > 1 else None
        return self.parts_class(joined_resource) if joined_resource else None

    def dirname(self):
        """Returns directory name of path. Returns self if path is a project.

        To avoid making API calls, canonical paths will return the project ID
        """
        if not self.resource:
            return self
        else:
            parts = self._get_parts()
            if len(parts) == 2:  # paths like ('dx://proj:file') need different logic
                parts[0] += ':'
                new_path = '/'.join(parts)
                return self.path_class(self.drive + self.path_module.dirname(new_path))
            else:
                return super(DXPath, self).dirname()

    @property
    def name(self):
        """File or folder name of the path. Empty string for projects or folders
        with trailing slash.

        Makes no API calls to server, canonical paths are treated normally, and the basename
        of the path is returned.
        """
        parts = self._get_parts()
        if len(parts) == 2 and parts[1]:  # paths like ('dx://proj:file') need different logic
            parts[0] += ':'
            new_path = '/'.join(parts)
            return self.parts_class(self.path_module.basename(new_path))
        else:
            return super(DXPath, self).name

    def remove(self):
        """Removes a single object from DX platform

        Raises:
            ValueError: The path is invalid.
        """
        if not self.resource:
            raise ValueError('DXPath.remove() can only be called on single object')
        file_handler = dxpy.DXFile(dxid=self.canonical_resource,
                                   project=self.canonical_project)
        with _wrap_dx_calls():
            file_handler.remove()
        self.clear_cached_properties()

    @_wrap_dx_calls()
    def rmtree(self):
        """
        Removes a resource and all of its contents.
        The path should point to a project or directory.

        Raises:
            NotFoundError: The path points to a nonexistent directory
        """
        proj_handler = dxpy.DXProject(self.canonical_project)
        if not self.resource:
            folders = self.listdir(only='folders')
            files = self.listdir(only='objects')
            for folder_p in folders:
                folder_p.rmtree()
            for file_p in files:
                file_p.remove()
            return
        try:
            proj_handler.remove_folder('/' + self.resource, recurse=True)
        except dxpy.exceptions.ResourceNotFound as e:
            raise stor_exceptions.NotFoundError('No folders were found with the given path ({})'
                                                .format(self), e)
        self.clear_cached_properties()

    def makedirs_p(self, mode=0o777):
        """Make directories, including parents on DX from DX folder paths.

        Args:
            mode: unused, present for compatibility
                (access permissions are managed at project level)
        """
        if not self.resource:
            if not self.exists():
                raise ValueError('Cannot create a project via makedirs_p()')
            return
        proj_handler = dxpy.DXProject(self.canonical_project)
        with _wrap_dx_calls():
            proj_handler.new_folder('/' + self.resource, parents=True)

    def isdir(self):
        """Determine if path is directory-like
        (i.e., it's a project, or it's a folder that can be listed)

        Returns:
            bool: True if path is an existing folder path or project
        """
        if not self.resource and self.exists():  # path could be a project
            return True
        # or path could be a folder
        try:
            self.listdir(only='folders')
            return True
        except stor_exceptions.NotFoundError:
            return False

    def isfile(self):
        """Determine an object exists at the specified path

        Returns:
            bool: True if path points to an existing file
        """
        if not self.resource or utils.has_trailing_slash(self):
            return False
        try:
            self.stat()
            return True
        except stor_exceptions.NotFoundError:
            return False

    def _rename(self, new_name):
        """Rename a single data object on the DX Platform

        Args:
            new_name (str): New name of the object

        Raises:
            ValueError: When trying to rename a project
            NotFoundError: When path cannot be resolved to a file.
        """
        if not self.resource:
            raise ValueError('Projects cannot be renamed')
        if new_name == self.name:
            return
        file_handler = dxpy.DXFile(dxid=self.canonical_resource,
                                   project=self.canonical_project)
        with _wrap_dx_calls():
            file_handler.rename(new_name)
        self.clear_cached_properties()

    def _prep_for_copy(self, dest):
        """Handles logic, for finalizing target destination, making parent folders
        and deleting existing target, common to _clone and _move"""
        dest_is_dir = dest.isdir()
        target_dest = dest
        if dest_is_dir or utils.has_trailing_slash(dest):
            target_dest = dest / self.name
        if not dest_is_dir and target_dest.parent.resource:
            target_dest.parent.makedirs_p()
        if target_dest.isfile():
            target_dest.remove()
        should_rename = not dest_is_dir and not utils.has_trailing_slash(dest)
        return target_dest, should_rename

    def _clone(self, dest):
        """Clones the data object into the destination path.
        The original file is retained.

        Args:
            dest (Path): The destination file/folder path in a different project

        Raises:
            ValueError: If attempting to clone a project
            DNAnexusError: If cloning within same project
        """
        if not self.resource:
            raise ValueError('Cannot clone project ({})'.format(self))
        if dest.canonical_project == self.canonical_project:
            raise DNAnexusError('Cannot clone within same project')
        file_handler = dxpy.DXFile(dxid=self.canonical_resource,
                                   project=self.canonical_project)
        target_dest, should_rename = self._prep_for_copy(dest)

        with _wrap_dx_calls():
            new_file_h = file_handler.clone(project=dest.canonical_project,
                                            folder='/' + (target_dest.parent.resource or ''))
            # no need to rename if we changed destination to include original name
            if should_rename:
                new_file_h.rename(dest.name)

    def _move(self, dest):
        """Moves the data object to a different folder within project.

        Args:
            dest (Path): The destination file/folder path within same project

        Raises:
            ValueError: When attempting to move projects
            DNAnexusError: If attempting to move across projects
        """
        if not self.resource:
            raise ValueError('Cannot move project ({})'.format(self))
        if dest.canonical_project != self.canonical_project:
            # This can be implemented by clone and remove original
            raise DNAnexusError('Cannot move across different projects')
        if self == dest:
            return

        file_handler = dxpy.DXFile(dxid=self.canonical_resource,
                                   project=self.canonical_project)
        target_dest, should_rename = self._prep_for_copy(dest)

        with _wrap_dx_calls():
            file_handler.move('/' + (target_dest.parent.resource or ''))
            if should_rename:
                file_handler.rename(dest.name)
        self.clear_cached_properties()

    def copy(self, dest, raise_if_same_project=False, **kwargs):
        """Copies data object to destination path.

        If dest already exists as a directory on the DX platform, the file is copied
        underneath dest directory with original name.

        If the target destination already exists as a file, it is first deleted before
        the copy is attempted.

        For example, assume the following file hierarchy::

            dxProject/
            - a/
            - - 1.txt

            anotherDxProject/

        Doing a copy of ``1.txt`` to a new destination of ``b.txt`` is
        performed with::

            Path('dx://dxProject:/a/1.txt').copy('dx://anotherDxProject/b.txt')

        The end result for anotherDxProject looks like::

            anotherDxProject/
            - b.txt

        And, if the destination already exists as a directory, i.e. we have::

            dxProject/
            - a/
            - - 1.txt

            anotherDxProject/
            - b.txt/

        Performing copy with following command::

            Path('dx://dxProject:/a/1.txt').copy('dx://anotherDxProject/b.txt')

        Will yield the resulting structure to be::

            anotherDxProject/
            - b.txt/
            - - 1.txt

        If the source file and destination belong to the same project, the files are
        moved instead of copied, if the raise_if_same_project flag is False; because
        the same underlying file cannot appear in two locations in the same project.

        If the final destination for the file already is an existing file,
        that file is deleted before the file is copied.

        Args:
            dest (Path|str): The destination file or directory.
            raise_if_same_project (bool, default False): Controls moving file within project
                instead of cloning. If True, raises an error to prevent this move.
                Only takes effect when both source and destination are
                within the same DX Project

        Raises:
            DNAnexusError: When copying within same project with raise_if_same_project=False
            NotFoundError: When the source file path doesn't exist
        """
        dest = Path(dest)
        if utils.is_dx_path(dest):
            if self.isfile():
                if dest.canonical_project == self.canonical_project:
                    if not raise_if_same_project:
                        self._move(dest)
                    else:
                        raise DNAnexusError('Source and destination are in same project. '
                                            'Set raise_if_same_project=False to allow this.')
                else:
                    self._clone(dest)
            else:
                raise stor_exceptions.NotFoundError(
                    'No data object was found for the given path on DNAnexus')
        else:
            super(DXPath, self).copy(dest)  # for other filesystems, delegate to utils.copy

    def copytree(self, dest, raise_if_same_project=False, **kwargs):
        """Copies a source directory to a destination directory.
        This is not an atomic operation.

        If the destination path already exists as a directory, the source tree
        including the root folder is copied over as a subfolder of the destination.

        If the source and destination directories belong to the same project,
        the tree is moved instead of copied. Also, in such cases, the root folder
        of the project cannot be the source path. Please listdir the root folder
        and copy/copytree individual items if needed.

        For example, assume the following file hierarchy::

            project1/
            - b/
            - - 1.txt

            project2/

        Doing a copytree from ``project1:/b/`` to a new dx destination of
        ``project2:/c`` is performed with::

            Path('dx://project1:/b').copytree('dx://project2:/c')

        The end result for project2 looks like::

            project2/
            - c/
            - - 1.txt

        If the destination path directory already exists, the folder is copied
        as a subfolder of the destination. If this new destination also exists,
        a TargetExistsError is raised.

        If the source is a root folder, and is cloned to an existing destination directory
        or if the destination is also a root folder, the tree is moved under project name.

        Refer to ``dx`` docs for detailed information.

        Args:
            dest (Path|str): The directory to copy to. Must not exist if
                its a posix directory
            raise_if_same_project (bool, default False): Allows moving files within project
                instead of cloning. If True, raises an error to prevent moving the directory.
                Only takes effect when both source and destination directory are
                within the same DX Project

        Raises:
            DNAnexusError: Attempt to clone within same project and raise_if_same_project=True
            TargetExistsError: All possible destinations for source directory already exist
            NotFoundError: source directory path doesn't exist
        """
        dest = Path(dest)
        if utils.is_dx_path(dest):
            if self.isdir():
                if dest.canonical_project == self.canonical_project:
                    if not raise_if_same_project:
                        self._movetree(dest)
                    else:
                        raise DNAnexusError('Source and destination are in same project. '
                                            'Set raise_if_same_project=False to allow this.')
                else:
                    self._clonetree(dest)
            else:
                raise stor_exceptions.NotFoundError(
                    'No project or directory was found at path ({})'.format(self))
        else:
            super(DXPath, self).copytree(dest)  # for other filesystems, delegate to utils.copytree

    def _prep_for_copytree(self, dest):
        """Handles logic, for finalizing target destination, making parent folders
        and checking for clashes, common to _clonetree and _movetree"""
        source = utils.remove_trailing_slash(self)
        dest_is_dir = dest.isdir()
        should_rename = True
        target_dest = dest

        if dest_is_dir or utils.has_trailing_slash(dest):
            target_dest = dest / (source.name if source.resource else source.virtual_project)
            if target_dest.isdir():
                raise stor_exceptions.TargetExistsError(
                    'Destination path ({}) already exists, will not cause '
                    'duplicate folders to exist. Remove the original first'
                    .format(target_dest)
                )
            should_rename = False

        if not source.resource:
            target_dest.makedirs_p()
        elif not dest_is_dir and target_dest.parent.resource:  # don't call makedirs_p on project
            target_dest.parent.makedirs_p()

        moved_folder_path = target_dest.parent / source.name
        return target_dest, should_rename, moved_folder_path

    def _clonetree(self, dest):
        """Clones the project or directory into the destination path.
        The original tree is retained.

        If the destination path already exists as a directory, the source tree
        including the root folder is copied over as a subfolder of the destination.

        If the source root folder is cloned to an existing destination directory
        or to root folder of destination, the tree is moved under project name.

        Args:
            dest (Path): The destination directory path in a different project

        Raises:
            TargetExistsError: When all possible destinations for source directory already exist
            DNAnexusError: When cloning within same project
        """
        if dest.canonical_project == self.canonical_project:
            raise DNAnexusError('Cannot clonetree within same project')
        if dest == (self.drive+dest.project):  # need to convert dx://proj to dx://proj:
            dest = dest + ':'
        target_dest, should_rename, moved_folder_path = self._prep_for_copytree(dest)

        project_handler = dxpy.DXProject(self.canonical_project)
        with _wrap_dx_calls():
            project_handler.clone(
                container=dest.canonical_project,
                destination=('/' + (target_dest.parent.resource or '')
                             ) if self.resource else '/' + target_dest.resource,
                folders=['/' + (self.resource or '')]
            )
            if self.resource and should_rename:
                dxpy.api.project_rename_folder(
                    dest.canonical_project,
                    input_params={
                        'folder': '/' + moved_folder_path.resource,
                        'name': target_dest.name
                    }
                )

    def _movetree(self, dest):
        """Moves the project or directory to a different folder within project.

        Like copytree, if the destination exists as a folder already, the source dir is
        moved inside that dest folder with its original name.

        The source cannot be the root directory.

        Refer to copytree or copytree for detailed information.

        Args:
            dest (Path): The destination directory path within same project

        Raises:
            TargetExistsError: When destination directory already exists
            DNAnexusError: When attempting to move across projects
        """
        if dest.canonical_project != self.canonical_project:
            raise DNAnexusError('Cannot movetree across different projects')
        if not self.resource:
            raise DNAnexusError('Cannot move root folder within same project on DX')
        if self == dest:
            return
        if dest == (self.drive+dest.project):  # need to convert dx://proj to dx://proj:
            dest = dest + ':'
        target_dest, should_rename, moved_folder_path = self._prep_for_copytree(dest)

        project_handler = dxpy.DXProject(self.canonical_project)
        with _wrap_dx_calls():
            project_handler.move_folder(
                folder='/' + self.resource,
                destination='/' + (target_dest.parent.resource or '')
            )
            if should_rename:
                dxpy.api.project_rename_folder(
                    dest.canonical_project,
                    input_params={
                        'folder': '/' + moved_folder_path.resource,
                        'name': target_dest.name
                    }
                )
        self.clear_cached_properties()

    @_wrap_dx_calls()
    def download_object(self, dest, **kwargs):
        """Download a single path or object to file.

        Args:
            dest (Path): The output file

        Raises:
            NotFoundError: When source path is not an existing file
        """
        dxpy.download_dxfile(
            dxid=self.canonical_resource,
            filename=dest,
            project=self.canonical_project
        )

    def download_objects(self,
                         dest,
                         objects):
        def is_parent_dir(poss_parent, poss_child):
            """Checks if poss_child is a sub-path of poss_parent"""
            if not poss_parent.resource:
                return poss_child.resource and poss_child.project == poss_parent.project
            return poss_child.startswith(utils.with_trailing_slash(poss_parent))

        source = self
        if source == (self.drive + self.project):  # need to convert dx://proj to dx://proj:
            source = DXPath(self + ':')

        for obj in objects:
            if utils.is_dx_path(obj) and not is_parent_dir(source, DXPath(obj)):
                raise ValueError(
                    '"%s" must be child of download path "%s"' % (obj, self))
        # Convert requested download objects to full object paths
        objs_to_download = {
            obj: DXPath(obj) if utils.is_dx_path(obj) else source / obj
            for obj in objects
        }
        results = {}
        for obj, dx_obj in objs_to_download.items():
            dest_resource = dx_obj[len(source):].lstrip('/')
            dest_obj = PosixPath(dest) / dest_resource
            dx_obj.copy(dest_obj)
            results[obj] = dest_obj
        return results

    @_wrap_dx_calls()
    def download(self, dest, **kwargs):
        """Download a directory.

        Args:
            dest (Path): The output directory

        Raises:
            NotFoundError: When source or dest path is not a directory
        """
        dxpy.download_folder(
            project=self.canonical_project,
            destdir=dest,
            folder='/' + (self.resource or '')
        )

    def upload(self, to_upload, **kwargs):
        """Upload a list of files and directories to a directory.

        This is not a batch level operation.
        If some file errors, the files uploaded before will remain present.

        Args:
            to_upload (List[Union[str, OBSUploadObject]]): A list of posix file names,
                directory names, or OBSUploadObject objects to upload.

        Raises:
            ValueError: When source path is not a directory
            TargetExistsError: When destination directory already exists
        """
        dx_upload_objects = [
            name for name in to_upload
            if isinstance(name, OBSUploadObject)
        ]
        all_files_to_upload = utils.walk_files_and_dirs([
            name for name in to_upload
            if not isinstance(name, OBSUploadObject)
        ])
        dx_upload_objects.extend([
            OBSUploadObject(f,
                            object_name=('/' + self.resource if self.resource
                                         else Path('')) /
                            utils.file_name_to_object_name(f))
            for f in all_files_to_upload
        ])

        for upload_obj in dx_upload_objects:
            upload_obj.object_name = Path(upload_obj.object_name)
            upload_obj.source = Path(upload_obj.source)
            dest_file = Path('{drive}{project}:{path}'.format(
                drive=self.drive, project=self.canonical_project,
                path=upload_obj.object_name))

            if upload_obj.source.isfile():
                dest_is_file = dest_file.isfile()
                if dest_is_file:  # only occurs if upload is called directly with existing objects
                    logger.warning(
                        'Destination path ({}) already exists, will not cause '
                        'duplicate file objects on the platform. Skipping...'
                        .format(dest_file))
                else:
                    with _wrap_dx_calls():
                        dxpy.upload_local_file(
                            filename=upload_obj.source,
                            project=self.canonical_project,
                            folder='/' + (dest_file.parent.resource or ''),
                            parents=True,
                            name=dest_file.name
                        )
            elif upload_obj.source.isdir():
                dest_file.makedirs_p()
            else:
                raise stor_exceptions.NotFoundError(
                    'Source path ({}) does not exist. Please provide a valid source'
                    .format(upload_obj.source))

    def read_object(self):
        """Reads an individual object from DX.
        Note dxpy for Py3 automatically decodes the DXFile.read using utf-8.

        Returns:
            bytes: the raw bytes from the object on DX.
        """
        if not self.resource:
            raise ValueError('Can only read_object() on a file path, not a project')
        file_handler = dxpy.DXFile(dxid=self.canonical_resource,
                                   project=self.canonical_project)
        with _wrap_dx_calls():
            result = file_handler.read()
        # TODO (akumar): allow other encoding after update of encoding in dxpy for Py3
        result = result.encode('utf-8')  # dxpy for py3 already decodes the data with 'utf-8'
        return result

    def write_object(self, content, **kwargs):
        """Writes an individual object to DX.

        Note that this method writes the provided content to a temporary
        file before uploading. This allows us to reuse code from DXPath's
        uploader (multi part object support, etc.).

        Args:
            content (bytes): raw bytes to write to OBS
            **kwargs: Keyword arguments to pass to
                `DXPath.upload`
        """
        if not self.resource:
            raise ValueError('Cannot write to project. Please provide a file path')
        if not isinstance(content, bytes):  # pragma: no cover
            # bytes/unicode a little confused so allow it
            warnings.warn('A future version of stor will raise a'
                          ' TypeError if content is not bytes')
        mode = 'wb' if type(content) == bytes else 'wt'
        if self.isfile():
            self.remove()
        with tempfile.NamedTemporaryFile(mode=mode) as fp:
            fp.write(content)
            fp.flush()
            suo = OBSUploadObject(fp.name, object_name='/' + self.resource)
            return self.upload([suo], **kwargs)

    def open(self, mode='r', encoding=None):
        """
        Opens a OBSFile that can be read or written to and is uploaded to
        the remote service.

        For examples of reading and writing opened objects, view
        OBSFile.

        Args:
            mode (str): The mode of object IO. Currently supports reading
                ("r" or "rb") and writing ("w", "wb")
            encoding (str): text encoding to use. Defaults to
                ``locale.getpreferredencoding(False)``

        Returns:
            OBSFile: The file object for Swift/S3/DX.

        Raises:
            ValueError: if attempting to write to project
            DNAnexusError: A dxpy client error occured.
        """
        if encoding and encoding not in ('utf-8', 'utf8'):
            raise ValueError('For DNAnexus paths, encoding is always assumed to be '
                             'utf-8. Please switch your encoding')
        if not self.resource:
            raise ValueError("Can only read or write on file paths not project paths")
        return super().open(mode=mode, encoding=encoding)

    def list(self,
             canonicalize=False,
             starts_with=None,
             limit=None,
             classname=None,
             condition=None
             ):
        """List contents using the resource of the path as a prefix. This will only
        list the file resources (and not empty directories like other OBS).

        .. warning::
            Prefer `list_iter()` to this method in production code. If there are many files (i.e.,
            more than 1-2K) to list, this method may take a long time to return and use a lot of
            memory to construct all of the objects.

        Examples:
            >>> Path('dx://MyProject:/my/path/').list(canonicalize=False)
            [Path('dx://MyProject:/my/path/to/file.txt, ...]
            >>> Path('dx://MyProject:/my/path/').list(canonicalize=True)
            [Path('dx://project-123:file-123'), ...]

        Args:
            canonicalize (bool, default False): if True, return canonical paths
            starts_with (str): Allows for an additional search path to
                be appended to the resource of the dx path. Note that this
                resource path is treated as a directory
            limit (int): Limit the amount of results returned
            classname (str): Restricting class : One of 'record', 'file', 'gtable,
                'applet', 'workflow'
            condition (function(results) -> bool): The method will only return
                when the results matches the condition.

        Returns:
             List[DXPath]: Iterates over listed files that match an optional pattern.
        """
        results = list(self.walkfiles(
            canonicalize=canonicalize,
            starts_with=starts_with,
            limit=limit,
            classname=classname
        ))
        if not results or not results[0]:  # when results == [[]]
            results = []
        utils.validate_condition(condition)
        utils.check_condition(condition, results)
        return results

    def list_iter(self,
                  canonicalize=False,
                  starts_with=None,
                  limit=None,
                  classname=None
                  ):
        """Iterable that yields objects under prefix (especially useful when a folder may have
        many small files)

        Note that this is a wrapper function to walkfiles.

        Args:
            canonicalize (bool, default False): if True, return canonical paths
            starts_with (str): Allows for an additional search path to
                be appended to the resource of the dx path. Note that this
                resource path is treated as a directory
            limit (int): Limit the amount of results returned
            classname (str): Restricting class : One of 'record', 'file', 'gtable,
                'applet', 'workflow'

        Returns:
             Iterable[DXPath]: Iterates over listed files that match an optional pattern.
        """
        return self.walkfiles(
            canonicalize=canonicalize,
            starts_with=starts_with,
            limit=limit,
            classname=classname
        )

    def listdir(self, only='all', canonicalize=False):
        """List the path as a dir, returning top-level directories and files.

        Args:
            canonicalize (bool, default False): if True, return canonical paths
            only (str): "objects" for only objects, "folders" for only folders,
                    "all" for both

        Returns:
            List[DXPath]: Iterates over listed files directly within the resource

        Raises:
            NotFoundError: When resource folder is not present on DX platform
        """
        proj_id = self.canonical_project
        proj_name = self.virtual_project
        ans_list = []
        kwargs = {
            'only': only,
            'describe': {'fields': {'name': True, 'folder': True}},
            'folder': '/' + (self.resource or '')
        }
        with _wrap_dx_calls():
            obj_dict = dxpy.DXProject(dxid=proj_id).list_folder(**kwargs)
        for key, values in obj_dict.items():
            for entry in values:
                if canonicalize:
                    ans_list.append(DXCanonicalPath('dx://{}:/{}'.format(
                        proj_id, (entry.lstrip('/') if key == 'folders' else entry['id']))))
                else:
                    if key == 'folders':
                        ans_list.append(DXVirtualPath('{drive}{proj_name}:{folder}'.format(
                            drive=self.drive, proj_name=proj_name, folder=entry)))
                    else:
                        ans_list.append(DXVirtualPath('{drive}{proj_name}:{folder}/{name}'.format(
                            drive=self.drive,
                            proj_name=proj_name,
                            folder=entry['describe']['folder'].rstrip('/'),
                            name=entry['describe']['name']))
                        )
        return ans_list

    def listdir_iter(self, canonicalize=False):
        """Iterate the path as a dir, returning top-level directories and files.

        Args:
            canonicalize (bool, default False): if True, return canonical paths

        Returns:
            Iterable[DXPath]: Iterates over listed files directly within the resource
        """
        folders = self.listdir(only='folders', canonicalize=canonicalize)
        for folder in folders:
            yield folder
        for data in self.walkfiles(canonicalize=canonicalize, recurse=False):
            yield data

    def walkfiles(self,
                  pattern=None,
                  canonicalize=False,
                  recurse=True,
                  starts_with=None,
                  limit=None,
                  classname=None):
        """Iterates over listed files that match an optional pattern.

        Args:
            pattern (str): glob pattern to match the filenames against.
            canonicalize (bool, default False): if True, return canonical paths
            recurse (bool, default True): if True, look in subfolders of folder as well
            starts_with (str): Allows for an additional search path to
                be appended to the resource of the dx path. Note that this
                resource path is treated as a directory
            limit (int): Limit the amount of results returned
            classname (str): Restricting class : One of 'record', 'file', 'gtable,
                'applet', 'workflow'

        Returns:
             Iter[DXPath]: Iterates over listed files that match an optional pattern.
        """
        proj_id = self.canonical_project
        proj_name = self.virtual_project
        kwargs = {
            'project': proj_id,
            'name': pattern,
            'name_mode': 'glob',
            # the query performance is similar w/wo describe field,
            # hence no need to customize query based on canonicalize flag
            'describe': {'fields': {'name': True, 'folder': True}},
            'recurse': recurse,
            'classname': classname,
            'limit': limit,
            'folder': ('/' + (self.resource or '')) + (starts_with or '')
        }
        with _wrap_dx_calls():
            list_gen = dxpy.find_data_objects(**kwargs)
        for obj in list_gen:
            if canonicalize:
                yield DXCanonicalPath('dx://{}:/{}'.format(obj['project'], obj['id']))
            else:
                yield DXVirtualPath('{drive}{proj_name}:{folder}/{name}'.format(
                    drive=self.drive,
                    proj_name=proj_name,
                    folder=obj['describe']['folder'].rstrip('/'),
                    name=obj['describe']['name'])
                )

    def glob(self, pattern, condition=None, canonicalize=False):
        """ Glob for pattern relative to this directory."""

        results = list(self.walkfiles(
            canonicalize=canonicalize,
            pattern=pattern
        ))
        if not results or not results[0]:  # when results == [[]]
            results = []
        utils.validate_condition(condition)
        utils.check_condition(condition, results)
        return results

    def getsize(self):
        if not self.resource:
            return self.stat()['dataUsage']*1e9
        else:
            return self.stat()['size']

    @_wrap_dx_calls()
    def stat(self):
        """Performs a stat on the path. This method follows (slightly vague) behavior of dxpy's
        describe method. It works as expected for a virtual path. However, for a canonical path:

            Path('dx://project-123:/file-123')

        say project-123 exists and file-123 exists, but file-123 doesn't exist inside project-123,
        stat will still return the describe response on file-123 (with its default project).

        Use stor.exists to check if a canonical path actually exists.

        Raises:
            MultipleObjectsSameNameError: If project or resource is not unique
            NotFoundError: When the project or resource cannot be found
            ValueError: If path is folder path
        """
        if not self.resource:
            return dxpy.DXProject(dxid=self.canonical_project).describe()
        return dxpy.DXFile(dxid=self.canonical_resource,
                           project=self.canonical_project).describe()

    @property
    def content_type(self):
        """Get content type for DXObject. Returns empty string if not present or is project/"""
        return self.stat().get('media') or ''


class DXVirtualPath(DXPath):
    """Class Handler for DXPath of form 'dx://MyProject:/a/b/c' or 'dx://project-{uuid}:/b/c'"""

    @cached_property
    def virtual_project(self):
        """Returns the virtual name of the project associated with the DXVirtualPath"""
        if utils.is_valid_dxid(self.project, 'project'):
            with _wrap_dx_calls():
                return dxpy.DXProject(dxid=self.project).name
        return self.project

    @property
    def virtual_resource(self):
        """Human-readable path to the object in its DNAnexus Project (as PosixPath)"""
        return self.resource

    @property
    def virtual_path(self):
        """Path as DXVirtualPath"""
        return self

    @cached_property
    def canonical_project(self):
        """The dxid of the unique project for the given project name. Only resolves
        project user has access to.

        Raises:
            MultipleObjectsSameNameError: If project name is not unique on DX platform
            NotFoundError: If project name doesn't exist on DNAnexus
        """
        if utils.is_valid_dxid(self.project, 'project'):
            return self.project

        with _wrap_dx_calls():
            try:
                proj_dict = dxpy.find_one_project(
                    name=self.project, level='VIEW', zero_ok=True, more_ok=False)
            except DXSearchError as e:
                raise MultipleObjectsSameNameError('Found more than one project for given name: '
                                                   '{!r}'.format(self.project), e)

        if proj_dict is None:
            raise ProjectNotFoundError('Found no projects for name: {!r}'
                                       .format(self.project))

        return proj_dict['id']

    @cached_property
    def canonical_resource(self):
        """The dxid of the file at this path

        Raises:
            MultipleObjectsSameNameError: if filename is not unique
            NotFoundError: if resource is not found on DX platform
            ValueError: if path looks like a folder path (i.e., ends with trailing slash)
        """
        if not self.resource:
            return None
        if utils.has_trailing_slash(self):
            raise ValueError('Invalid operation ({method}) on folder path ({path})'
                             .format(path=self, method=sys._getframe(2).f_code.co_name))
        objects = [{
            'name': self.name,
            'folder': ('/' + self.resource).parent,
            'project': self.canonical_project,
            'batchsize': 2
        }]
        with _wrap_dx_calls():
            results = dxpy.resolve_data_objects(objects=objects)[0]
        if len(results) > 1:
            raise MultipleObjectsSameNameError('Multiple objects found at path ({}). '
                                               'Try using a canonical ID instead'.format(self))
        elif len(results) == 1:
            return results[0]['id']
        else:
            raise stor_exceptions.NotFoundError(
                'No data object was found for the given path ({}) on DNAnexus'.format(self))

    @property
    def canonical_path(self):
        """The unique file or project that matches the given path"""
        return DXCanonicalPath('{drive}{proj_id}:/{resource}'.format(
            drive=self.drive, proj_id=self.canonical_project,
            resource=(self.canonical_resource or '')))

    def normpath(self):
        normed_resource = self.path_module.normpath('/' + (self.resource or ''))[1:]
        norm_pth = self.path_class(self.drive + self.project + ':/' + normed_resource)
        if isinstance(norm_pth, DXCanonicalPath):
            return norm_pth.normpath()
        return norm_pth

    def splitpath(self):
        """Wrapper around base splitpath function which calls splitpath on the normpath of self
        """
        path_to_split = self.normpath()
        parent, child = self.path_module.split(path_to_split)
        return self.path_class(parent), child

    def exists(self):
        """Checks existence of the path.

        Returns:
            bool: True if the path exists, False otherwise.
        """
        try:
            # first see if there is a specific corresponding object
            self.stat()
            return True
        except (stor_exceptions.NotFoundError, ValueError):
            pass
        # otherwise we could be a directory, so try to listdir folder
        # note: list doesn't error on non-existent folder and cannot be used here
        try:
            self.listdir(only='folders')
            return True
        except stor_exceptions.NotFoundError:
            return False


class DXCanonicalPath(DXPath):
    """Represents fully canonicalized DNAnexus paths:
    'dx://project-{dxID}:/file-{dxID}' or 'dx://project-{dxID}:'
    """

    @property
    def virtual_project(self):
        """The virtual (human-readable) name of the project associated with this path"""
        return self.virtual_path.project

    @property
    def virtual_resource(self):
        """The virtual (human-readable) path of the resource associated with this path"""
        return self.virtual_path.resource

    @cached_property
    @_wrap_dx_calls()
    def virtual_path(self):
        """The DXVirtualPath instance equivalent to the canonical path within the specified project
        """
        proj = dxpy.DXProject(dxid=self.project)
        virtual_p = DXVirtualPath(self.drive + proj.name + ':/')
        if self.resource:
            file_h = dxpy.DXFile(dxid=self.canonical_resource,
                                 project=self.canonical_project)
            virtual_p = virtual_p / file_h.folder[1:] / file_h.name
        return virtual_p

    @property
    def canonical_project(self):
        """The canonical dxid for the project"""
        return self.project

    @property
    def canonical_resource(self):
        """The canonical dxID of the file resource"""
        return self.resource

    @property
    def canonical_path(self):
        """Get DXCanonicalPath instance for path"""
        return self

    def normpath(self):
        return self.path_class(self.drive + self.project + ':' + (self.resource or ''))

    def splitpath(self):
        """Wrapper around base splitpath function which calls splitpath on the normpath of self
        """
        if self.resource:
            path_to_split = self.path_class(self.drive + self.project + ':/' + self.resource)
        else:
            path_to_split = self.path_class(self.drive + self.project + ':/')

        parent, child = self.path_module.split(path_to_split)
        return self.path_class(parent), child

    def exists(self):
        """Checks existence of the path.

        Returns:
            bool: True if the path exists, False otherwise.
        """
        if self.resource:
            # check that file exists AND is in the specified project
            # (list_projects() returns {} when file id doesn't exist)
            return self.canonical_project in dxpy.DXFile(self.canonical_resource).list_projects()
        else:
            try:
                self.stat()
                return True
            except stor_exceptions.NotFoundError:
                return False
