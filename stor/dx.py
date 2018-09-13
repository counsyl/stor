from cached_property import cached_property
import sys

import dxpy
from dxpy.exceptions import DXSearchError
from dxpy.exceptions import DXError

from stor import exceptions as stor_exceptions
from stor import Path
from stor import utils
from stor.obs import OBSPath
from stor.obs import OBSUploadObject


DNAnexusError = stor_exceptions.RemoteError
NotFoundError = stor_exceptions.NotFoundError


# def _parse_dx_error(exc, **kwargs):
#     """
#     Parses DXError exceptions to throw a more informative exception.
#     """
#     msg = exc.message
#     exc_type = type(exc)
#
#     if exc_type is dx_exceptions.DXSearchError:
#         if msg and 'found more' in msg.lower():
#             return DuplicateError(msg, exc)
#         elif msg and 'found none' in msg.lower():
#             return NotFoundError(msg, exc)


class DuplicateError(DNAnexusError):
    """Thrown when multiple objects exist with the same name

    Currently, we throw this when trying to get the canonical project
    from virtual path and two or more projects were found with same name
    """
    pass


class DuplicateProjectError(DuplicateError):
    """Thrown when multiple projects exist with the same name

    Currently, we throw this when trying to get the canonical project
    from virtual path and two or more projects were found with same name
    """
    pass


class ProjectNotFoundError(NotFoundError):
    """Thrown when no project exists with the given name

    Currently, we throw this when trying to get the canonical project
    from virtual path and no project was found with same name
    """
    pass


class DXPath(OBSPath):
    """
        Provides the ability to manipulate and access resources on DNAnexus
        with a similar interface to the path library.
        """

    def __new__(cls, path):
        """Custom __new__ method so that the validation checks happen during creation

        This ensures invalid dx paths like DXPath('dx://) are never initialized
        """
        return super(DXPath, cls).__new__(Path, path)

    drive = 'dx://'

    def _get_parts(self):
        """Returns the path parts (excluding the drive) as a list of strings."""
        parts = super(DXPath, self)._get_parts()
        if len(parts) > 0 and parts[0]:
            # first part can be 'proj:file' or 'proj:' or 'proj'
            parts_first = parts[0].split(':')
            parts[0] = parts_first[0]
            if len(parts_first) > 1 and parts_first[1]:
                parts.insert(1, parts_first[1])
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

    @property
    def project(self):
        """Returns the project name from the path or None"""
        parts = self._get_parts()
        return parts[0] if len(parts) > 0 and parts[0] else None

    def temp_url(self, lifetime=300, filename=None):
        """Obtains a temporary URL to a DNAnexus data-object.

        Args:
            lifetime (int): The time (in seconds) the temporary
                URL will be valid
            filename (str, optional): A urlencoded filename to use for
                attachment, otherwise defaults to object name
        """
        if not self.resource:
            raise ValueError('DX Projects cannot have a temporary download url')
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
        parts = self._get_parts()
        joined_resource = '/'.join(parts[1:]) if len(parts) > 1 else None
        return self.parts_class('/'+joined_resource) if joined_resource else None

    def dirname(self):
        """Returns directory name of path. Returns self if path is a project"""
        if not self.resource:
            return self
        else:
            return super(DXPath, self).dirname()

    def download_objects(self):  # may not need it
        raise NotImplementedError

    def remove(self):
        """Removes a single object from DX platform

        Raises:
            ValueError: The path is invalid.
        """
        if not self.resource:
            raise ValueError('DXPath must point to single data object to call remove')
        file_handler = dxpy.DXFile(dxid=self.canonical_resource,
                                   project=self.canonical_project)
        file_handler.remove()

    def rmtree(self):
        """
        Removes a resource and all of its contents.
        The path should point to a project or directory.

        Raises:
            NotFoundError: The path points to a nonexistent directory
        """
        proj_handler = dxpy.DXProject(self.canonical_project)
        if not self.resource:
            return proj_handler.destroy()
        try:
            proj_handler.remove_folder(self.resource, recurse=True)
        except dxpy.exceptions.ResourceNotFound as e:
            raise NotFoundError('No folders were found with the given path ({})'
                                .format(self), e)

    def makedirs_p(self):
        """Make directories, including parents on DX from DX folder paths.

        The resulting folders will have same access permissions as the project.
        """
        if not self.resource:
            if not self.exists():
                raise ValueError('Cannot create a project via makedirs_p()')
            return
        proj_handler = dxpy.DXProject(self.canonical_project)
        proj_handler.new_folder(self.resource, parents=True)

    def isdir(self):
        """Determine if path is an existing directory

        Returns:
            bool: True if path is an existing folder path or project
        """
        if not self.resource and self.exists():  # path could be a project
            return True
        # or path could be a folder
        try:
            self.listdir()
            return True
        except NotFoundError:
            return False

    def isfile(self):
        """Determine if path is an existing file

        Returns:
            bool: True if path points to an existing file
        """
        if not self.resource or utils.has_trailing_slash(self):
            return False
        try:
            self.stat()
            return True
        except NotFoundError:
            return False

    def rename(self, new_name):
        """Rename a data object on the DX platform

        Args:
            new_name (str): New name of the object

        Raises:
            ValueError: When trying to rename a project
        """
        if not self.resource:
            raise ValueError('Cannot rename project ({})'.format(self))
        if new_name == self.name:
            return
        file_handler = dxpy.DXFile(dxid=self.canonical_resource,
                                   project=self.canonical_project)
        file_handler.rename(new_name)

    def _clone(self, dest):
        """Clones the data object into the destination path.
        The original file is retained.

        Args:
            dest (path): The destination file/folder path in a different project

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
        dest_exists = dest.isdir()
        if dest_exists:
            new_dest = dest / self.name
            if new_dest.isfile():
                raise DuplicateError('Dest path already exists. Do not duplicate!')
            folder_dest = dest
        else:
            if dest.isfile():
                raise DuplicateError('Dest path ({}) already exists. Do not duplicate!'
                                     .format(dest))
            folder_dest = dest.parent
            folder_dest.makedirs_p()

        new_file_h = file_handler.clone(project=dest.canonical_project,
                                        folder=folder_dest.resource or '/')
        if not dest_exists and not utils.has_trailing_slash(dest):
            new_file_h.rename(dest.name)

    def _move(self, dest):
        """Moves the data object to a different folder within project

        Args:
            dest (path): The destination file/folder path within same project

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
        dest_exists = dest.isdir()
        if dest_exists:
            new_dest = dest / self.name
            if new_dest.isfile():
                raise DuplicateError('Dest path ({}) already exists. Do not duplicate!'
                                     .format(new_dest))
            folder_dest = dest
        else:
            if dest.isfile():
                raise DuplicateError('Dest path ({}) already exists. Do not duplicate!'
                                     .format(dest))
            folder_dest = dest.parent
            folder_dest.makedirs_p()

        file_handler.move(folder_dest.resource or '/')

        if not dest_exists and not utils.has_trailing_slash(dest):
            file_handler.rename(dest.name)

    def copy(self, dest, move_within_project=False):
        """Copies data object to destination path.

        Args:
            dest (path|str): The destination file or directory.
            move_within_project (bool): If True, move the file instead of cloning.
                Only comes into effect when both source and destination are
                within the same DX Project

        Raises:
            DNAnexusError: When cloning within same project with move_within_project=False
            DuplicateError: When cloning data object or folder to already existing path
            NotFoundError: When the source file path doesn't exist
        """
        dest = Path(dest)
        if utils.is_dx_path(dest):
            if self.isfile():
                if dest.canonical_project == self.canonical_project:
                    if move_within_project:
                        self._move(dest)
                    else:
                        raise DNAnexusError('Cannot copy within same project without flag set')
                else:
                    self._clone(dest)
            else:
                raise NotFoundError('No data object was found for the given path on DNAnexus')
        else:
            super(DXPath, self).copy(dest)

    def copytree(self, dest, move_within_project=False):
        """Copies a source directory to a destination directory.
        This is not an atomic operation. Also note that copytree
        from dx to dx paths doesn't copy empty directories

        Refer to utils.copytree for detailed information.

        Args:
            dest (path|str): The directory to copy to. Must not exist if
                its a posix directory
            move_within_project (bool): If True, move the file instead of cloning.
                Only comes into effect when both source and destination are
                within the same DX Project

        Raises:
            DNAnexusError: When cloning within same project with move_within_project=False
            DuplicateError: When cloning data object or folder to already existing path
            NotFoundError: When the source directory path doesn't exist
        """
        dest = Path(dest)
        if utils.is_dx_path(dest):
            if self.isdir():
                if dest.canonical_project == self.canonical_project:
                    if move_within_project:
                        self._movetree(dest)
                    else:
                        raise DNAnexusError('Cannot copytree within same project without flag set')
                else:
                    self._clonetree(dest)
            else:
                raise NotFoundError('No project or directory was found at path ({})'.format(self))
        else:
            super(DXPath, self).copytree(dest)

    def _clonetree(self, dest):
        """Clones the project or directory into the destination path.
        The original tree is retained.

        Args:
            dest (path): The destination directory path in a different project

        Raises:
            DuplicateError: When destination directory already exists
            DNAnexusError: When cloning within same project
        """
        if dest.canonical_project == self.canonical_project:
            raise DNAnexusError('Cannot clonetree within same project')
        if dest == (self.drive+dest.project):  # need to convert dx://proj to dx://proj:
            dest = dest + ':'
        source = utils.remove_trailing_slash(self)
        dest_exists = dest.isdir()
        to_rename = True

        if dest_exists or utils.has_trailing_slash(dest):
            dest = dest / (source.name if source.resource else source.virtual_project)
            if dest.isdir():
                raise DuplicateError('Dest path already exists. Do not duplicate!')
            to_rename = False

        folder_dest = dest.parent
        if not source.resource or not dest.resource:
            dest.makedirs_p()
        elif not dest_exists and folder_dest.resource:  # avoid calling makedirs_p on project
            folder_dest.makedirs_p()

        project_handler = dxpy.DXProject(source.canonical_project)
        project_handler.clone(
            container=dest.canonical_project,
            destination=(folder_dest.resource or '/') if source.resource else dest.resource,
            folders=[source.resource or '/']
        )

        if source.resource and to_rename:
            moved_folder_path = folder_dest / source.name
            dxpy.api.project_rename_folder(
                dest.canonical_project,
                input_params={
                    'folder': moved_folder_path.resource,
                    'name': dest.name
                }
            )

    def _movetree(self, dest):
        """Moves the project or directory to a different folder within project

        Args:
            dest (path): The destination directory path within same project

        Raises:
            DuplicateError: When destination directory already exists
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

        source = utils.remove_trailing_slash(self)
        dest_exists = dest.isdir()
        to_rename = True

        if dest_exists or utils.has_trailing_slash(dest):
            dest = dest / source.name
            if dest.isdir():
                raise DuplicateError('Dest path already exists. Do not duplicate!')
            to_rename = False

        folder_dest = dest.parent
        if not dest.resource:
            dest.makedirs_p()
        elif not dest_exists and folder_dest.resource:  # avoid calling makedirs_p on project
            folder_dest.makedirs_p()

        project_handler = dxpy.DXProject(source.canonical_project)
        project_handler.move_folder(
            folder=source.resource,
            destination=folder_dest.resource or '/'
        )

        if to_rename:
            moved_folder_path = folder_dest / source.name
            dxpy.api.project_rename_folder(
                dest.canonical_project,
                input_params={
                    'folder': moved_folder_path.resource,
                    'name': dest.name
                }
            )

    def download_object(self, dest, **kwargs):
        """Download a single path or object to file.

        Args:
            dest (Path): The output file

        Raises:
            ValueError: When source path is not a file
        """
        if not self.isfile():
            raise NotFoundError('No data object was found at given path ({}) on DNAnexus'
                                .format(self))
        dxpy.download_dxfile(
            dxid=self.canonical_resource,
            filename=dest,
            project=self.canonical_project
        )

    def download(self, dest, **kwargs):
        """Download a directory.

        Args:
            dest (Path): The output directory

        Raises:
            ValueError: When source or dest path is not a directory
        """
        if not self.isdir():
            raise NotFoundError('No folder or project was found at given path ({}) on DNAnexus'
                                .format(self))
        dxpy.download_folder(
            project=self.canonical_project,
            destdir=dest,
            folder=self.resource or '/'
        )

    def upload(self, to_upload, **kwargs):
        """Upload a list of files and directories to a directory.

        Note that unlike swift, this is not a batch level operation.
        If some file errors, the files before will remain uploaded

        Args:
            to_upload (List): A list of file names, directory names, or
                OBSUploadObject objects to upload.

        Raises:
            ValueError: When source path is not a directory
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
                            object_name=(self.resource or Path('')) /
                            utils.file_name_to_object_name(f))
            for f in all_files_to_upload
        ])
        for upload_obj in dx_upload_objects:
            upload_obj.object_name = Path(upload_obj.object_name)
            upload_obj.source = Path(upload_obj.source)
            dest_file = Path('{drive}{project}:{path}'.format(
                drive=self.drive, project=self.canonical_project,
                path=upload_obj.object_name))

            dest_exists = dest_file.isdir()
            if upload_obj.source.isdir():
                if dest_exists:
                    raise DuplicateError('Dest path already exists. Do not duplicate!')
                else:
                    dest_file.makedirs_p()
                    continue

            if dest_exists:
                new_dest = dest_file / upload_obj.source.name
                if new_dest.isfile():
                    raise DuplicateError('Dest path already exists. Do not duplicate!')
                folder_dest = dest_file
                file_name = upload_obj.source.name
            else:
                folder_dest = dest_file.parent
                file_name = upload_obj.object_name.name

            dxpy.upload_local_file(
                filename=upload_obj.source,
                project=self.canonical_project,
                folder=folder_dest.resource or '/',
                parents=True,
                name=file_name
            )

    def open(self, mode='r', encoding=None):
        """
        Opens a OBSFile that can be read or written to and is uploaded to
        the remote service.
        """
        raise NotImplementedError

    def list(self,
             canonicalize=False,
             starts_with=None,
             limit=None,
             classname=None,
             condition=None
             ):
        """List contents using the resource of the path as a prefix.

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
        """Iterate over contents using the resource of the path as a prefix.

        Args:
            canonicalize (bool, default False): if True, return canonical paths
            starts_with (str): Allows for an additional search path to
                be appended to the resource of the dx path. Note that this
                resource path is treated as a directory
            limit (int): Limit the amount of results returned
            classname (str): Restricting class : One of 'record', 'file', 'gtable,
                'applet', 'workflow'

        Returns:
             Iter[DXPath]: Iterates over listed files that match an optional pattern.
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
            only (str): "objects" for only objects, "folders" for only folder,
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
            'folder': self.resource or '/'
        }
        try:
            obj_dict = dxpy.DXProject(dxid=proj_id).list_folder(**kwargs)
        except dxpy.exceptions.ResourceNotFound:
            raise NotFoundError('The specified folder ({}) was not found'.format(
                self.resource))
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
            Iter[DXPath]: Iterates over listed files directly within the resource
        """
        folders = self.listdir(only='folders', canonicalize=canonicalize)
        for folder in folders:
            yield folder
        for data in self.walkfiles(
                        canonicalize=canonicalize,
                        recurse=False
                    ):
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
            'folder': (self.resource or '/') + (starts_with or '')
        }
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

    def exists(self):
        """Checks existence of the path.

        Returns True if the path exists, False otherwise.

        Returns:
            bool: True if the path exists, False otherwise.
        """
        try:
            # first see if there is a specific corresponding object
            self.stat()
            return True
        except (NotFoundError, ValueError):
            pass
        # otherwise we could be a directory, so try to listdir folder
        # note list doesn't error on non-existent folder and cannot be used here
        try:
            self.listdir()
            return True
        except NotFoundError:
            return False

    def getsize(self):
        if not self.resource:
            return self.stat()['dataUsage']*1e9
        else:
            return self.stat()['size']

    def stat(self):
        """Performs a stat on the path.

        Raises:
            NotFoundError: When the project or resource cannot be found.
        """
        if not self.resource:
            return dxpy.DXProject(dxid=self.canonical_project).describe()
        return dxpy.DXFile(dxid=self.canonical_resource,
                           project=self.canonical_project).describe()


class DXVirtualPath(DXPath):
    """Class Handler for DXPath of form 'dx://project-{ID}:/a/b/c' or 'dx://a:/b/c'"""

    @property
    def virtual_project(self):
        if utils.is_valid_dxid(self.project, 'project'):
            return dxpy.DXProject(dxid=self.project).name
        return self.project

    @property
    def virtual_resource(self):
        return self.resource

    @property
    def virtual_path(self):
        return self

    @cached_property
    def canonical_project(self):
        """Returns the unique project that matches the name that user has view access to.
        If no match is found, returns None

        Raises:
            DuplicateProjectError: If project name is not unique on DX platform
            NotFoundError: If project name doesn't exist on DNAnexus
        """
        if utils.is_valid_dxid(self.project, 'project'):
            return self.project

        try:
            proj_dict = dxpy.find_one_project(
                name=self.project, level='VIEW', zero_ok=True, more_ok=False)
        except DXSearchError as e:
            raise DuplicateProjectError('Duplicate projects were found with given name ({})'
                                        .format(self.project), e)

        if proj_dict is None:
            raise ProjectNotFoundError('No projects were found with given name ({})'
                                       .format(self.project))

        return proj_dict['id']

    @cached_property
    def canonical_resource(self):
        """Returns the dx file-ID of the uniquely matched filename

        Raises:
            DuplicateError: if filename is not unique
            NotFoundError: if resource is not found on DX platform
            ValueError: if path is folder path
        """
        if not self.resource:
            return None
        if utils.has_trailing_slash(self):
            raise ValueError('Invalid operation ({method}) on folder path ({path})'
                             .format(path=self, method=sys._getframe(2).f_code.co_name))
        objects = [{
            'name': self.name,
            'folder': self.resource.parent,
            'project': self.canonical_project,
            'batchsize': 2
        }]
        results = dxpy.resolve_data_objects(objects=objects)[0]
        if len(results) > 1:
            raise DuplicateError('Multiple objects found at path ({}). '
                                 'Try using a canonical ID instead'.format(self))
        elif len(results) == 1:
            return results[0]['id']
        else:
            raise NotFoundError('No data object was found for the given path on DNAnexus')

    @property
    def canonical_path(self):
        """Returns the unique file that matches the given path"""
        return DXCanonicalPath('{drive}{proj_id}:/{resource}'.format(
            drive=self.drive, proj_id=self.canonical_project,
            resource=(self.canonical_resource or '')))


class DXCanonicalPath(DXPath):
    """Class Handler for DXPath of form 'dx://project-{ID}:/file-{ID}' or 'dx://project-{ID}:'"""

    @property
    def virtual_project(self):
        return self.virtual_path.project

    @property
    def virtual_resource(self):
        return self.virtual_path.resource

    @cached_property
    def virtual_path(self):
        proj = dxpy.DXProject(dxid=self.project)
        virtual_p = DXVirtualPath(self.drive + proj.name + ':/')
        if self.resource:
            file_h = dxpy.DXFile(dxid=self.canonical_resource,
                                 project=self.canonical_project)
            virtual_p = virtual_p / file_h.folder[1:] / file_h.name
        return virtual_p

    @property
    def canonical_project(self):
        return self.project

    @property
    def canonical_resource(self):
        return self.resource.lstrip('/') if self.resource else None

    @property
    def canonical_path(self):
        return self
