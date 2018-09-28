.. _dx:

DNAnexus
========

Canonical Paths on DNAnexus
---------------------------
Every file resource on DNAnexus is given a canonical ID to refer it by. This is
internally given by the platform and is not determined by stor. However, stor
does provide the option of referring to the resource through their canonical IDs.
Each individual project and file has a canonical ID associated with it. This
ID consists of 24 case-sensitive characters from ``0123456789BFGJKPQVXYZbfgjkpqvxyz``
prepended with 'project-' or 'file-' respectively. Example canonical IDs are
``project-j47b1k3z8Jqqv001213v312j1``, and ``file-47jK67093475061g3v95369p``.

Each individual file with its unique ID, can manifest in only one project on
DNAnexus. This is because the metadata associated with the file in that project
can only have one path to the file. Also, since the canonicalized path to the
file on DNAnexus is represented by::

    'project-j47b1k3z8Jqqv001213v312j1:file-47jK67093475061g3v95369p'

each project can only have each canonical file at one location within the project.
However, one canonical file can be present in multiple projects at different paths.

Thus, stor has two subclass implementations of `DXPath`: `DXCanonicalPath` and
`DXVirtualPath`. As the name suggests, `DXCanonicalPath` deals with paths like::

    Path('dx://project-j47b1k3z8Jqqv001213v312j1:file-47jK67093475061g3v95369p')
    OR
    Path('dx://project-j47b1k3z8Jqqv001213v312j1')

`DXVirtualPath` handles paths that have any human readable element in them::

    Path('dx://project-j47b1k3z8Jqqv001213v312j1:/path/to/file.txt')
    OR
    Path('dx://myproject:/path/to/file.txt')

You can obtain the `DXCanonicalPath` for a resource from a `DXVirtualPath`
and vice versa.

Projects on DNAnexus
--------------------
All resources on the DX platform are organized into projects, which is the main
container of the resource. The access and billing etc permissions to the resource
are determined by (and the same as) the user's permission to the project.

On stor, the project is the first part of any `DXPath` and is unique because
it must end in a trailing ':'. Every `DXPath` must have a project associated.
More on this later.

Directories on DNAnexus
-----------------------
DNAnexus has the concept of directories on the platform like posix (unlike
Swift/S3). These directories can be empty, and also have names with extensions.
Two folders with the same parent path cannot have the same name, i.e., no
duplicate folders are allowed (like posix).

Note here that folders are not actual resources on the DNAnexus platform.
They are only handled through metadata on the server and as a result, do not
have a canonical ID of their own. As a result, paths to folders can only be
`DXVirtualPath`. Accessing the `DXVirtualPath.canonical_path` property for folders
would raise an error.

Files on DNAnexus
-----------------
Files stored on the DX platform are immutable. This is because the files are
internally stored in AWS while the metadata handling is taken care of by the
platform. Hence, once a file is uploaded, it cannot be modified.

When one file is copied to another project, only additional metadata is produced,
while the underlying file on AWS remains the same. This is essential. The same
file with the same canonical ID will appear in both projects, and can have
different folder paths. Deleting a file from one project is possible, which
deletes the metadata and leaves the file untouched in other projects.

DXPath on stor
--------------
All `DXPath` instances on stor must have a project. The project is the first part
of the path and is determined by a trailing ``:``. Thus, paths without a project
like::

    Path('dx://')
    OR
    Path('dx://path/to/file')

can never be initialized on stor. Trying so will result in an error. The one and
only exception to this scenario is when only a project is mentioned. Thus,::

    Path('dx://myproject:')
    OR
    Path('dx://myproject')

both are valid, and essentially refer to the same proejct path.

Files and directories can have the same name on the DX platform. This is because
files without extentions and/or directories with extensions are allowed. A
`DXPath` instance on stor can be ambiguous as a result. For example, the path::

    Path('dx://myproject:/path/to/folder_file')

can refer to a file and/or a folder, since ``folder_file`` can be a folder and/or
a file. Hence different operations may refer to the different underlying object.
As an example, `DXPath.listdir` would treat the above path as a folder, while
`DXPath.stat` would treat the same path as a file.

DXPath
-------

.. automodule:: stor.dx
    :members:

Behavior of copy and copytree
-----------------------------
Since there are directories on DNAnexus, and  a file and a folder with the
same path can also have duplicate names, copy and copytree work differently
on the platform compared to other OBS services (Swift/S3).

Suppose we are trying to copy a file to a destination path::

    stor.copy('dx://project1:/folder/file.txt', 'dx://project2:/new_folder')
    OR
    Path('dx://project1:/folder/file.txt').copy('dx://project2:/new_folder')

The outcome of the above commands is determined by the file/folder structure
already present at the destination. If ``project2:/new_folder`` is an already
existing directory, stor will set the new destination of the file as under that
folder (i.e. as ``dx://project2:/new_folder/file.txt``). If the final destination
path already exists, it deletes this file and copies the new file there.
Conversely, if ``project2:/new_folder`` is not an existing directory, stor
attempts to copy ``file.txt`` to ``project2:/new_folder`` as a file (remember:
files without extensions are allowed, hence stor has no reason to believe
``new_folder`` is not a filename we're trying to copy to). Thus, stor will
copy our file to the *file* ``project2:/new_folder``. The caveat here again
is that if ``project2:/new_folder`` already existed as a file, it is deleted,
before a copy is attempted.

Copytree also works in a similar fashion, wherein directory names are compared,
instead of filenames. Suppose we are trying to copytree a directory::

    stor.copytree('dx://project1:/folder', 'dx://project2:/new_folder')
    OR
    Path('dx://project1:/folder').copytree('dx://project2:/new_folder')

Again, if the ``project2:/new_folder`` is an already existing directory, stor
will set the new destination for the folder as a subfolder to that directory
(i.e. as ``dx://project2:/new_folder/folder``. However, if this new destination is
already present, a `TargetExistsError` is raised, instead of deleting it like in
`DXPath.copy`, because we don't wish to delete full directories through copytree
(use `DXPath.rmtree` for that) or merge two directories. If ``project2:/new_folder``
is not originally present, then the folder is copied to this path.

The above discussion was for DNAnexus to DNAnexus paths. The scenario shifts
for posix to DX or DX to posix paths. For posix to DX paths, the destination
mentioned in the copy/copytree command is never altered based on the dest
filesystem. The original destination mentioned in the command is taken to be
the only intended endpoint. Thus, in our scenario above, if ``project2:/new_folder``
does not exist as a file, the copy is done to that file, else a `TargetExistsError`
is raised. Similarly for copytree on a directory. This behavior is the same
as other OBS services (SwiFT/S3) that has been traditionally applied.
For DX to posix paths also, the traditional behavior has been followed,
to maintain consistency on posix systems.

Open on stor
------------
The ``open`` functionality in stor works by returning an instance of
``stor.obs.OBSFile`` like with other OBS paths(Swift/S3). Although the python
package of DNAnexus ``dxpy`` also has an open functionality on their DXFile,
this is not carried over to stor. One of the main reasons to do this is to wrap
the scope of dxfile.open to what is expected of stor. As an example, ``dxpy``'s
version of ``DXFile.open`` does not have ``readline`` and ``readlines`` methods
for reading the file. On the other hand, ``dxpy`` does support an 'append' mode
to their ``DXFile.open`` which can be confusing to a stor user, because there
are very restricted scenarios this can be used in, and the user would have to
know the different internal states of a file on the DNAnexus platform,
what they mean, when they happen, what operations are allowed on them, etc.
By instantiating ``stor.obs.OBSFile`` for ``open``, we maintain the
support that is standard by stor, without any real decrease in functionality.

DX on Python 3
--------------
DNAnexus is in the process of supporting Python 3 for their platform. As a result,
some of the functions will not work on stor for Python 3 until support is
provided. This includes the copy function to DX paths, and copy/copytree
from posix to dx paths. In general, other stor functions *should* work but any
function that uses dxpy's 'upload' and/or 'clone' methods is prone to fail.