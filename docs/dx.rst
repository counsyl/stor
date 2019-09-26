.. _dx:

DNAnexus
========

DNAnexus paths on stor are prefixed with ``dx://`` and have two components:
``dx://<PROJECT>:<FILE_OR_FOLDER>``
where project and file can be virtual paths (i.e., human names) or canonical paths
(opaque globally unique IDs that the platform assigns) - see below for more details.

Canonical Paths on DNAnexus
---------------------------
Files on DNAnexus have a globally unique immutable handle (called a dxid) and also a
virtual path in each project. DNAnexus only allows one copy of a file to be in a specific
project. Also, since the canonicalized path to the file on DNAnexus is represented by::

    'project-j47b1k3z8Jqqv001213v312j1:file-47jK67093475061g3v95369p'

having multiple locations for a single file within a project is infeasible.
However, one canonical file can be present in multiple projects at different paths.

Thus, stor has two subclass implementations of `DXPath`: `DXCanonicalPath` and
`DXVirtualPath`. As the name suggests, `DXCanonicalPath` deals with paths like::

    Path('dx://project-j47b1k3z8Jqqv001213v312j1:file-47jK67093475061g3v95369p')
    OR
    Path('dx://project-j47b1k3z8Jqqv001213v312j1:/file-47jK67093475061g3v95369p')
    OR
    Path('dx://project-j47b1k3z8Jqqv001213v312j1')

`DXVirtualPath` handles paths that have any human readable element in them::

    Path('dx://project-j47b1k3z8Jqqv001213v312j1:/path/to/file.txt')
    OR
    Path('dx://myproject:/path/to/file.txt')
    OR
    Path('dx://myproject:path/to/file.txt')

You can obtain the `DXCanonicalPath` from a `DXVirtualPath` and vice versa, like so::
    >>> stor.Path('dx://StorTesting:/1.bam').canonical_path
    DXCanonicalPath("dx://project-FJq288j0GPJbFJZX43fV5YP1:/file-FKzjPgQ0FZ3VpBkpKJz4Vb70")
    >>> stor.Path('dx://StorTesting:1.bam').canonical_path.canonical_path
    DXCanonicalPath("dx://project-FJq288j0GPJbFJZX43fV5YP1:/file-FKzjPgQ0FZ3VpBkpKJz4Vb70")
    >>> stor.Path('dx://StorTesting:/1.bam').virtual_path.canonical_path
    DXCanonicalPath("dx://project-FJq288j0GPJbFJZX43fV5YP1:/file-FKzjPgQ0FZ3VpBkpKJz4Vb70")

    >>> stor.Path('dx://project-FJq288j0GPJbFJZX43fV5YP1:/file-FKzjPgQ0FZ3VpBkpKJz4Vb70').virtual_path
    DXVirtualPath("dx://StorTesting:/1.bam")


The canonical_path and virtual_path attributes are cached and hence, each call to these
properties doesn't invoke a new API request to the DX server.

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

The summary of different behaviors for different filesystems is presented here. The
individual details are explained further below.

.. list-table:: Filesystem Differences
   :widths: 15 30 25 30
   :header-rows: 1

   * - Topic
     - Posix
     - Swift/S3
     - DNAnexus
   * - File/ Directory names
     - Filenames without extension and dirnames with ext are allowed.
       A dir and a file can have the same name in same path
     - Anonymous file / dir names are not allowed on swift paths
     - Filenames without extension and dirnames with ext are allowed.
       A dir and a file can have the same name in same path
   * - Duplicates
     - Not allowed
     - Not allowed
     - Duplicate filenames are allowed. Duplicate dir names are not allowed
   * - list
     - Recurisvely lists the files and empty directories
     - Recurisvely lists the files and empty directory markers.
     - Recursively lists the files within DX folder path. No empty
       directories are listed.
   * - list with prefix
     - Treated as prefix to absolute path
     - Treated as prefix to absolute path
     - Treated as path to a subfolder to list
   * - list on filepath
     - Returns [filepath]
     - Returns [filepath]
     - Returns []
   * - listdir on non-existent folder
     - Returns []
     - Returns []
     - Raises NotFoundError
   * - copy, target exists and is file
     - Overwrites the file
     - Overwrites the file
     - Deletes the existing file before copying over
   * - copy, target exists and is dir
     - Not allowed
     - Not possible
     - Copied within existing dir
   * - serverside copy/copytree
     - Allowed
     - Not allowed
     - Allowed
   * - copytree, target exists as file
     - Not allowed
     - Allowed
     - Allowed
   * - copytree, target exists as dir
     - Merges the two directories
     - Overwrites the dir
     - Copies inside existing directory.
       If root folder is moved, project name is used while copying if needed.

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
Project is always required for DNAnexus instances::

    >>> Path('dx://path/to/file')
    Traceback (most recent call last):
    ...
    <exception>

but projects are normalized::

    >>> Path('dx://myproject')
    DXVirtualPath('dx://myproject:')

Duplicate names on DNAnexus
---------------------------

A single virtual path can refer to multiple files (and even a folder) simultaneously!
Currently, stor will error if a specific virtual path resolves to multiple files (use
the dx-tool in these cases), but you can always use a canonical path.::

    $ dx upload myfile.txt -o MyProject:/myfile.txt
    $ dx upload anotherfile.txt -o MyProject:/myfile.txt
    $ stor cat dx://MyProject:/myfile.txt
    # MultipleObjectsSameNameError: Multiple objects found at path (dx://StorTesting:/1.bam). Try using a canonical ID instead

When a folder has the same name as a file, stor uses the method you call to check for
a folder or a file (i.e., `DXPath.listdir` will assume folder, `DXPath.stat` will assume file).

DXPath
-------

.. automodule:: stor.dx
    :members:

Copy and copytree by example
-----------------------------
copy and copytree behave differently when the target output path exists and is a folder. (this holds DX -> DX and also POSIX -> DX)

..  list-table:: Copy/copytree example
    :header-rows: 1

    * - command
      - output path (no folder)
      - output path (folder exists)
    * - stor cp  myfile.vcf dx://project2:/call
      - dx://project2:/call
      - dx://project2:/call/myfile.vcf
    * - stor cp -r ./trip-photos dx://newproject:/all
      - dx://newproject:/all
      - dx://newproject/all/trip-photos

Note that if the output path exists and is a file, the file will be *overwritten*


List, Listdir and walkfiles
---------------------------
``stor ls``, ``stor list`` and ``stor walkfiles`` for DXPaths take in a ``--canonicalize``
flag which returns the results with canonical dxIDs instead of human readable virtual
paths. This is especially useful for manipulating paths directly using dx-toolkit
through piping. This flag is ignored when passed for other paths (swift/s3/posix).

Open on stor
------------
The ``open`` functionality in dx works by returning an instance of
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

The dx variable exposed as a setting 'wait_on_close', has a default value of 0 (seconds). This variable
determines how long an ``stor.open`` action waits for the file to go to 'closed' state on DNAnexus.
If a file is not in the 'closed' state internally on the platform, it cannot be read from, or
downloaded. If you need consistency from reading right after writing, then you should set
wait_on_close to be a value > 0. The default is kept so that in the event of multiple uploads,
each upload doesn't wait for *wait_on_close* seconds before initiating the next upload. However,
setting *wait_on_close* > 0 can cause unexpected performance issues, depending on the performance of
the DNAnexus platform, so you need to know what you're doing when changing this.
