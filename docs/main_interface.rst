..  _main_interface:

Main Interface
==============

.. automodule:: stor
  :members:
  :exclude-members: Path

.. module:: stor.base

Path Class
----------

The ``stor.Path`` class is the parent to all concrete base classes and
should be used as the main entry point to the object oriented API.

.. autoclass:: Path
  :members:


Common Path Methods
-------------------

By using the top-level functions in the stor module (or the main
``Path`` interface) users can write code that is portable across Swift storage,
Windows, and Posix-based filesystems.  In particular, the following methods:

* ``open``: Opens the resource identified by the path and returns a file (or file-like) object.
* ``glob``: Globs the path based on an input pattern. Returns matching path objects.
* ``exists``: Returns True if the resource exists, False otherwise.
* ``remove``: Removes the resource.
* ``rmtree``: Removes a directory and all resources it contains.
* ``copy``: Copies the path to a destination.
* ``copytree``: Copies a directory to a destination.
* ``walkfiles``: Recursively walks files and matches an optional pattern.

.. NOTE::
   ``Path`` fully implements the above methods, but ``SwiftPath`` may only
   partially implement the method (e.g. only allowing prefix globbing). Read
   the ``SwiftPath`` documentation below about restrictions on the methods.

.. NOTE::
    Copying from object storage to windows is currently not supported.


SwiftPath
---------

``SwiftPath`` objects returned from the ``path`` factory partially support the
common path methods listed above along with supporting the basic path
manipulation methods (e.g. 'my' / 'path' == 'my/path'). For more information on
accessing swift, go to the :ref:`swift` section.

S3Path
------

``S3Path`` objects returned from the ``Path`` factory partially support the
common path methods listed above along with supporting the basic path
manipulation methods (e.g. 'my' / 'path' == 'my/path'). For more information on
accessing swift, go to the :ref:`s3` section.

PosixPath
---------

``PosixPath`` objects return from the ``path`` factory fully support the common
path methods listed above and also provide additional functionality. For more
information, go to the :ref:`posix` section.

WindowsPath
-----------

``WindowsPath`` objects return from the ``path`` factory fully support the
common path methods listed above and also provide additional functionality. For
more information, go to the :ref:`windows` section.
