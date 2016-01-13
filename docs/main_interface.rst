..  _main_interface:

Main Interface
==============
.. automodule:: storage_utils
  :members:


Common Path Methods
-------------------
By using the ``path`` factory function, users can write code that is portable across Swift storage and Posix-based file systems. In order to ensure portability, users can only use the following methods:

* ``open``: Opens the resource identified by the path and returns a file (or file-like) object.
* ``glob``: Globs the path based on an input pattern. Returns matching path objects.
* ``exists``: Returns True if the resource exists, False otherwise.
* ``remove``: Removes the resource.
* ``rmtree``: Removes a directory and all resources it contains.
* ``copy``: Copies the path to a destination.

.. NOTE::
   ``Path`` fully implements the above methods, but ``SwiftPath`` may only partially implement the method (e.g. only allowing prefix globbing). Read the ``SwiftPath`` documentation below about restrictions on the methods.


SwiftPath
---------
``SwiftPath`` objects returned from the ``path`` factory partially support the common path methods listed above along with supporting the basic path manipulation methods (e.g. 'my' / 'path' == 'my/path'). For more information on accessing swift, go to the :ref:`swift` section.

PosixPath
---------
``PosixPath`` objects return from the ``path`` factory fully support the common path methods listed above and also provide additional functionality. For more information, go to the :ref:`posix` section.
