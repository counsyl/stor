..  _path:

Path
====
Counsyl Storage Utils comes with the ability to create paths in a similar manner to `path.py <https://pypi.python.org/pypi/path.py>`_. It is expected that the three functions below are the only ones directly used. (i.e. ``Path`` or ``SwiftPath`` objects should never be explicitly instantiated).

.. automodule:: storage_utils
.. autofunction:: storage_utils.path
.. autofunction:: storage_utils.is_posix_path
.. autofunction:: storage_utils.is_swift_path

.. NOTE::
    It's okay to repeatedly call ``path`` on any ``Path`` object since it will return itself.


Common Path Methods
-------------------
By using the ``path`` factory function, users can write code that is portable across Swift storage and Posix-based file systems. In order to ensure portability, users can only use the following methods:

* ``open``: Opens the resource identified by the path and returns a file (or file-like) object.
* ``glob``: Globs the path based on an input pattern. Returns matching path objects.
* ``exists``: Returns True if the resource exists, False otherwise.
* ``remove``: Removes the resource.
* ``rmtree``: Removes a directory and all resources it contains.

.. NOTE::
   ``Path`` fully implements the above methods, but ``SwiftPath`` may only partially implement the method (e.g. only allowing prefix globbing). Read the ``SwiftPath`` documentation below about restrictions on the methods.


SwiftPath
---------
``SwiftPath`` objects returned from the ``path`` factory partially support the common path methods listed above along with supporting the basic path manipulation methods (e.g. 'my' / 'path' == 'my/path').

.. automodule:: storage_utils.swift_path 
.. autoclass:: storage_utils.swift_path.SwiftPath
    :members:
