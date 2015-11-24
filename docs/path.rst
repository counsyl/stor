..  _path:

Path
====
Counsyl IO comes with the ability to create paths in a similar manner to `path.py <https://pypi.python.org/pypi/path.py>`_. The path utilities are as follows:

* A ``path`` factory function that is used to construct the appropriate object based on the path. This is the primary interface that should be used for obtaining access to resources.
* A ``SwiftPath`` object that is returned if the path starts with "swift://". This class provides the ability to access Swift object storage using a restricted version of path.py. Along with inheriting from ``Path`` (in the package from above), it provides several other methods specific to Swift object storage that are documented in more detail.
* An ``OSPath`` object that is returned for all other paths. This class inherits from ``Path`` and provides other custom methods for path manipulation that are documented in more detail.


Path Factory
------------
The ``path`` factory is the primary interface for constructing paths. Its usage is described below.

.. automodule:: counsyl_io
.. autofunction:: counsyl_io.path


Common Path Methods
-------------------
By using the ``path`` factory function, users can write code that is portable across Swift storage and Posix-based file systems. In order to ensure portability, users can only use the following methods:

* ``open``: Opens the resource identified by the path and returns a file (or file-like) object.
* ``glob``: Globs the path based on an input pattern. Returns matching path objects.
* ``exists``: Returns True if the resource exists, False otherwise.
* ``remove``: Removes the resource.
* ``rmtree``: Removes a directory and all resources it contains.

.. NOTE::
   ``OSPath`` fully implements the above methods, but ``SwiftPath`` may only partially implement the method (e.g. only allowing prefix globbing). Read the ``SwiftPath`` documentation below about restrictions on the methods.


OSPath
------
``OSPath`` objects returned from the ``path`` factory fully support all operations from the `path.py <https://pypi.python.org/pypi/path.py>`_ project. For a full listing of the supported methods, look at the `path API <https://pythonhosted.org/path.py/api.html>`_. Additional methods on ``OSPath`` are described below:

.. automodule:: counsyl_io.os_path 
.. autoclass:: counsyl_io.os_path.OSPath
    :members:

    .. automethod:: __new__


SwiftPath
---------
``SwiftPath`` objects returned from the ``path`` factory partially support the common path methods listed above along with supporting the basic path manipulation methods (e.g. 'my' / 'path' == 'my/path').

.. automodule:: counsyl_io.swift_path 
.. autoclass:: counsyl_io.swift_path.SwiftPath
    :members:

    .. automethod:: __new__