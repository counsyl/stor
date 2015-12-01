counsyl-storage-util Documentation
===================================
Counsyl Storage Utils provides utilities for performing Posix file system IO and accessing object-based storage. Below is a quick start for using the core ``path`` functionality of this module. Other low-level interfaces are described in the later sections

Path Quick Start
----------------
The core of this project is a ``path`` factory function for accessing Posix file systems or Swift object based storage in a unified manner. The returned paths behave in the same manner of those from `path.py <https://pypi.python.org/pypi/path.py>`_. The ``path`` factory can be used to instantiate a path to a Posix file system or to Swift object storage like so:

.. code-block:: python

  from storage_utils import path

  p = path('/my/local/path')

  # Perform normal path.py operations
  p = p / 'file'
  contents = p.open().read()

  # Access swift object storage
  p = path('swift://tenant/container/object/being/accessed')

  # Perform a restricted set of path.py operations
  files = p.glob('prefix*')


With this interface, one can write code that is compatible with Swift and Posix file storage. For more details about the path module and how to access Swift storage directory, go to the :ref:`path` section.


In Depth Documentation
----------------------
Along with a generic ``path`` factory, this project provides interfaces that are specific to accessing Posix file systems (``Path``) and for Swift object-based storage (``SwiftPath``). These interfaces are documented in the :ref:`path` section.

Counsyl Storage Utils also provides some utilities related to file system procedures. These are described in the :ref:`utils` section.
