stor Documentation
==================
Stor provides utilities for performing Posix/Windows file system IO and accessing object-based storage. Below is a quick start for using the core ``path`` functionality of this module. Other low-level interfaces are described in the later sections

Path Quick Start
----------------

The core of this project is a shared cross-compatible API for doing file
manipulation and file access on Posix/Windows file systems and Swift Object
Based Storage. The module is designed to work either as a drop-in replacement
for most functionality found in ``os.path`` or with an object-oriented API via
``stor.Path``.

Ultimately, stor lets you write one piece of code to work with local
or remote files.

.. code-block:: python

  import stor

  p = '/my/local/somemanifest.json'

  # Perform normal path.py operations
  p = stor.join(p, 'file')
  with stor.open(p) as fp:
    json.load(fp)

  # Access swift object storage
  p = 'swift://tenant/container/object/somemanifest.json')
  with stor.open(p) as fp:
    json.load(fp)


With this interface, one can write code that is compatible with Swift, Posix, and Windows file storage. For more details about the path module and how to access Swift storage directory, go to the :ref:`main_interface` section.


In Depth Documentation
----------------------
For more detailed documentation about the various interfaces offered by Counsyl Storage Utils, check out the following:

- :ref:`main_interface`: In depth documentation about the main interface.
- :ref:`swift`: In depth documentation about Swift access.
- :ref:`posix`: In depth documentation about Posix access.
- :ref:`windows`: In depth documentation about Windows access.
- :ref:`testing`: Testing components for testing with Swift storage.
