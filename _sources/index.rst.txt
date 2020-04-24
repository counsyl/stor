stor Documentation
==================
Stor provides a cross-compatible API for doing file manipulation and file access on Posix/Windows file systems and object-based storage (OBS) systems.

A rich command line interface and library is provided as part of Stor. Below is a quick start for using the :ref:`cli` interface and the core library functionality of this module. Other low-level interfaces are described in the later sections.

CLI Quick Start
---------------

Stor offers a command line interface for easy use of some common
functions provided by the library. Accessing files and paths in object-based storage systems like Swift and S3 is easy: simply prefix the path with ``swift://`` or ``s3://``.

The :ref:`cli` also offers some additional features such as copying from ``stdin``,
setting a current working directory for an OBS system, or outputting a
file's contents to ``stdout`` with a ``cat`` command::


  $ stor cd s3://bucket
  $ stor pwd
  s3://bucket
  swift://
  $ stor list s3:.
  s3://bucket/obj1
  s3://bucket/obj2
  s3://bucket/dir/obj3
  $ echo "hello world" | stor cp - swift://tenant/container/file
  $ stor cat swift://tenant/container/file
  hello world


For more details on using the CLI, go to the :ref:`cli` section or see ``stor --help`` on the command line after installing.

.. note::  The CLI also has tab completion capabilities that can be installed with instructions :ref:`here <cli_tab_completion_installation>`.

Library Quick Start
-------------------

The library is designed to work either as a drop-in replacement
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


With this interface, one can write code that is compatible with S3, Swift, Posix, and Windows file storage. For more details about the library and how to access low-level interfaces directly, go to the :ref:`main_interface` section.


In Depth Documentation
----------------------
For more detailed documentation about the various interfaces offered by stor, check out the following:

- :ref:`cli`: In depth documentation about the command line interface.
- :ref:`main_interface`: In depth documentation about the main library interface.
- :ref:`swift`: In depth documentation about Swift access.
- :ref:`posix`: In depth documentation about Posix access.
- :ref:`windows`: In depth documentation about Windows access.
- :ref:`testing`: Testing components for testing with Swift storage.
