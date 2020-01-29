Release Notes
=============

v4.0 - Python 3 + Poetry!
-------------------------

Only Python 3.6+ is supported - Python 2.7 and Python 3.5 support is dropped!

Note: the major version bump is because we've dropped 2.7 support, aside from that 
this would be a feature level change.

Now stor installs via ``poetry`` (this should be transparent to users).

API Additions
^^^^^^^^^^^^^

* Tab completions are now available via the ``stor completions`` command.
  Use it to dump completions to appropriate location (see :ref:`installation instructions <cli_tab_completion_installation>`
  for more detail).
* Added ``content_type`` property for OBS paths representing consistent
  Content-Type header that would be set on download.
* Added ``DX_FILE_PROXY_URL`` and ``[dx] -> file_proxy_url`` setting to allow for constructing
  DNAnexus paths with a custom gateway.  You might want this to be able to construct valid bam
  paths for samtools or IGV!

  Generally, usage will look like::

    $ stor url dx://proj:/folder/mypath.csv
    https://dl.dnanex.us/F/D/awe1323/mypath.csv
    $ export DX_FILE_PROXY_URL=https://my-dnax-proxy.example.com/gateway
    $ stor url dx://proj:/folder/mypath.csv
    https://my-dnax-proxy.example.com/gateway/proj/folder/mypath.csv

  Interested in your own file proxy gateway? Email support@dnanexus.com to request it! :)
  Also, you can contact the Bioinformatics Software Engineering team at Myriad Genetics
  to ask to share code.

API Breaks
^^^^^^^^^^

* ``stor-completions.bash`` is no longer on the path (see :ref:`installation instructions <cli_tab_completion_installation>`
  for more detail).
* Only python3.6+ is supported. Python 2.7 and Python 3.5 are no longer supported.

Bug Fixes
^^^^^^^^^

* Do not ever ``wait_on_close`` when write happened in ``__del__`` method (so
  no accidental blocking of main thread). Fixes issues where malformed
  ``OBSFile`` objects would throw an exception in ``__del__`` method.
* Only call ``OBSFile._wait_on_close()`` when we actually wrote data AND we
  were in write mode.
* Eliminate exception in ``__del__`` method when calling ``stor.open`` on a DNAnexus project path.
* ``stor.open()`` now throws an error earlier when incorrectly calling ``open()`` on a DNAnexus project path.
* Now ``temp_url()`` always sets filename to virtual object name when creating temp urls, matching the docstring.
  To get the old (buggy!) behavior, pass ``filename=''`` to ``temp_url()``

Developer Changes
^^^^^^^^^^^^^^^^^

* ``stor`` now installs via ``poetry``. You'll need it installed to work with the library.
* Tests for ``stor`` now run on Github Actions on Python 3.6+ and we've removed tox for local testing.

v3.1.0
------

* Added python 3.7 to test suite, removed EOL python 3.4

API Additions
^^^^^^^^^^^^^

* Implement ``readable()``, ``writable()``, and ``seekable()`` methods on
  ``OBSFile`` so it better implements ``io.IOBase`` specification.

v3.0.5
------
* Changed ``dxpy3`` requirement to ``dxpy`` to reflect new Python3 compatible ``dxpy`` module.

v3.0.4
------
* Fix release notes.

v3.0.3
------
* Fix required version of ``six`` package. We need at least 1.9.0, because we're using ``six.raise_from``.
* Add ``--canonicalize`` flag to ``stor list``, ``stor ls`` and ``stor walkfiles`` cli commands

v3.0.2
------

* Fixed ``DXPath.joinpath`` and ``DXPath.splitpath`` to be custom implementations to handle paths
  with no resources, instead of defaulting to base behavior.

v3.0.1
------

* Fixed ``DXPath.normpath`` functionality  (i.e., for ``DXVirtualPath`` and ``DXCanonicalPath``)
  when no resource present in provided path.

v3.0.0
------

API Additions
^^^^^^^^^^^^^

* Add API and CLI support for manipulating resources on DNAnexus platform via the ``DXPath``
  (i.e., the ``DXVirtualPath`` and ``DXCanonicalPath``) classes.

API Breaks
^^^^^^^^^^

* ``stor.open`` api changed to only accept ``mode`` and ``encoding`` parameters. Other arguments
  (including ``swift_upload_options``  / ``swift_upload_kwargs`` / ``swift_download_kwargs``) removed.
* ``write_object()`` *only* allows ``bytes`` data in Python 3 (it will raise a
  ``TypeError`` otherwise). Behavior in Python 2 is unchanged.
* The alias ``stor.swift.SwiftFile`` (which pointed to ``stor.obs.OBSFile``) is no longer available.

v2.1.3
------

* Retry upload on ``AuthenticationError``.

v2.1.2
------

* Nicer README for PyPI

v2.1.1
------

* Fix trove identifier for License and ensure MIT License is included with source distributions.

v2.1.0
------

* Add ``stor.S3Path.restore(days, tier)`` to restore *single* objects from Glacier to S3.
  Paired with this change are two specific exceptions related to S3 restores,
  ``RestoreAlreadyInProgressError`` (raised when restore has already started)
  and ``AlreadyRestoredError`` (raised when restoring object already in S3).
* Move ``ConflictError`` to ``stor.exceptions`` (still available under ``stor.swift``)

v2.0.0
------

CLI additions
^^^^^^^^^^^^^

* ``stor url <path>`` to translate swift and s3 paths to HTTP paths.
* ``stor convert-swiftstack [--bucket] <path>`` cli tool to convert s3 <-> swiftstack paths.

API Additions
^^^^^^^^^^^^^

* Add ``to_url()`` method on Path and ``url`` cli method to translate swift and s3 paths to HTTP paths.
* GETs on unrestored Glacier objects now raise a more useful ``ObjectInColdStorageError``.
* Add `stor.extensions.swiftstack` module for translating swift paths to s3.
* Add ``stor.makedirs_p(path, mode=0o777)`` to cross-compatible API. This does
  nothing on OBS-paths (just there for convenience).


API Breaks
^^^^^^^^^^

* ``OBSFile`` can no longer (accidentally or intentionally) create zero-byte objects.
* GETs on unrestored Glacier objects no longer raise ``UnauthorizedError`` (see above).
* Removed already-deprecated ``stor.listpath`` and ``stor.path``.


Bug fixes
^^^^^^^^^

* ``OBSFile`` objects no longer attempt to load buffers on garbage collection.
  This should resolve the ``Exception ignored in OBSFile.__del__`` messages and
  eliminate "hangs" on garbage collection or closing python terminal.
* ``stor cp`` no longer claims to be an alias of copy.

Other Changes
^^^^^^^^^^^^^

* ``stor`` no longer depends on ``cached-property``.

v1.5.2
------

* Hoist ``stor.utils.is_obs_path`` --> ``stor.is_obs_path``
* Build universal wheels for both Python 2 and Python 3.
  (no actual code changes)

v1.5.1
------

Summary
^^^^^^^

Many changes to correctly handle binary and text data in both Python 2 and Python 3. Overall, falls
back on ``locale.getpreferredencoding(False)`` to handle this behavior correctly.  This change
should be completely backwards-compatible (any new behaviors would have raised an exception in
earlier versions of stor).

This release also includes some consistency fixes for certain rare edge cases relating to empty or
non/existent files and directories. Drops testing for Python 3.3.

API additions
^^^^^^^^^^^^^

* Add ``encoding`` keyword argument (supported only in Python 3) to ``open()`` and ``OBSFile``.
  This keyword arg overrides default encoding, otherwise, ``encoding`` for text data is pulled from
  ``locale.getpreferredencoding(False)`` the same as Python 3.
* File reading and writing now works in Python 3 in both text and binary modes.

Consistency Fixes
^^^^^^^^^^^^^^^^^

* Fix inconsistency with ``walkfiles()`` on ``PosixPath`` so that it does not
  return empty directories (causes a small potential perf hit).
* Auto-create parent directories on the filesystem for ``stor.copy``, ``stor.open``, and ``stor.copytree``.
* Allow ``stor.copytree`` to work if it targets an empty target directory (removes the other directory first)
* Fix S3 integration tests so they are easier to run.

Deprecations
^^^^^^^^^^^^

* Using text data with ``read_object()`` and ``write_object()`` is deprecated. These functions
  ought to only work with ``bytes`` (and will have unexpected behavior otherwise).
* Python 3.3 is no longer tested in the test suite (but we still think stor
  will run correctly in Python 3.3 - but this was never explicitly supported)

(v1.5.0 was a premature release and was removed from PyPI)

v1.4.6
------

* Tweak segment container hiding regex to be ``.segments`` instead of
  ``.segments_`` (more flexible in general).


v1.4.5
------

* Fixed release notes.

v1.4.4
------

* Fix ``dirname()`` for top-level Swift paths, like ``swift://``.
* Fix ``dirname()`` for top-level S3 paths, like ``s3://``.

v1.4.3
------

* Add ``stor.utils.is_writeable``.

v1.4.2
------

* Fix error message output when specifying ``stor`` without a command under Python 3.

v1.4.1
------

* Support source-only releases in PyPI to allow pip installing from Python 3
  (python 3 wheels are still a TODO)

v1.4.0
------

* Python 3 compatibility :D

v1.3.3
------

* New-style exception messages

v1.3.2
------

* Support multiple files with ``use_manifest=True``


v1.3.1
------

* Ensure OBSFile cleans itself up (commits to remote / deletes local buffer /
  etc) even when not used in ``with`` statement.

v1.3.0
------
* When deleting a swift container, also attempt to delete
  ``$CONTAINER+segments``, which is the format that SwiftStack's S3 emulation
  layer uses for multipart uploads.  (really tiny perf impact, since it only
  applies when directly working with containers).

v1.2.2
------
* Include ``X-Trans-Id`` on auth failures as well.

v1.2.1
------
* Add explicit dependence on six to requirements.txt

v1.2.0
------

* Include ``X-Trans-Id`` header in Swift exception messages and reprs if
  available to facilitate debugging.

v1.1.2
------

* Skip broken symlinks during upload, download and listing of files, allowing
  ``copytree``, ``list`` to work on folders that contain broken symlinks.

v1.1.1
------

* Added .travis.yml for testing against Python 2 and 3
* Added additional coverage to get to 100%
* Updated package classifiers
* Clarify ``stor.glob()``'s strange calling format (that will be altered in a future version of the library).
* Ignore ``DistributionNotFound`` error in weird install situations.

v1.1.0
------

* Rename ``stor.listpath`` to ``stor.list`` for simplicity.

v1.0.0
------

* Initial release of stor
