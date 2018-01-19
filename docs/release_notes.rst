Release Notes
=============

v1.5.3
------

* Hoist ``stor.utils.is_obs_path`` --> ``stor.is_obs_path``

v1.5.2
------

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
