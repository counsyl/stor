Release Notes
=============

v6.10.0
-------

* Add InconsistentDownloadError to track when swift downloader hits
  inconsistent md5sum or content-length.

v6.9.0
------

* `SwiftPath.post` works on tenants and objects instead of just containers
* Added ``headers`` option to `SwiftPath.upload`
* Headers are now returned in `SwiftPath.stat`

v6.8.3
------

* Fixed `SwiftPath.temp_url` errors when object names have special URL characters

v6.8.2
------

* Fixed swift method docs

v6.8.1
------

* Fixed output error in upload progress logger

v6.8.0
------

* Add file iterator methods

v6.7.0
------

* Added progress logging to `SwiftPath.upload` and `SwiftPath.download` 

v6.6.1
------

* Updated uploading to ensure manifest is committed first before starting upload
* Fixed an issue where the manifest is uploaded multiple times on upload retries with ``use_manifest=True``

v6.6.0
------

* Added ``FileSystemPath.fnmatch`` method ported from path.py

v6.5.0
------

* Added ``FileSystemPath.walkfiles`` method ported from path.py

v6.4.0
------

* Added `Path.namebase` property

v6.3.1
------

* Add `SwiftFile.tell` method to enable `SwiftFile` to work with `gzip.GzipFile` objects transparently.
* Make `SwiftFile` delegated methods show up in docs.

v6.3.0
------

* Added ``object_threads`` option to `SwiftPath.rmtree` for specifying number of
  delete threads
* Ignore `swift.NotFoundError` exceptions when calling `SwiftPath.rmtree`

v6.2.2
------

* Fix ``storage_utils.NamedTemporaryDirectory`` to delete temp directory on exception as well.

v6.2.1
------

* Upgraded swiftclient fork to fix infinite retry bug on auth token invaliation

v6.2.0
------

* Added ``use_manifest`` option to `SwiftPath.list`, `SwiftPath.download`, and
  `SwiftPath.upload` for generating data manifests and validation

v6.1.0
------

* Added `storage_utils.getsize` to public API (along with
  `Path.getsize` and `SwiftPath.getsize`). Port of `os.path.getsize`.

v6.0.0
------

* Added additional options to `SwiftPath.upload` and `SwiftPath.download`
* Provided a cross-compatible API at module level that accepts strings or ``Path`` objects for easy usage,
  and implemented most ``os.path.is*`` methods on swift, particularly ``isdir()`` and ``isfile()``.
* Removed certain "fancy" path.py methods from ``FileSystemPath`` to reduce the
  public API surface and make it easier to test everything in use.
* Eliminated ``storage_utils.third_party`` and integrated into library (added
  note to LICENSE file with original inspiration)
* Reworked path hierarchy so that all classes inherit from ``storage_utils.Path``
* Segment containers are ignored by default when doing `SwiftPath.listdir`
* Cache authentication credentials for performance.
* Renamed ``storage_utils.is_posix_path``.
* Added `WindowsPath` for windows compatibility
* Updated swift methods so that they always use forward slashes in paths, even if
  objects are uploaded from windows
* Added `swift.file_name_to_object_name` for converting file names on any system to
  their associated object name
* Integration tests are part of package's tests
* Addition of `swift.ConflictError` exception, which is thrown when storage
  nodes have consistency issue deleting container
* Retry logic on `SwiftPath.rmtree` so that container deletes will retry
  when hitting a `swift.ConflictError`
* Swift conditions are now functions that take results and return a Boolean.
* `SwiftPath.download` returns a list of downloaded paths

v5.0.0
------

* Default `SwiftPath.temp_url` to using ``inline=True``, since that's the general
  expectation for how we've used Apache filer / generally you expect URLs to
  render in-browser rather than default to attachment.

v4.5.0
------

* Added `SwiftPath.temp_url` for generating temporary object URLs
* Added environment variable ``OS_TEMP_URL_KEY`` for pulling in default temp url key

v4.4.1
------

* Fixed bug in copying to posix directory
* Copying to a container also throws an error if the path is ambiguous

v4.4.0
------

* Added integration tests

v4.3.0
------

* The default segment container is .segments_$container

v4.2.3
------

* Absolute and relative swift upload paths are handled properly
* copytree abides by shutil behavior, checking that the destination doesn't already exist

v4.2.2
------

* Fixed issue in gocd deployment

v4.2.1
------

* `SwiftPath.rmtree` on a container also deletes the segment container if it exists

v4.2.0
------

* Updated to use new https auth endpoint by default
* Uses a newer fork of python-swiftclient

v4.1.0
------

* Caught HA error for uploading objects and raised it as a `swift.FailedUploadError`

v4.0.0
------

* Added `SwiftPath.copytree` for copying directories
* Updated semantics of `SwiftPath.copy` to only copy one file at a time
* Added `SwiftPath.download_objects` to download a list of objects

v3.0.0
------

* Added `swift.update_settings` function for updating swift module settings. 
  Settings may no longer be changed at module level.

v2.3.0
------

* Included more backwards compatibility methods on `SwiftPath` to be compatible
  with `PosixPath`, such as ``SwiftPath.normpath``, ``SwiftPath.expand``, and
  ``SwiftPath.expandvars``.

* Include additional abilty to write objects returned by `SwiftPath.open`.

* Added `SwiftPath.stat` to get metadata about tenants, containers, and objects.

* Added `SwiftPath.listdir` to list directories

v2.1.0
------

* Allowed the ability to write individual opens after they are opened with
  `SwiftPath.open`.

* Updated the default args to `SwiftPath.upload`. Static large objects are
  used by default.

* add ``SwiftPath.expand``, ``SwiftPath.expandvars`` and ``SwiftPath.normpath``
  following ``os.path`` versions of functions.

v2.0.0
------

* Vendored path.py into ``storage_utils.third_party.path`` to address version
  conflicts in downstream packages and encourage users not to import the path
  class directly.

v1.0.0
------

* 1.0 release. No changes

v0.5.0
------

* Added copy methods to posix and swift paths

v0.4.0
------

* Added UnauthorizedError as swift exception for when permission errors happen

v0.3.0
------

* Added basic path.py methods to SwiftPath

v0.2.1
------

* Fixed a bug in returning values from globbing

v0.2
----

* Added functionality to pass number of threads to upload / download
* Added ability to place conditions on returned results
* Added retry logic to some of the swift calls

v0.1
----

* The initial release of counsyl-storage-utils.
* Provides a path factory that creates SwiftPath and Path objects.
* Provides various utilities for file system procedures.
* Provides a SwiftTestCase class for testing Swift.
