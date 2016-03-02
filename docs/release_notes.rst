Release Notes
=============

v6.0.0
------

* Renamed `storage_utils.is_posix_path` to `storage_utils.is_filesystem_path` but
  kept `storage_utils.is_posix_path` for backwards compatibility
* Added `WindowsPath` for windows compatibility
* Updated swift methods so that they always use forward slashes in paths, even if
  objects are uploaded from windows
* Added `swift.file_name_to_object_name` for converting file names on any system to
  their associated object name

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
  with `PosixPath`, such as `SwiftPath.normpath`, `SwiftPath.expand`, and
  `SwiftPath.expandvars`.

* Include additional abilty to write objects returned by `SwiftPath.open`.

* Added `SwiftPath.stat` to get metadata about tenants, containers, and objects.

* Added `SwiftPath.listdir` to list directories

v2.1.0
------

* Allowed the ability to write individual opens after they are opened with
  `SwiftPath.open`.

* Updated the default args to `SwiftPath.upload`. Static large objects are
  used by default.

* add `SwiftPath.expand`, `SwiftPath.expandvars` and `SwiftPath.normpath`
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
