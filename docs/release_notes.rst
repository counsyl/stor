Release Notes
=============

v2.1.0
------

* Allowed the ability to write individual opens after they are opened with
  `SwiftPath.open`.

* Updated the default args to `SwiftPath.upload`. Static large objects are
  used by default.

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
