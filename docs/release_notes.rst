Release Notes
=============

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
