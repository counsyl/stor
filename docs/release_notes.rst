Release Notes
=============

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
