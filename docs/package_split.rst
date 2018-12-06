Stor Modularization
===================

Stor is implemented as modular packages: the core ``stor`` package which is required to use stor, and
which handles posix and windows (local) paths, and the plugins ``stor_dx``, ``stor_swift``, and
``stor_s3``. The plugins are meant to be used with the core package ``stor``, and are not meant to
function as stand-alone packages. This may be changed in the future.

The legacy version of stor was implemented as one monolith which supported dx, swift, s3, etc, but
was changed due to extraneous dependencies and modular requirements. Stor now supports multiple backends
flexibly, so that only required dependencies need to be installed. This page describes these changes.


Implementation
--------------

Each of the modular packages work by registering themselves onto ``pkg_resources`` with an entry_point
``stor.providers`` using setuptools. This configuration is placed in ``setup.cfg`` of each package.
This entry point is a function which takes in a prefix and a path, and returns the class that the path
should be instantiated to, as well as the path (with any possible changes) that should be initialized.
Typically, this function is called ``class_for_path`` in ``stor_dx``, ``stor_s3``, and ``stor_swift``.

.. code:: python

    [entry_points]
    stor.providers =
        dx = stor_dx:class_for_path


A plugin must implement a single function that takes in A(prefix) and B(str) and returns a tuple of
(C(class), D(str)),  then it can be registered with ``stor.providers``. The prefix determines the path
prefix that the plugin in question would support, and the paths stor would forward to the plugin. For
example, ``stor_dx.class_for_path`` errors if the prefix is not ``dx``. In addition, ``class_for_path``
in each plugin module can assume that the prefix argument is truly the prefix to the path argument as
this is guaranteed by the core ``stor`` package. Future plugins will be implemented in this fashion.


Versioning
----------
Since the core stor package and the plugins live on the same github repo, and PBR is used for semantic
versioning, they will have the same published version at any given time. However, it is possible to
install an older version of ``stor`` with a newer version of the plugins ``stor_dx``, etc. The behavior is
undefined in this case. The core ``stor`` package has been implemented to require the extra plugins for now.
