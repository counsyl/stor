Stor Modularization
===================

The legacy version of stor has been split into 4 packages: ``stor``, ``stor_dx``, ``stor_swift``, and
``stor_s3``. ``stor_swift``, ``stor_dx``, ``stor_s3`` are modular packages implemented to be used with
the core package ``stor`` directly. The core package ``stor`` now only supports Posix and Windows
filesystems, apart from supporting extra plugins.


Implementation
--------------

Each of the modular packages work by registering themselves onto pkg_resources with an entry_point
``stor.providers``. This entry point should be a function which takes in a prefix and a path, and
returns the cls that the path should be instantiated to, as well as the path that should be initialized.
Typically, this function is called ``class_for_path`` in ``stor_dx``, ``stor_s3``, and ``stor_swift``.
Each plugin module currently raise an error if the prefix passed is not the prefix it supports. For
example, ``stor_dx.class_for_path`` errors if the prefix is not ``dx``. ``get_class_for_path`` in each
plugin module may assume that the prefix is the true prefix to the path argument as this is guaranteed
by the core ``stor`` package.


Code Changes
------------

`stor.copy`, `stor.copytree` and ``stor.open`` which were earlier present in the core ``stor.utils`` and
``stor.obs`` have been split according to their individual functionalities into the three packages.
These functions in the core package now only deal with posix/windows paths while the three plugins
implement the finer aspects of the logic individual to each platform. The only external effect of
these changes is that `stor.copy` and `stor.copytree` now don't support a ``source`` kwarg, instead
expect the first argument to be a ``Path | str``, which is then taken to be the source to be copied from.

``is_swift_path`` has been removed from the core `stor` package. Thus, using ``stor.is_swift_path`` will
fail. This is because the plugins determine the prefix they support and the core package cannot know in
advance if ``swift://`` is a supported path. In cases where ``stor.is_swift_path`` was being used,
``stor.is_obs_path`` is a possible substitution. Thus, each individual plugin ``stor_dx``, ``stor_s3`` and
``stor_swift`` is now responsible for supporting ``is_dx_path``, ``is_s3_path`` and ``is_swift_path`` resp.


Versioning
----------
Since the core stor package and the plugins live on the same github repo, and PBR is used for semantic
versioning, they will have the same published version at any given time. However, it is possible to
install an older version of ``stor`` with a newer version of the plugins ``stor_dx``, etc. The behavior is
undefined in this case. The core ``stor`` package has been implemented to require the extra plugins for now.
