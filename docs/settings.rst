Settings
========

Provides global settings and options for accessing OBS services like swift
and configuring methods such as `SwiftPath.upload` and `SwiftPath.download`.

Settings are stored internally as nested dictionaries. When using `update` or
`use`, this dictionary structure should be followed.

Examples:

    An example settings dictionary. ::

        example_settings = {
            'swift:delete': {
                'object_threads': 10
            },
            'swift:download': {
                'container_threads': 10,
                'object_threads': 10,
                'shuffle': True,
                'skip_identical': True
            },
            'swift:upload': {
                'changed': False,
                'checksum': True,
                'leave_segments': True,
                'object_threads': 10,
                'segment_size': 1073741824,
                'segment_threads': 10,
                'skip_identical': False,
                'use_slo': True
            }
        }

Precedence
----------
Settings can be configured in the following ways in order of precedence:

1. When using the :ref:`cli`, a configuration file specified using the ``--config`` flag.

2. Setting environment variables.

3. User-specified configuration in a ``~/.stor.cfg`` file.


Default Settings
----------------

.. literalinclude:: ../stor/default.cfg

Settings API
------------

.. automodule:: stor.settings
    :members:
