.. module:: storage_utils.swift

.. _swift:

Swift
=====

Authentication Settings
-----------------------

.. autodata:: auth_url

.. autodata:: username

.. autodata:: password

.. _swiftretry:

Retry Settings
--------------

.. autodata:: initial_retry_sleep

.. autodata:: num_retries

.. autodata:: retry_sleep_function


SwiftPath
---------

.. autoclass:: SwiftPath
  :members:

  .. automethod:: exists()

  .. automethod:: first()

  .. automethod:: glob(pattern, num_objs_cond=None, \**retry_args)

  .. automethod:: list(starts_with=None, limit=None, num_objs_cond=None, \**retry_args)

  .. automethod:: post(options)

  .. automethod:: remove(\**retry_args)

  .. automethod:: stat()

  .. automethod:: copy(dest, \**retry_args)

  .. automethod:: download_objects(dest, objects, object_threads=10, container_threads=10, \**retry_args)

  .. automethod:: upload(to_upload, segment_size=DEFAULT_SEGMENT_SIZE, use_slo=True, segment_container=None, leave_segments=False, changed=False, object_name=None, object_threads=10, segment_threads=10)


SwiftFile
---------

.. autoclass:: SwiftFile
  :members:

Errors
------

.. autoexception:: SwiftError

.. autoexception:: NotFoundError

.. autoexception:: UnavailableError

.. autoexception:: UnauthorizedError

.. autoexception:: ConfigurationError

.. autoexception:: ConditionNotMetError
