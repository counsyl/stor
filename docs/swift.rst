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

  .. automethod:: copy(dest, \**retry_args)

  .. automethod:: download(dest, object_threads=10, container_threads=10, num_objs_cond=None, \**retry_args)

  .. automethod:: download_object(out_file, \**retry_args)

  .. automethod:: download_objects(dest, objects, object_threads=10, container_threads=10, \**retry_args)

  .. automethod:: exists(\**retry_args)

  .. automethod:: first(\**retry_args)

  .. automethod:: glob(pattern, num_objs_cond=None, \**retry_args)

  .. automethod:: list(starts_with=None, limit=None, num_objs_cond=None, \**retry_args)

  .. automethod:: post(options=None, \**retry_args)

  .. automethod:: remove(\**retry_args)

  .. automethod:: read_object(\**retry_args)

  .. automethod:: rmtree(\**retry_args)

  .. automethod:: stat(\**retry_args)

  .. automethod:: upload(to_upload, segment_size=DEFAULT_SEGMENT_SIZE, use_slo=True, segment_container=None, leave_segments=False, changed=False, object_name=None, object_threads=10, segment_threads=10)


SwiftFile
---------

.. autoclass:: SwiftFile
  :members:

Exceptions
----------

.. autoexception:: SwiftError

.. autoexception:: NotFoundError

.. autoexception:: UnavailableError

.. autoexception:: UnauthorizedError

.. autoexception:: ConfigurationError

.. autoexception:: ConditionNotMetError
