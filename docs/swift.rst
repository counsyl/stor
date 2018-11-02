.. module:: stor.swift

.. _swift:

Swift
=====

.. _swiftretry:


Retry Settings
--------------

.. autodata:: initial_retry_sleep

.. autodata:: retry_sleep_function


SwiftPath
---------

.. autoclass:: SwiftPath
  :members:

  .. automethod:: open(mode='r')

  .. automethod:: glob(pattern, num_objs_cond=None, \**retry_args)

  .. automethod:: exists(\**retry_args)

  .. automethod:: remove(\**retry_args)

  .. automethod:: rmtree(\**retry_args)

  .. automethod:: copy(dest, swift_retry_args=None)

  .. automethod:: copytree(dest, copy_cmd='cp -r')

  .. automethod:: upload(to_upload, segment_size=DEFAULT_SEGMENT_SIZE, use_slo=True, segment_container=None, leave_segments=False, changed=False, object_name=None, object_threads=10, segment_threads=10, condition=None, use_manifest=False, \**retry_args)

  .. automethod:: download(dest, object_threads=10, container_threads=10, condition=None, use_manifest=False, \**retry_args)

  .. automethod:: download_objects(dest, objects, object_threads=10, container_threads=10, \**retry_args)

  .. automethod:: first(\**retry_args)

  .. automethod:: list(starts_with=None, limit=None, condition=None, use_manifest=False, \**retry_args)

  .. automethod:: post(options=None, \**retry_args)

  .. automethod:: stat(\**retry_args)

  .. automethod:: temp_url


Using Swift Conditions with Retry Settings
------------------------------------------

Swift is a storage system with eventual consistency, meaning (for example) that
uploaded objects may not be able to be listed immediately after being uploaded.
In order to make applications more resilient to consistency issues, various
swift methods can take conditions that must pass before results are returned.

For example, imagine your application is downloading data using the
`SwiftPath.download` method. In order to ensure that your application downloads
exactly 10 objects, one can do the following::

    SwiftPath('swift://tenant/container/dir').download('.', condition=lambda results: len(results) == 10)

In the above, ``condition`` takes the results from `SwiftPath.download` and verifies there are
10 elements. If the condition fails, `SwiftPath.download` will retry based on `retry settings <swiftretry>`
until finally throwing a `ConditionNotMetError` if the condition is not met. If ``condition`` passes,
``download`` returns results.

Note that if you want to combine multiple conditions, you can do this easily as::

  condition = lambda results: all(f(results) for f in my_list_of_conditions)


SwiftUploadObject
-----------------

.. autoclass:: SwiftUploadObject
  :members:

Utilities
---------

.. autofunction:: stor.utils.file_name_to_object_name
