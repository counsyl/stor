Contributing
============

Running the tests
-----------------

To run all tests, type::

    make test

Run Swift Integration Tests
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Set the following env vars:

* ``SWIFT_TEST_USERNAME`` - username for test (will also set test tenant)
* ``SWIFT_TEST_PASSWORD`` - password to run tests
* ``OS_AUTH_URL`` - url that runs tests

Run S3 Integration Tests
^^^^^^^^^^^^^^^^^^^^^^^^

Set following environment variables for an AWS IAM user that has
read/write/list permissions:

* ``AWS_TEST_ACCESS_KEY_ID``
* ``AWS_TEST_SECRET_ACCESS_KEY_ID``
* (optional) ``AWS_TEST_BUCKET`` - overrides ``s3://stor-test-bucket`` as target.

Run Swift -> S3 and S3 -> Swift Integration Tests
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Set both sets of env vars!

Code Quality
------------

We use flake8 for code quality, which you can run easily with::

    make lint

Code Styling
------------
Please follow `Google's python style`_ guide wherever possible, particularly
for docstrings. We use 4 spaces to indent and a 99 character max line limit.

.. _Google's python style: http://google-styleguide.googlecode.com/svn/trunk/pyguide.html

Building the docs
-----------------

When in the project directory::

    make docs
    open docs/_build/html/index.html


Running Integration Tests
-------------------------


.. automodule:: stor.tests.integration
    :members:
