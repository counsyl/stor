Contributing
============

Running the tests
-----------------

To run all tests, type::

    make test

In order to run swift integration tests, create a swift tenant called ``AUTH_swft_test`` and provide environment variables for a user that has permissions to write to that tenant (``SWIFT_TEST_USERNAME`` and ``SWIFT_TEST_PASSWORD``). Also set the swift auth url environment variable (``OS_AUTH_URL``)

In order to run S3 integration tests, create a ``stor-test-bucket`` S3 bucket and provide environment variables for an AWS user that has permissions to write to it (``AWS_TEST_ACCESS_KEY_ID`` and ``AWS_ACCESS_KEY_ID``).

Code Quality
------------

For code quality, please run flake8::

    pip install flake8
    flake8 .

Code Styling
------------
Please follow `Google's python style`_ guide wherever possible.

.. _Google's python style: http://google-styleguide.googlecode.com/svn/trunk/pyguide.html

Building the docs
-----------------

When in the project directory::

    make docs
    open docs/_build/html/index.html
