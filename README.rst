stor
====

|Build Status|

``stor`` provides a cross-compatible CLI and Python API for accessing
block and object storage. ``stor`` was created so you could write one
piece of code to work with local or remote files, without needing to
write specialized code to handle failure modes, retrying or temporarily
system unavailability. The functional API (i.e., ``stor.copytree``,
``stor.rmtree``, ``stor.remove``, ``stor.listdir``) will work with the
same semantics across all storage backends. This makes it really easy to
develop/test code locally with files and then take advantage of robust
and cheaper object storage when you push to remote.

View full docs for stor at https://counsyl.github.io/stor/ .

Quickstart
----------

::

    pip install stor

``stor`` provides both a CLI and a Python library for manipulating Posix
and OBS with a single, cross-compatible API.

Quickstart - CLI
----------------

::

    usage: stor [-h] [-c CONFIG_FILE] [--version]
                {list,ls,cp,rm,walkfiles,cat,cd,pwd,clear,url,convert-swiftstack}
                ...

    A command line interface for stor.

    positional arguments:
      {list,ls,cp,rm,walkfiles,cat,cd,pwd,clear,url,convert-swiftstack}
        list                List contents using the path as a prefix.
        ls                  List path as a directory.
        cp                  Copy a source to a destination path.
        rm                  Remove file at a path.
        walkfiles           List all files under a path that match an optional
                            pattern.
        cat                 Output file contents to stdout.
        cd                  Change directory to a given OBS path.
        pwd                 Get the present working directory of a service or all
                            current directories.
        clear               Clear current directories of a specified service.
        url                 generate URI for path
        convert-swiftstack  convert swiftstack paths

    optional arguments:
      -h, --help            show this help message and exit
      -c CONFIG_FILE, --config CONFIG_FILE
                            File containing configuration settings.
      --version             Print version

You can ``ls`` local and remote directories

::

    ›› stor ls s3://stor-test-bucket
    s3://stor-test-bucket/b.txt
    s3://stor-test-bucket/counsyl-storage-utils
    s3://stor-test-bucket/file_test.txt
    s3://stor-test-bucket/counsyl-storage-utils/
    s3://stor-test-bucket/empty/
    s3://stor-test-bucket/lots_of_files/
    s3://stor-test-bucket/small_test/

Copy files locally or remotely or upload from stdin

::

    ›› echo "HELLO WORLD" | stor cp - swift://AUTH_stor_test/hello_world.txt
    starting upload of 1 objects
    upload complete - 1/1   0:00:00 0.00 MB 0.00 MB/s
    ›› stor cat swift://AUTH_stor_test/hello_world.txt
    HELLO WORLD
    ›› stor cp swift://AUTH_stor_test/hello_world.txt hello_world.txt
    ›› stor cat hello_world.txt
    HELLO WORLD

Quickstart - Python
-------------------

List files in a directory, taking advantage of delimiters

.. code:: python

    >>> stor.listdir('s3://bestbucket')
    [S3Path('s3://bestbucket/a/')
     S3Path('s3://bestbucket/b/')]

List all objects in a bucket

.. code:: python

    >>> stor.list('s3://bestbucket')
    [S3Path('s3://bestbucket/a/1.txt')
     S3Path('s3://bestbucket/a/2.txt')
     S3Path('s3://bestbucket/a/3.txt')
     S3Path('s3://bestbucket/b/1.txt')]

Or in a local path

.. code:: python

    >>> stor.list('stor')
    [PosixPath('stor/__init__.py'),
     PosixPath('stor/exceptions.pyc'),
     PosixPath('stor/tests/test_s3.py'),
     PosixPath('stor/tests/test_swift.py'),
     PosixPath('stor/tests/test_integration_swift.py'),
     PosixPath('stor/tests/test_utils.py'),
     PosixPath('stor/posix.pyc'),
     PosixPath('stor/base.py'),

Read and write files from POSIX or OBS, using python file objects.

.. code:: python

    import stor
    with stor.open('/my/exciting.json') as fp:
        data1 = json.load(fp)

    data1['read'] = True

    with stor.open('s3://bestbucket/exciting.json') as fp:
        json.dump(data1, fp)

Testing code that uses stor
---------------------------

The key design consideration of ``stor`` is that your code should be
able to transparently use POSIX or any object storage system to read and
update files. So, rather than use mocks, we suggest that you structure
your test code to point to local filesystem paths and restrict yourself
to the functional API. E.g., in your prod settings, you could set
``DATADIR = 's3://bestbucketever'``\ and when you test, you could use
``DATADIR = '/somewhat/cool/path/to/test/data'``, while your actual code
just says:

.. code:: python

    with stor.open(stor.join(DATADIR, experiment)) as fp:
        data = json.load(fp)

Easy! and no mocks required!

Running the Tests
-----------------

::

    make test

Contributing and Semantic Versioning
------------------------------------

We use semantic versioning to communicate when we make API changes to
the library. See CONTRIBUTING.md for more details on contributing to
stor.

.. |Build Status| image:: https://travis-ci.org/counsyl/stor.svg?branch=master
   :target: https://travis-ci.org/counsyl/stor
