Contributing
============

## Semantic Versioning


This project uses Semantic Versioning through PBR. This means when you make a
commit you can add a message like:

`Sem-Ver: feature - Added this functionality that does blah.`

Then the version will automatically get bumped in the right way when releasing
the package.

More info about how to use the Semantic Versioning through PBR:
http://docs.openstack.org/developer/pbr/#version

## Test Coverage

We try to maintain 100% test coverage on every merged pull request.  In
addition, we maintain a full suite of integration tests that run against S3 and
Swift (for now the Swift tests run within Counsyl's infrastructure, but we'd be
open to running tests against a Swift All-in-One docker image or other test
harness)

## Style Guidelines for stor


### tl;dr

* 4 spaces per tab
* 99 character line width
* All code should follow [Google style guide for Python][GoogleStyle]
  (except use 4 spaces per tab).


### Docstrings!

Every (important) function and class should have a good docstring that explains
what it does in a single sentence and then gives some explanation of its
arguments. We're using the [Google docstring format][GoogleDocString] because
it's easier to read directly in code
and [still can be formatted nicely in automated output][sphinxnapoleon].

E.g., here's the docstring for `open`.

```python
def open(self, mode='r', swift_upload_options=None):
    """Opens a `SwiftFile` that can be read or written to.

    For examples of reading and writing opened objects, view
    `SwiftFile`.

    Args:
        mode (str): The mode of object IO. Currently supports reading
            ("r" or "rb") and writing ("w", "wb")
        swift_upload_options (dict): DEPRECATED (use `stor.settings.use()`
            instead). A dictionary of arguments that will be
            passed as keyword args to `SwiftPath.upload` if any writes
            occur on the opened resource.

    Returns:
        SwiftFile: The swift object.

    Raises:
        SwiftError: A swift client error occurred.
    """
```

[GoogleDocString]: https://google-styleguide.googlecode.com/svn/trunk/pyguide.html?showone=Comments#Comments
[GoogleStyle]: https://google-styleguide.googlecode.com/svn/trunk/pyguide.html
[sphinxnapoleon]: http://sphinxcontrib-napoleon.readthedocs.org/en/latest/
