import posixpath
import boto3
from storage_utils.base import Path
from storage_utils.posix import PosixPath


class S3Path(Path):
    """
    Provides the ability to manipulate and access resources on Amazon S3
    with a similar interface to the path library.
    """
    s3_drive = 's3://'
    path_module = posixpath
    parts_class = PosixPath

    def __init__(self, s3):
        """
        Validates S3 path is in the proper format.

        Args:
            s3 (str): A path that matches the format of
                ``s3://{bucket_name}/{rest_of_path}``
                The ``s3://`` prefix is required in the path.
        """
        if not hasattr(s3, 'startswith') or not s3.startswith(self.s3_drive):
            raise ValueError('path must have %s (got %r)' % (self.s3_drive, s3))
        return super(S3Path, self).__init__(s3)

    def __repr__(self):
        return '%s("%s")' % (type(self).__name__, self)

    def _get_parts(self):
        """Returns the path parts (excluding s3://) as a list of strings."""
        if len(self) > len(self.s3_drive):
            return self[len(self.s3_drive):].split('/')
        else:
            return []

    @property
    def name(self):
        """The name of the path, mimicking path.py's name property"""
        return self.parts_class(super(S3Path, self).name)

    @property
    def parent(self):
        """The parent of the path, mimicking path.py's parent property"""
        return self.path_class(super(S3Path, self).parent)

    @property
    def bucket(self):
        """Returns the bucket name from the path or None"""
        parts = self._get_parts()
        return parts[0] if len(parts) > 0 and parts[0] else None

    @property
    def resource(self):
        """
        Returns the resource as a ``PosixPath`` object or None.

        A resource can be a single object or a prefix to objects.
        Note that it's important to keep the trailing slash in a resource
        name for prefix queries.
        """
        parts = self._get_parts()
        joined_resource = '/'.join(parts[1:]) if len(parts) > 1 else None

        return self.parts_class(joined_resource) if joined_resource else None
