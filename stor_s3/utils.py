import tempfile

import stor.utils as stor_utils


def is_s3_path(p):
    """Determines if the path is a S3 path.

    All S3 paths start with ``s3://``

    Args:
        p (str): The path string

    Returns
        bool: True if p is a S3 path, False otherwise.
    """
    from stor_s3.s3 import S3Path
    return p.startswith(S3Path.drive)
