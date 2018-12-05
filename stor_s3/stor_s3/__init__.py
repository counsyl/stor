from stor_s3.utils import is_s3_path
from stor_s3.s3 import S3Path


def class_for_path(prefix, path):
    if prefix+'://' != S3Path.drive:
        raise ValueError('Invalid prefix to initialize S3Paths: {}'.format(prefix))
    cls = S3Path
    return cls, path
