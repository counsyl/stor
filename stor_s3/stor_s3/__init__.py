from stor_s3.s3 import S3Path


drive = S3Path.drive


def find_cls_for_path(prefix, path):
    if prefix+'://' != drive:
        raise ValueError('Invalid prefix to initialize S3Paths: {}'.format(prefix))
    cls = S3Path
    return cls, path
