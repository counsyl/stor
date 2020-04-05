"""
SwiftStack Utilities
====================

Utilities specific to SwiftStack's implementation of Openstack Swift.

Use this module to translate swift paths to S3 paths.
"""
import stor
import hashlib


def swift_to_s3(swift_path, bucket):
    """SwiftStack Swift Path to S3 Cloud-Shunt Path

    Args:
        swift_path (str|Path): path to convert
        bucket (str): name of S3 bucket
    Returns:
        S3Path: the converted path

    See https://www.swiftstack.com/docs/admin/cluster_management/cloud_sync.html#swift-object-representation-in-s3 for details
    """  # noqa
    if not bucket:
        raise TypeError('bucket is required')
    swift_path = stor.Path(swift_path)
    h = hashlib.md5((u'%s/%s' % (swift_path.tenant, swift_path.container)
                     ).encode("utf8")).hexdigest()
    prefix = hex(int(h, 16) % 16**6).lstrip('0x').rstrip('L')
    pth = stor.join('s3://%s' % bucket, prefix, swift_path.tenant, swift_path.container)
    if swift_path.resource:
        pth = stor.join(pth, swift_path.resource)
    return pth


def s3_to_swift(s3_path):
    """S3 Cloud-Sync style path to SwiftStack Path

    Args:
        s3_path (str|Path): path to convert
    Returns:
        SwiftPath: the converted path
    """
    return stor.join('swift://', *stor.Path(s3_path).resource.split('/')[1:])
