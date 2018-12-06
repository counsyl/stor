from stor_swift.utils import is_swift_path
from stor_swift.swift import SwiftPath


def class_for_path(prefix, path):
    if prefix+'://' != SwiftPath.drive:
        raise ValueError('Invalid prefix to initialize SwiftPaths: {}'.format(prefix))
    cls = SwiftPath
    return cls, path
