from stor_swift.swift import SwiftPath


drive = SwiftPath.drive


def find_cls_for_path(prefix, path):
    if prefix+'://' != drive:
        raise ValueError('Invalid prefix to initialize SwiftPaths: {}'.format(prefix))
    cls = SwiftPath
    return cls, path
