import stor
from stor_dx import utils
from stor_swift import SwiftPath


swift_drive = SwiftPath.drive
dx_drive = stor.dx.DXPath.drive


def find_cls_for_path(prefix, path):
    if prefix+'://' != swift_drive:
        raise ValueError('Invalid prefix to shunt Swift paths: ({}). '
                         'Need a swift path to convert to DXPath'.format(prefix))
    new_path = convert_swift_path_to_dx(path)
    if not new_path:
        return SwiftPath, path
    cls = utils.find_dx_class(new_path)
    return cls, new_path


def convert_swift_path_to_dx(swift_p):
    parts = swift_p[len(swift_drive):].split('/')
    tenant = parts[0] if len(parts) > 0 and parts[0] else None
    container = parts[1] if len(parts) > 1 and parts[1] else None

    if not tenant or not container:
        return None

    if tenant == 'AUTH_dna' and container == 'nexus':
        resource_parts = parts[2:]
        if resource_parts and resource_parts[0]:
            resource_parts[0] = resource_parts[0].rstrip(':') + ':'
            dx_p = dx_drive + '/'.join(resource_parts)
            return dx_p
    return None
