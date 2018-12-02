from stor_dx.dx import DXPath
from stor_dx import utils


drive = DXPath.drive


def find_cls_for_path(prefix, path):
    if prefix+'://' != drive:
        raise ValueError('Invalid prefix to initialize DXPaths: {}'.format(prefix))
    cls = utils.find_dx_class(path)
    return cls, path
