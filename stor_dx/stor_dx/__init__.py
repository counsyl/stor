from stor_dx import dx
from stor_dx import utils


drive = dx.DXPath.drive


def find_cls_for_path(prefix, path):
    if prefix+'://' != drive:
        raise ValueError('Invalid prefix to initialize DXPaths: {}'.format(prefix))
    cls = utils.find_dx_class(path)
    return cls, path
