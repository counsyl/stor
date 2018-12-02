from dxpy.bindings import verify_string_dxid
from dxpy.exceptions import DXError


def is_dx_path(p):
    """Determines if the path is a DX path.

    All DX paths start with ``dx://``

    Args:
        p (str): The path string

    Returns
        bool: True if p is a DX path, False otherwise.
    """
    from stor_dx import DXPath
    return p.startswith(DXPath.drive)


def is_valid_dxid(dxid, expected_classes):
    """wrapper class for verify_string_dxid, because
    verify_string_dxid returns None if success, raises error if failed

    Args: Accepts same args as verify_string_dxid

    Returns
        bool: Whether given dxid is a valid path of one of expected_classes
    """
    try:
        return verify_string_dxid(dxid, expected_classes) is None
    except DXError:
        return False


def find_dx_class(p):
    """Finds the class of the DX path : DXVirtualPath or DXCanonicalPath

    Args:
        p (str): The path string

    Returns
        cls: DXVirtualPath or DXCanonicalPath
    """
    from stor_dx.dx import DXPath, DXCanonicalPath, DXVirtualPath
    colon_pieces = p[len(DXPath.drive):].split(':', 1)
    if not colon_pieces or not colon_pieces[0] or '/' in colon_pieces[0]:
        raise ValueError('Project is required to construct a DXPath')
    project = colon_pieces[0]
    resource = (colon_pieces[1] if len(colon_pieces) == 2 else '').lstrip('/')
    resource_parts = resource.split('/')
    root_name, rest = resource_parts[0], resource_parts[1:]
    canonical_resource = is_valid_dxid(root_name, 'file') or not resource
    if canonical_resource and rest:
        raise ValueError('DX folder paths that start with a valid file dxid are ambiguous')
    canonical_project = is_valid_dxid(project, 'project')

    if canonical_project and canonical_resource:
        return DXCanonicalPath
    else:
        return DXVirtualPath
