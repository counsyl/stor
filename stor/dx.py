from stor.obs import OBSPath


class DXPath(OBSPath):
    """
        Provides the ability to manipulate and access resources on swift
        with a similar interface to the path library.
        """
    drive = 'dx://'
