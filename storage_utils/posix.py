"""
Provides additional functionality on top of the path.py package.
"""
from path import Path
from storage_utils import utils


class PosixPath(Path):
    def copy(self, destination, copy_cmd='cp -r', object_threads=20,
             segment_threads=20, **retry_args):
        """Copies the path to the given destination using cmd as the copy
        command (if the destination is a posix path).

        Args:
            destination (str|path): The posix or swift path to copy to.
            copy_cmd (str): The copy command to use if copying to a posix path.
            object_threads (int): The amount of object threads to use
                if copying to swift.
            segment_threads (int): The amount of segment threads to use
                if copying to swift.
            retry_args (dict): Optional retry arguments for swift. View the
                swift module-level documentation for information on
                retry arguments.
        """
        return utils.copy(self,
                          destination,
                          copy_cmd=copy_cmd,
                          segment_threads=segment_threads,
                          object_threads=object_threads,
                          **retry_args)
