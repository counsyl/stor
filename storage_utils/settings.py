import ast
import copy
from ConfigParser import SafeConfigParser
import os
import threading

CONFIG_FILE = 'default.cfg'

_global_settings = {}
thread_local = threading.local()


def _parse_config_val(value):
    try:
        return ast.literal_eval(value)
    except (SyntaxError, ValueError):
        return value


def parse_config_file(filename):
    """
    Parses a configuration file and returns a settings dictionary.

    Args:
        filename (str): File to read configuration settings from.

    Returns:
        dict: A dictionary of settings options.
    """
    parser = SafeConfigParser()

    with open(filename) as fp:
        parser.readfp(fp)

    settings = {
        section: {
            item[0]: _parse_config_val(item[1])
            for item in parser.items(section)
        }
        for section in parser.sections()
    }

    return settings


def _initialize(filename=None):
    """
    Initialize global settings from configuration file. The configuration file
    **must** define all required settings, otherwise `storage_utils` will not work.
    Every time this function is called it overwrites existing ``_global_settings``.
    When trying to update or change ``_global_settings``, `update` or `use` should
    be called instead.

    Defaults to reading from the default configuration file ``default.cfg``.

    Args:
        filename (str): File to read initial default configuration settings from.

    Returns:
        None
    """
    global _global_settings
    _global_settings.clear()
    filename = filename or os.path.join(os.path.dirname(__file__), CONFIG_FILE)
    _global_settings.update(parse_config_file(filename))


def _update(d, updates):
    """Updates a nested dictionary with given dictionary"""
    for key, value in updates.iteritems():
        if type(value) is dict:
            if not type(d) is dict or key not in d:
                d[key] = {}
            _update(d[key], value)
        else:
            d[key] = value


def get():
    """
    Returns a deep copy of global settings as a dictionary.

    This function should always be used rather than accessing
    ``global_settings`` directly.
    """
    try:
        return copy.deepcopy(thread_local.settings)
    except AttributeError:
        return copy.deepcopy(_global_settings)


def update(settings=None):
    """
    Updates global settings permanently (in place).

    Arguments:
        settings (dict): A nested dictionary of settings options.

    Returns:
        None
    """
    if hasattr(thread_local, 'settings'):
        raise RuntimeError('update() cannot be called from within a settings context manager')
    if settings:
        _update(_global_settings, settings)


class _Use(object):
    """
    Context manager for temporarily modifying settings.
    """
    def __init__(self, settings=None):
        settings = settings or {}

        if hasattr(thread_local, 'settings'):
            self.old_settings = get()
        else:
            self.old_settings = None
        self.temp_settings = get()
        _update(self.temp_settings, settings)
        thread_local.settings = self.temp_settings

    def __enter__(self):
        pass

    def __exit__(self, type, value, traceback):
        if self.old_settings:
            thread_local.settings = self.old_settings
        else:
            del thread_local.settings

#: Context manager for temporarily modifying settings.
#:
#: Arguments:
#:   settings (dict): A nested dictionary of settings options.
#:
#: Example:
#:      >>> from storage_utils import settings
#:      >>> with settings.use({'swift:upload': {'object_threads': 20}}):
#:      >>>     # do something here
use = _Use
