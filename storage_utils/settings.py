import ast
import copy
from ConfigParser import SafeConfigParser
import os
import threading

CONFIG_FILE = 'default.cfg'

_global_settings = {}
thread_local = threading.local()


def initialize(filename=None):
    """
    Initialize global settings from configuration file.
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

    parser = SafeConfigParser()
    filename = filename or os.path.join(os.path.dirname(__file__), CONFIG_FILE)

    with open(filename) as fp:
        parser.readfp(fp)

    settings = {}

    for section in parser.sections():
        if parser.items(section):
            settings[section] = {}
            for item in parser.items(section):
                # Parse Python value from string
                try:
                    settings[section][item[0]] = ast.literal_eval(item[1])
                except (SyntaxError, ValueError):
                    settings[section][item[0]] = item[1]

    _global_settings.update(settings)


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
