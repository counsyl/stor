import ast
import copy
from ConfigParser import SafeConfigParser
import os
import threading

CONFIG_FILE = 'default.cfg'
DELIMITER = '.'
BASE_NAME = 'stor'

_global_settings = {}
thread_local = threading.local()


def initialize(filename=None):
    """
    Initialize global settings from configuration file.

    Defaults to reading from the default configuration file ``default.cfg``.

    Args:
        filename (str): File to read initial configuration settings from.

    Returns:
        None
    """
    global _global_settings
    _global_settings.clear()

    parser = SafeConfigParser()
    if filename:
        parser.readfp(open(filename))
    else:
        parser.readfp(open(os.path.join(os.path.dirname(__file__), CONFIG_FILE)))

    settings = {}

    for section in parser.sections():
        _build(settings, section, parser.items(section))

    _global_settings.update(settings)


def _build(settings, section, items):
    """Helper function for building a nested dictionary of settings"""
    new_dict = dict(items)

    for key, val in new_dict.iteritems():
        # Parse ConfigParser's string values into Python values
        try:
            new_dict[key] = ast.literal_eval(val)
        except (SyntaxError, ValueError):
            continue

    layers = section.split(DELIMITER)
    layers.reverse()
    if BASE_NAME in layers:
        layers.remove(BASE_NAME)
    for layer in layers:
        new_dict = {layer: new_dict}
    _update(settings, new_dict)


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


class Use(object):
    """
    Context manager for temporarily modifying settings.
    """
    def __init__(self, settings={}):
        if hasattr(thread_local, 'settings'):
            self.old_settings = get()
        else:
            self.old_settings = None
        self.temp_settings = get()
        _update(self.temp_settings, settings)
        thread_local.settings = self.temp_settings

    def __enter__(self):
        return copy.deepcopy(self.temp_settings)

    def __exit__(self, type, value, traceback):
        if self.old_settings:
            thread_local.settings = self.old_settings
        else:
            del thread_local.settings
