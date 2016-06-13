import ast
import copy
from ConfigParser import SafeConfigParser
from contextlib import contextmanager
import os
import storage_utils

CONFIG_FILE = 'default.cfg'
DELIMITER = '.'
BASE_NAME = 'stor'


def initialize(filename=None):
    """
    Initialize settings from configuration file.

    Defaults to reading from the default configuration file ``default.cfg``.

    Args:
        filename (str): File to read initial configuration settings from.

    Returns:
        dict: The configuration settings.
    """
    parser = SafeConfigParser()
    if filename:
        parser.readfp(open(filename))
    else:
        parser.readfp(open(os.path.join(os.path.dirname(__file__), CONFIG_FILE)))

    settings = {}

    for section in parser.sections():
        _build(settings, section, parser.items(section))

    return settings


def _build(settings, section, items):
    """Helper function for building a nested dictionary of settings"""
    new_dict = dict(items)

    for key, val in new_dict.iteritems():
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
    return copy.deepcopy(storage_utils.global_settings)


def update(settings=None):
    """
    Updates global settings permanently (in place).

    Arguments:
        settings (dict): A nested dictionary of settings options.

    Returns:
        None
    """
    if settings:
        _update(storage_utils.global_settings, settings)


@contextmanager
def use(settings=None):
    """
    Context manager for temporarily modifying settings.

    Arguments:
        settings (dict): A nested dictionary of settings options. If specified, the global
            settings will be updated with the specified settings. Otherwise,
            the global settings will be used.

    Yields:
        dict: A copy of the new settings

    Examples:
        >>> from storage_utils import settings
        >>> with settings.use({'swift': {'upload': {'object_threads': 5}}) as my_settings:
        >>>     # Do operations that use settings (eg. swift.upload() or swift.download())
        >>>     # Can also access settings values at my_settings,
        >>>     # eg. my_settings['swift']['upload']['object_threads']
    """
    copy_settings = get()
    update(settings=settings)
    yield get()
    update(settings=copy_settings)
