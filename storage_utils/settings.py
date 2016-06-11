import ast
from ConfigParser import SafeConfigParser
from contextlib import contextmanager
import storage_utils

CONFIG_FILE = 'default.cfg'
DELIMITER = '.'
BASE_NAME = 'stor'


def initialize():
    """
    Initialize settings from configuration file.

    TODO: Look in specific locations for user-specified files.
    """
    parser = SafeConfigParser()
    parser.readfp(open(CONFIG_FILE))

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
    global_settings directly.

    TODO: Allow specification of just a specific subset?
    (eg. get('swift')  or get('swift.upload') or get('swift', 'upload'))
    """
    return storage_utils.global_settings


def update(settings=None):
    """
    Updates global settings permanently.

    Input should be a dictionary with keys and values corresponding to different
    options.

    TODO:
    - Support inputting updated options as keyword arguments or in a more
      friendly format?
    - Do some sort of error checking when updating settings.
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
