import ast
import copy
import os
import threading

from configparser import SafeConfigParser

CONFIG_FILE = 'default.cfg'
USER_CONFIG_FILE = '~/.stor.cfg'

_ENV_VARS = {
    'swift': {
        'username': 'OS_USERNAME',
        'password': 'OS_PASSWORD',
        'auth_url': 'OS_AUTH_URL',
        'temp_url_key': 'OS_TEMP_URL_KEY',
        'num_retries': 'OS_NUM_RETRIES'
    },
    'dx': {
        'auth_token': 'DX_AUTH_TOKEN',
        'file_proxy_url': 'DX_FILE_PROXY_URL',
    }
}
"""
A dictionary of options and their corresponding environment variables.

The top-level dictionary is a set of key-value pairs where keys correspond
to config sections and values are dictionaries where the keys are the option
names and values are the environment variables.s
"""

_global_settings = {}
thread_local = threading.local()


def _parse_config_val(value):
    try:
        return ast.literal_eval(value)
    except (SyntaxError, ValueError):
        return value


def _get_env_vars():
    """
    Update settings with environment variables, if applicable.

    Currently handles swift and dx credentials.
    """
    new_settings = {}
    for section in _ENV_VARS:
        options = {}
        for option in _ENV_VARS[section]:
            if _ENV_VARS[section][option] in os.environ:
                options[option] = _parse_config_val(os.environ.get(_ENV_VARS[section][option]))
        new_settings[section] = options
    update(new_settings)


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


def _initialize():
    """
    Initialize global settings from configuration file. The configuration file
    **must** define all required settings, otherwise `stor` will not work.
    Every time this function is called it overwrites existing ``_global_settings``.
    When trying to update or change ``_global_settings``, `update` or `use` should
    be called instead.

    Also looks in the user's home directory for a custom configuration
    specified in ``~/.stor.cfg``.

    Defaults to reading from the default configuration file ``default.cfg``.

    Args:
        filename (str): File to read initial default configuration settings from.

    Returns:
        None
    """
    _global_settings.clear()
    default_cfg = os.path.join(os.path.dirname(__file__), CONFIG_FILE)
    update(parse_config_file(default_cfg), validate=False)
    custom_cfg = os.path.expanduser(USER_CONFIG_FILE)
    if os.path.exists(custom_cfg):
        update(parse_config_file(custom_cfg))
    _get_env_vars()


def _update(d, updates, validate=True):
    """
    Updates a nested dictionary with given dictionary

    If validate is set to True, the key being updated must already exist
    in the dictionary.
    """
    for key, value in updates.items():
        if type(value) is dict:
            if key not in d or not type(d[key]) is dict:
                if validate:
                    raise ValueError('\'%s\' is not a valid setting' % key)
                d[key] = {}
            _update(d[key], value, validate)
        else:
            if validate and key not in d:
                raise ValueError('\'%s\' is not a valid setting' % key)
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


def update(settings=None,
           # not documented
           validate=True):
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
        _update(_global_settings, settings, validate=validate)


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
#:      >>> from stor import settings
#:      >>> with settings.use({'swift:upload': {'object_threads': 20}}):
#:      >>>     # do something here
use = _Use
