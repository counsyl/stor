"""
The CLI can be accessed through the command ``stor``. For details on
valid subcommands, usage, and input options, refer to the ``--help`` / ``-h``
flag.

In addition to UNIX-like basic list, copy, and remove commands, the CLI also
has some features such as specifying a current working directory on an OBS service
(which allows for relative paths), ``cat``, and copying from ``stdin``.

The ``list`` command is different from the ``ls`` command. ``list`` recursively
lists all files and directories under a given path while ``ls`` lists the path
as a directory in a way that is similar to the UNIX command.

To copy or remove a tree, use the ``-r`` flag with ``cp`` or ``remove``.

Relative Paths
--------------

Using the CLI, the user can specify a current working directory on supported
OBS services (currently swift and s3) using the ``cd`` subcommand::

    $ stor cd s3://bucket
    $ stor cd swift://tenant/container

To check the current working directory, use the ``pwd`` subcommand::

    $ stor pwd
    s3://bucket
    swift://tenant/container

To clear the current working directory, use the ``clear`` subcommand::

    $ stor clear
    $ stor pwd
    s3://
    swift://

This also means that relative paths can be used. Relative paths are indicated
by omitting the ``//`` in the path and instead indicating a relative path, as
shown::

    $ stor cd s3://bucket/dir
    $ stor list s3:child
    s3://bucket/dir/child/file1
    s3://bucket/dir/child/file2
    $ stor list s3:./child
    s3://bucket/dir/child/file1
    s3://bucket/dir/child/file2
    $ stor list s3:..
    s3://bucket/a
    s3://bucket/b/obj1
    s3://bucket/dir/child/file1
    s3://bucket/dir/child/file2

``stdin`` and ``stdout``
------------------------

The CLI offers the ability to copy from ``stdin`` and output a path's
contents to ``stdout``.

To copy from ``stdin``, use the special ``-`` symbol. This means that the
user can pipe output from one command into the ``stor`` CLI::

    $ echo "hello world" | stor cp - s3://my/file1

The user can also output a path's contents to ``stdout`` using the ``cat``
subcommand::

    $ stor cat s3://my/file1
    hello world

Direct file transfer between OBS services or within one OBS service (server-side copy)
is not yet supported.
"""

import argparse
import copy
import ConfigParser
from functools import partial
import logging
import os
import shutil
import signal
import sys
import tempfile

import stor
from stor import exceptions
from stor import settings
from stor import Path
from stor import utils

PRINT_CMDS = ('list', 'listdir', 'ls', 'cat', 'pwd', 'walkfiles')
SERVICES = ('s3', 'swift')

ENV_FILE = os.path.expanduser('~/.stor-cli.env')
PKG_ENV_FILE = os.path.join(os.path.dirname(__file__), 'default.env')


def perror(msg):
    """Print error message and exit."""
    sys.stderr.write(msg)
    sys.exit(1)


def force_exit(signum, frame):  # pragma: no cover
    sys.stderr.write(' Aborted\n')
    os._exit(1)

signal.signal(signal.SIGINT, force_exit)


class TempPath(Path):
    """Persist stdin to a temporary file for CLI operations with OBS."""
    def __del__(self):
        os.remove(str(self))


def _make_stdin_action(func, err_msg):
    """
    Return a StdinAction object that checks for stdin.
    func should be the function associated with the parser's -r flag
    that is not valid to use with stdin.
    """
    class StdinAction(argparse._StoreAction):
        def __call__(self, parser, namespace, values, option_string=None):
            if values == '-':
                if namespace.func == func:
                    raise argparse.ArgumentError(self, err_msg)
                else:
                    ntf = tempfile.NamedTemporaryFile(delete=False)
                    try:
                        ntf.write(sys.stdin.read())
                    finally:
                        ntf.close()
                    super(StdinAction, self).__call__(parser,
                                                      namespace,
                                                      TempPath(ntf.name),
                                                      option_string=option_string)
            else:
                super(StdinAction, self).__call__(parser,
                                                  namespace,
                                                  values,
                                                  option_string=option_string)
    return StdinAction


def _get_env():
    """
    Get the current environment using the ENV_FILE.

    Returns a ConfigParser.
    """
    parser = ConfigParser.SafeConfigParser()
    # if env file doesn't exist, copy over the package default
    if not os.path.exists(ENV_FILE):
        shutil.copyfile(PKG_ENV_FILE, ENV_FILE)
    with open(ENV_FILE) as fp:
        parser.readfp(fp)
    return parser


def _get_pwd(service=None):
    """
    Returns the present working directory for the given service,
    or all services if none specified.
    """
    parser = _get_env()
    if service:
        try:
            return utils.with_trailing_slash(parser.get('env', service))
        except ConfigParser.NoOptionError:
            raise ValueError('%s is an invalid service' % service)
    return [utils.with_trailing_slash(value) for name, value in parser.items('env')]


def _env_chdir(pth):
    """Sets the new current working directory."""
    parser = _get_env()
    if utils.is_obs_path(pth):
        service = Path(pth).drive.rstrip(':/')
    else:
        raise ValueError('%s is an invalid path' % pth)
    if pth != Path(pth).drive:
        if not Path(pth).isdir():
            raise ValueError('%s is not a directory' % pth)
        pth = utils.remove_trailing_slash(pth)
    parser.set('env', service, pth)
    with open(ENV_FILE, 'w') as outfile:
        parser.write(outfile)


def _clear_env(service=None):
    """Reset current working directory for the specified service or all if none specified."""
    parser = _get_env()
    if service:
        _env_chdir(service + '://')
    else:
        for name, value in parser.items('env'):
            _env_chdir(name + '://')


def _cat(pth):
    """Return the contents of a given path."""
    return pth.open().read()


def _obs_relpath_service(pth):
    """
    Check if path is an OBS relative path and if so,
    return the service, otherwise return an empty string.
    """
    prefixes = tuple(service + ':' for service in SERVICES)
    if pth.startswith(prefixes):
        if pth.startswith(tuple(p + '//' for p in prefixes)):
            return ''
        elif pth in prefixes or pth.startswith(tuple(p + '/' for p in prefixes)):
            raise ValueError('%s is an invalid path' % pth)
        parts = pth.split(':', 1)
        return parts[0]
    return ''


def get_path(pth, mode=None):
    """
    Convert string to a Path type.

    The string ``-`` is a special string depending on mode.
    With mode 'r', it represents stdin and a temporary file is created and returned.
    """
    service = _obs_relpath_service(pth)
    if not service:
        return Path(pth)

    relprefix = service + ':'

    pwd = Path(_get_pwd(service=service))
    if pwd == pwd.drive:
        raise ValueError('No current directory specified for relative path \'%s\'' % pth)

    pwd = utils.remove_trailing_slash(pwd)
    path_part = pth[len(relprefix):]
    split_parts = path_part.split('/')
    rel_part = split_parts[0]

    prefix = pwd
    depth = 1
    if rel_part == '..':
        # remove trailing slash otherwise we won't find the right parent
        prefix = utils.remove_trailing_slash(prefix)
        while len(split_parts) > depth and split_parts[depth] == '..':
            depth += 1
        if len(pwd[len(pwd.drive):].split('/')) > depth:
            for i in range(0, depth):
                prefix = prefix.parent
        else:
            raise ValueError('Relative path \'%s\' is invalid for current directory \'%s\''
                             % (pth, pwd))
    elif rel_part != '.':
        return prefix / path_part
    return prefix / path_part.split(rel_part, depth)[depth].lstrip('/')


def create_parser():
    parser = argparse.ArgumentParser(description='A command line interface for stor.')

    # todo: make default an environment variable?
    parser.add_argument('-c', '--config',
                        help='File containing configuration settings.',
                        type=str,
                        metavar='CONFIG_FILE')
    parser.add_argument('--version', help='Print version',
                        action='version',
                        version=stor.__version__)

    subparsers = parser.add_subparsers(dest='cmd', metavar='')

    list_msg = 'List contents using the path as a prefix.'
    parser_list = subparsers.add_parser('list',
                                        help=list_msg,
                                        description=list_msg)
    parser_list.add_argument('path', type=get_path, metavar='PATH')
    parser_list.add_argument('-s', '--starts-with',
                             help='Append an additional path to the search path.',
                             type=str,
                             dest='starts_with',
                             metavar='PREFIX')
    parser_list.add_argument('-l', '--limit',
                             help='Limit the amount of results returned.',
                             type=int,
                             metavar='INT')
    parser_list.set_defaults(func=stor.list)

    ls_msg = 'List path as a directory.'
    parser_ls = subparsers.add_parser('ls',  # noqa
                                      help=ls_msg,
                                      description=ls_msg)
    parser_ls.add_argument('path', type=get_path, metavar='PATH')
    parser_ls.set_defaults(func=stor.listdir)

    cp_msg = 'Copy a source to a destination path.'
    cp_msg = 'Alias for copy.'
    parser_cp = subparsers.add_parser('cp',  # noqa
                                      help=cp_msg,
                                      description='%s\n \'-\' is a special character that allows'
                                                  ' for using stdin as the source.' % cp_msg)
    parser_cp.add_argument('-r',
                           help='Copy a directory and its subtree to the destination directory.'
                                ' Must be specified before any other flags.',
                           action='store_const',
                           dest='func',
                           const=stor.copytree,
                           default=stor.copy)
    parser_cp.add_argument('source',
                           type=get_path,
                           metavar='SOURCE',
                           action=_make_stdin_action(stor.copytree,
                                                     '- cannot be used with -r'))
    parser_cp.add_argument('dest', type=get_path, metavar='DEST')

    rm_msg = 'Remove file at a path.'
    parser_rm = subparsers.add_parser('rm',
                                      help=rm_msg,
                                      description='%s Use the -r flag to remove a tree.' % rm_msg)
    parser_rm.add_argument('-r',
                           help='Remove a path and all its contents.',
                           action='store_const',
                           dest='func',
                           const=stor.rmtree,
                           default=stor.remove)
    parser_rm.add_argument('path', type=get_path, metavar='PATH')

    walkfiles_msg = 'List all files under a path that match an optional pattern.'
    parser_walkfiles = subparsers.add_parser('walkfiles',
                                             help=walkfiles_msg,
                                             description=walkfiles_msg)
    parser_walkfiles.add_argument('-p', '--pattern',
                                  help='A regex pattern to match file names on.',
                                  type=str,
                                  metavar='REGEX')
    parser_walkfiles.add_argument('path', type=get_path, metavar='PATH')
    parser_walkfiles.set_defaults(func=stor.walkfiles)

    cat_msg = 'Output file contents to stdout.'
    parser_cat = subparsers.add_parser('cat', help=cat_msg, description=cat_msg)
    parser_cat.add_argument('path', type=partial(get_path, mode='r'), metavar='PATH')
    parser_cat.set_defaults(func=_cat)

    cd_msg = 'Change directory to a given OBS path.'
    parser_cd = subparsers.add_parser('cd', help=cd_msg, description=cd_msg)
    parser_cd.add_argument('path', type=get_path, metavar='PATH')
    parser_cd.set_defaults(func=_env_chdir)

    pwd_msg = 'Get the present working directory of a service or all current directories.'
    parser_pwd = subparsers.add_parser('pwd', help=pwd_msg, description=pwd_msg)
    parser_pwd.add_argument('service', nargs='?', type=str, metavar='SERVICE')
    parser_pwd.set_defaults(func=_get_pwd)

    clear_msg = 'Clear current directories of a specified service.'
    parser_clear = subparsers.add_parser('clear', help=clear_msg,
                                         description='%s The current directories of all services'
                                         ' will be cleared if SERVICE is omitted.' % clear_msg)
    parser_clear.add_argument('service', nargs='?', type=str, metavar='SERVICE')
    parser_clear.set_defaults(func=_clear_env)

    return parser


def process_args(args):
    args_copy = copy.copy(vars(args))
    config = args_copy.pop('config', None)
    func = args_copy.pop('func', None)
    pth = args_copy.pop('path', None)
    cmd = args_copy.pop('cmd', None)

    if config:
        settings.update(settings.parse_config_file(config))
    func_kwargs = {
        key: Path(val) if type(val) is TempPath else val
        for key, val in args_copy.iteritems() if val
    }
    try:
        if pth:
            return func(pth, **func_kwargs)
        return func(**func_kwargs)
    except NotImplementedError:
        if pth:
            value = pth
        elif len(func_kwargs) > 0:
            value = func_kwargs.values()[0]
        else:
            perror('%s is not a valid command for the given input\n' % cmd)
        perror('%s is not a valid command for %s\n' % (cmd, value))
    except ValueError as exc:
        perror('Error: %s\n' % str(exc))
    except exceptions.RemoteError as exc:
        if type(exc) is exceptions.NotFoundError and pth:
            perror('Not Found: %s' % pth)
        perror('%s: %s\n' % (exc.__class__.__name__, exc.message))


def print_results(results):
    if type(results) is str:
        sys.stdout.write(results)
        if not results.endswith('\n'):
            sys.stdout.write('\n')
    else:
        for result in results:
            sys.stdout.write('%s\n' % str(result))


def main():
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    s3_logger = logging.getLogger('stor.s3.progress')
    s3_logger.setLevel(logging.INFO)
    s3_logger.addHandler(handler)
    swift_logger = logging.getLogger('stor.swift.progress')
    swift_logger.setLevel(logging.INFO)
    swift_logger.addHandler(handler)

    settings._initialize()
    parser = create_parser()
    args = parser.parse_args()
    results = process_args(args)

    cmd = vars(args).get('cmd')
    if cmd in PRINT_CMDS:
        print_results(results)
