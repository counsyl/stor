"""
The CLI can be accessed through the command ``stor``. For details on
valid subcommands, usage, and input options, refer to the ``--help`` / ``-h``
flag.

In addition to basic list, copy, and remove commands, the CLI also has some
features such as specifying a current working directory on an OBS service
(which allows for relative paths), ``cat``, and copying from ``stdin``.

Relative Paths
--------------

Using the CLI, the user can specify a current working directory on supported
OBS services (currently swift and s3) using the ``cd`` subcommand:

    >>> stor cd s3://bucket
    >>> stor cd swift://tenant/container

To check the current working directory, use the ``pwd`` subcommand:

    >>> stor pwd
    s3://bucket
    swift://tenant/container

To clear the current working directory, use the ``clear`` subcommand:

    >>> stor clear
    >>> stor pwd
    s3://
    swift://

This also means that relative paths can be used:

    >>> stor cd s3://bucket/dir
    >>> stor list s3://./child
    s3://bucket/dir/child/file1
    s3://bucket/dir/child/file2
    >>> stor list s3://..
    s3://bucket/a
    s3://bucket/b/obj1
    s3://bucket/dir/child/file1
    s3://bucket/dir/child/file2

``stdin`` and ``stdout``
------------------------

The CLI offers the ability to copy from ``stdin`` and output a path's
contents to ``stdout``.

To copy from ``stdin``, use the special ``-`` symbol. This means that the
user can pipe output from one command into the ``stor`` CLI:

    >>> echo "hello world" | stor cp - s3://my/file1

The user can also output a path's contents to ``stdout`` using the ``cat``
subcommand.

    >>> stor cat s3://my/file1
    hello world

Direct file transfer between OBS services or within one OBS service is not
yet supported, but can be accomplished using the two aforementioned features:

    >>> stor cat s3://my/file1 | stor cp - s3://my/file2
    >>> stor cat s3://my/file2
    hello world

"""

import argparse
import copy
import ConfigParser
from functools import partial
import os
import sys
import tempfile

import storage_utils
from storage_utils import exceptions
from storage_utils import settings
from storage_utils import Path
from storage_utils import utils

PRINT_CMDS = ('list', 'listdir', 'ls', 'cat', 'pwd', 'walkfiles')

ENV_FILE = os.path.expanduser('~/.stor-cli.env')
PKG_ENV_FILE = os.path.join(os.path.dirname(__file__), 'stor.env')


class TempPath(Path):
    """Persist stdin to a temporary file for CLI operations with OBS."""
    def __del__(self):
        os.remove(str(self))


def _get_env():
    """
    Get the current environment using the ENV_FILE.

    Returns a ConfigParser.
    """
    parser = ConfigParser.SafeConfigParser()
    # if env file doesn't exist, copy over the package default
    if not os.path.exists(ENV_FILE):
        with open(ENV_FILE, 'w') as outfile, open(PKG_ENV_FILE) as infile:
            outfile.write(infile.read())
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
            return parser.get('env', service)
        except ConfigParser.NoOptionError:
            raise ValueError('%s is an invalid service' % service)
    return [value for name, value in parser.items('env')]


def _env_chdir(pth):
    """Sets the new current working directory."""
    parser = _get_env()
    if utils.is_obs_path(pth):
        service = Path(pth).drive.rstrip(':/')
    else:
        raise ValueError('%s is an invalid path' % pth)
    if pth != Path(pth).drive:
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


def get_path(pth, mode=None):
    """
    Convert string to a Path type.

    The string ``-`` is a special string depending on mode.
    With mode 'r', it represents stdin and a temporary file is created and returned.
    """
    if pth == '-' and mode == 'r':
        ntf = tempfile.NamedTemporaryFile(delete=False)
        print ntf.name
        ntf.write(sys.stdin.read())
        ntf.close()
        return TempPath(ntf.name)
    p = Path(pth)
    # resolve relative paths
    if not p.isabs():
        if utils.is_obs_path(pth):
            service = p.drive.rstrip(':/')
        else:
            return p

        pwd = Path(_get_pwd(service=service))
        if pwd == p.drive:
            raise ValueError('No current directory specified for relative path \'%s\'' % pth)

        pwd = utils.remove_trailing_slash(pwd)
        path_part = pth[len(p.drive):]
        split_parts = path_part.split('/')
        rel_part = split_parts[0]

        prefix = pwd
        depth = 1
        if rel_part == '..':
            while split_parts[depth] == '..':
                depth += 1
            if len(pwd[len(pwd.drive):].split('/')) > depth:
                for i in range(0, depth):
                    prefix = prefix.parent
            else:
                raise ValueError('Relative path \'%s\' is invalid for current directory \'%s\''
                                 % (pth, pwd))
        p = prefix / path_part.split(rel_part, depth)[depth].lstrip('/')
    return p


def create_parser():
    # base parsers to hold commonly-used arguments and options
    # todo: is this a good practice?
    manifest_parser = argparse.ArgumentParser()
    manifest_parser.add_argument('-u', '--use_manifest',
                                 help='Validate that results are in the data manifest.',
                                 action='store_true')

    parser = argparse.ArgumentParser(description='A command line interface for storage-utils.')

    # todo: make default an environment variable?
    parser.add_argument('-c', '--config',
                        help='File containing configuration settings.',
                        type=str,
                        metavar='CONFIG_FILE')

    subparsers = parser.add_subparsers(dest='cmd', metavar='')

    list_msg = 'List contents using the path as a prefix.'
    parser_list = subparsers.add_parser('list',
                                        help=list_msg,
                                        description=list_msg,
                                        parents=[manifest_parser],
                                        conflict_handler='resolve')
    parser_list.add_argument('path', type=get_path, metavar='PATH')
    parser_list.add_argument('-s', '--starts_with',
                             help='Append an additional path to the search path.',
                             type=str,
                             metavar='PREFIX')
    parser_list.add_argument('-l', '--limit',
                             help='Limit the amount of results returned.',
                             type=int,
                             metavar='INT')
    parser_list.set_defaults(func=storage_utils.listpath)

    listdir_msg = 'List path as a directory.'
    parser_listdir = subparsers.add_parser('listdir',
                                           help=listdir_msg,
                                           description=listdir_msg)
    parser_listdir.add_argument('path', type=get_path, metavar='PATH')
    parser_listdir.set_defaults(func=storage_utils.listdir)

    ls_msg = 'Alias for listdir.'
    parser_ls = subparsers.add_parser('ls',  # noqa
                                      help=ls_msg,
                                      description='%s %s' % (ls_msg, listdir_msg),
                                      parents=[parser_listdir],
                                      conflict_handler='resolve')

    copytree_msg = 'Copy a source directory to a destination directory.'
    parser_copytree = subparsers.add_parser('copytree',
                                            description=copytree_msg,
                                            help=copytree_msg,
                                            parents=[manifest_parser],
                                            conflict_handler='resolve')
    parser_copytree.add_argument('source', type=get_path, metavar='SOURCE')
    parser_copytree.add_argument('dest', type=get_path, metavar='DEST')
    parser_copytree.set_defaults(func=storage_utils.copytree)

    copy_msg = 'Copy a source file to a destination path.'
    parser_copy = subparsers.add_parser('copy',
                                        help=copy_msg,
                                        description='%s\n \'-\' is a special character that allows'
                                                    ' for using stdin as the source.' % copy_msg)
    parser_copy.add_argument('source', type=partial(get_path, mode='r'), metavar='SOURCE')
    parser_copy.add_argument('dest', type=get_path, metavar='DEST')
    parser_copy.set_defaults(func=storage_utils.copy)
    cp_msg = 'Alias for copy.'
    parser_cp = subparsers.add_parser('cp',  # noqa
                                      help=cp_msg,
                                      description='%s %s' % (cp_msg, copy_msg),
                                      parents=[parser_copy],
                                      conflict_handler='resolve')

    remove_msg = 'Remove file at path.'
    parser_remove = subparsers.add_parser('remove', help=remove_msg, description=remove_msg)
    parser_remove.add_argument('path', type=get_path, metavar='PATH')
    parser_remove.set_defaults(func=storage_utils.remove)
    rm_msg = 'Alias for remove.'
    parser_rm = subparsers.add_parser('rm',  # noqa
                                      help=rm_msg,
                                      description='%s %s' % (rm_msg, remove_msg),
                                      parents=[parser_remove],
                                      conflict_handler='resolve')

    rmtree_msg = 'Remove a path and all its contents.'
    parser_rmtree = subparsers.add_parser('rmtree', help=rmtree_msg, description=rmtree_msg)
    parser_rmtree.add_argument('path', type=get_path, metavar='PATH')
    parser_rmtree.set_defaults(func=storage_utils.rmtree)

    walkfiles_msg = 'List all files under a path that match an optional pattern.'
    parser_walkfiles = subparsers.add_parser('walkfiles',
                                             help=walkfiles_msg,
                                             description=walkfiles_msg)
    parser_walkfiles.add_argument('-p', '--pattern',
                                  help='A regex pattern to match file names on.',
                                  type=str,
                                  metavar='REGEX')
    parser_walkfiles.add_argument('path', type=get_path, metavar='PATH')
    parser_walkfiles.set_defaults(func=storage_utils.walkfiles)

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
            sys.stderr.write('%s is not a valid command for the given input\n' % cmd)
            sys.exit(1)
        sys.stderr.write('%s is not a valid command for %s\n' % (cmd, value))
        sys.exit(1)
    except ValueError as exc:
        sys.stderr.write('Error: %s\n' % str(exc))
        sys.exit(1)
    except exceptions.RemoteError as exc:
        sys.stderr.write('%s: %s\n' % (exc.__class__.__name__, exc.message))
        sys.exit(1)


def print_results(results):
    if type(results) is str:
        sys.stdout.write(results)
        if not results.endswith('\n'):
            sys.stdout.write('\n')
    else:
        for result in results:
            sys.stdout.write('%s\n' % str(result))


def main():
    parser = create_parser()
    args = parser.parse_args()
    results = process_args(args)

    cmd = vars(args).get('cmd')
    if cmd in PRINT_CMDS:
        print_results(results)
