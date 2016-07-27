import argparse
import copy
from functools import partial
import os
import sys
import tempfile

import storage_utils
from storage_utils import exceptions
from storage_utils import settings
from storage_utils import Path

PRINT_CMDS = ('list', 'listdir', 'ls', 'cat')


class TempPath(Path):
    pass


def cat(pth):
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
        ntf.write(sys.stdin.read())
        ntf.close()
        print ntf.name
        return TempPath(ntf.name)
    return Path(pth)


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

    upload_msg = 'Upload a source path or multiple source paths to a destination path.'
    parser_upload = subparsers.add_parser('upload',
                                          help=upload_msg,
                                          description=upload_msg,
                                          parents=[manifest_parser],
                                          conflict_handler='resolve')
    parser_upload.add_argument('source',
                               nargs='+',
                               type=get_path,
                               metavar='SOURCE')
    parser_upload.add_argument('path',
                               nargs=1,
                               type=get_path,
                               metavar='DEST')
    parser_upload.set_defaults(func=storage_utils.upload)

    download_msg = 'Download a source directory to a destination directory.'
    parser_download = subparsers.add_parser('download',
                                            help=download_msg,
                                            description=download_msg,
                                            parents=[manifest_parser],
                                            conflict_handler='resolve')
    parser_download.add_argument('path', type=get_path, metavar='SOURCE')
    parser_download.add_argument('dest', type=get_path, metavar='DEST')
    parser_download.set_defaults(func=storage_utils.download)

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

    cat_msg = 'Output file contents to stdout.'
    parser_cat = subparsers.add_parser('cat', help=cat_msg, description=cat_msg)
    parser_cat.add_argument('path', type=partial(get_path, mode='r'), metavar='PATH')
    parser_cat.set_defaults(func=cat)

    return parser


def process_args(args):
    args_copy = copy.copy(vars(args))
    config = args_copy.pop('config', None)
    func = args_copy.pop('func', None)
    pth = args_copy.pop('path', None)
    cmd = args_copy.pop('cmd', None)
    if type(pth) is list:
        pth = pth[0]

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
        sys.stderr.write('%s is not a valid command for %s' % (cmd, pth))
        sys.exit(1)
    except exceptions.RemoteError as exc:
        sys.stderr.write('%s: %s\n' % (exc.__class__.__name__, exc.message))
        sys.exit(1)


def clean_tempfiles(args):
    for key, val in vars(args).iteritems():
        if type(val) is TempPath:
            os.remove(str(val))


def print_results(results):
    if type(results) is str:
        sys.stdout.write(results)
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

    clean_tempfiles(args)
