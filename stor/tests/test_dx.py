import os
import pytest
import requests
import time
import unittest

import dxpy
import dxpy.bindings as dxb
from unittest import mock
from testfixtures import LogCapture

import stor
from stor import exceptions
from stor import Path
from stor import utils
from stor.dx import DXPath
import stor.dx as dx
from stor.test import DXTestCase
from stor.tests.shared_obs import SharedOBSFileCases


class TestBasicPathMethods(unittest.TestCase):
    def test_name(self):
        p = Path('dx://project:/path/to/resource')
        self.assertEqual(p.name, 'resource')

    def test_parent(self):
        p = Path('dx://project:/path/to/resource')
        self.assertEqual(p.parent, 'dx://project:/path/to')

    def test_dirname(self):
        p = Path('dx://project:/path/to/resource')
        self.assertEqual(p.dirname(), 'dx://project:/path/to')

    def test_dirname_top_level(self):
        p1 = Path('dx://project')
        self.assertEqual(p1.dirname(), 'dx://project')

        p2 = Path('dx://project:/')
        self.assertEqual(p2.dirname(), 'dx://project:/')

    def test_basename(self):
        p = Path('dx://project:/path/to/resource')
        self.assertEqual(p.basename(), 'resource')


class TestRepr(unittest.TestCase):
    def test_repr(self):
        # import so we can eval it
        from stor.dx import DXVirtualPath

        DXVirtualPath  # get around unused warning

        dx_p = DXPath('dx://t:/c/p')
        self.assertEqual(eval(repr(dx_p)), dx_p)


class TestPathManipulations(unittest.TestCase):
    def test_add(self):
        dx_p = DXPath('dx://a:')
        dx_p = dx_p + 'b' + Path('c')
        self.assertTrue(isinstance(dx_p, DXPath))
        self.assertEqual(dx_p, 'dx://a:bc')

    def test_div(self):
        dx_p = DXPath('dx://t:')
        dx_p = dx_p / 'c' / Path('p')
        self.assertTrue(isinstance(dx_p, DXPath))
        self.assertEqual(dx_p, 'dx://t:/c/p')


class TestProject(unittest.TestCase):
    def test_project_none(self):
        with pytest.raises(ValueError, match='Project is required'):
            DXPath('dx://')

    def test_project_exists(self):
        dx_p = DXPath('dx://project')
        self.assertEqual(dx_p.project, 'project')
        dx_p = DXPath('dx://project:')
        self.assertEqual(dx_p.project, 'project')
        dx_p = DXPath('dx://project:file')
        self.assertEqual(dx_p.project, 'project')
        dx_p = DXPath('dx://project:/')
        self.assertEqual(dx_p.project, 'project')


class TestResource(unittest.TestCase):

    def test_resource_none_w_project(self):
        dx_p = DXPath('dx://project:/')
        self.assertIsNone(dx_p.resource)

    def test_resource_object(self):
        dx_p = DXPath('dx://project:/obj')
        self.assertEqual(dx_p.resource, 'obj')

    def test_resource_trailing_slash(self):
        dx_p = DXPath('dx://project:/dir/')
        self.assertEqual(dx_p.resource, 'dir/')

    def test_resource_nested_obj(self):
        dx_p = DXPath('dx://project:/nested/obj.txt')
        self.assertEqual(dx_p.resource, 'nested/obj.txt')

    def test_resource_nested_dir(self):
        dx_p = DXPath('dx://project:/nested/dir/')
        self.assertEqual(dx_p.resource, 'nested/dir/')


class TestCompatHelpers(unittest.TestCase):
    def test_noops(self):
        self.assertEqual(DXPath('dx://project:/folder').expanduser(),
                         DXPath('dx://project:/folder'))
        self.assertEqual(DXPath('dx://project:').abspath(),
                         DXPath('dx://project:'))
        self.assertEqual(DXPath('dx://project:/folder/file').realpath(),
                         DXPath('dx://project:/folder/file'))

    @mock.patch.dict(os.environ, {'somevar': 'blah'}, clear=True)
    def test_expand(self):
        original = DXPath('dx://project:/$somevar//another/../a/')
        self.assertEqual(original.expand(),
                         DXPath('dx://project:/blah/a'))
        self.assertEqual(DXPath('dx://project://a/b').expand(),
                         DXPath('dx://project:/a/b'))

    def test_expandvars(self):
        original = DXPath('dx://project:/$somevar/another')
        other = DXPath('dx://project:/somevar/another')
        with mock.patch.dict(os.environ, {'somevar': 'blah'}, clear=True):
            expanded = original.expandvars()
            expanded2 = other.expandvars()
        self.assertEqual(expanded,
                         DXPath('dx://project:/blah/another'))
        self.assertEqual(expanded2, other)

    def test_normpath(self):
        self.assertEqual(DXPath('dx://project:/another/../b').normpath(),
                         DXPath('dx://project:/b'))
        # how projects should be normalized
        self.assertEqual(DXPath('dx://project:/..').normpath(),
                         DXPath('dx://project:/'))
        self.assertEqual(DXPath('dx://project:/folder/..').normpath(),
                         DXPath('dx://project:/'))
        self.assertEqual(DXPath('dx://project:dir///..///').normpath(),
                         DXPath('dx://project:/'))
        self.assertEqual(DXPath('dx://project:///..//..///..').normpath(),
                         DXPath('dx://project:/'))
        self.assertEqual(DXPath('dx://project:/a/b').normpath(),
                         DXPath('dx://project:/a/b'))
        self.assertEqual(DXPath('dx://project:a/b').normpath(),
                         DXPath('dx://project:/a/b'))
        self.assertEqual(DXPath('dx://project:/').normpath(),
                         DXPath('dx://project:/'))
        self.assertEqual(DXPath('dx://project:').normpath(),
                         DXPath('dx://project:/'))
        # ../ on root should not change project
        self.assertEqual(DXPath(
            'dx://project-123456789012345678901234:../file-123456789012345678901234'
        ).normpath(),
            DXPath(
            'dx://project-123456789012345678901234:file-123456789012345678901234'))
        # leading slash removed from canonical paths
        self.assertEqual(DXPath(
            'dx://project-123456789012345678901234:/file-123456789012345678901234'
        ).normpath(),
            DXPath(
            'dx://project-123456789012345678901234:file-123456789012345678901234'))
        self.assertEqual(DXPath('dx://project-123456789012345678901234:/').normpath(),
                         DXPath('dx://project-123456789012345678901234:'))


class TestRename(DXTestCase):
    def test_rename_project_fail(self):
        dx_p = DXPath('dx://Random_Project:/')
        with pytest.raises(ValueError, match='cannot be renamed'):
            dx_p._rename('RandomProject2:')

    def test_rename_folder_fail(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        with pytest.raises(exceptions.NotFoundError, match='No data object was found'):
            dx_p._rename('folder')

    def test_rename_file_pass(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file')
        dx_p._rename('folder_file.txt')
        new_dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file.txt')
        self.assertTrue(new_dx_p.exists())

    def test_rename_to_self(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file')
        dx_p._rename('folder_file')
        self.assertTrue(dx_p.exists())


class TestOpen(DXTestCase):
    # we need to give enough time for file to enter `closed` state so we can read it
    DX_WAIT_SETTINGS = {'dx': {'wait_on_close': 20}}

    def test_append_mode_fail(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project + ':temp_file')
        with pytest.raises(ValueError, match='invalid mode'):
            dx_p.open(mode='a')

    def test_read_on_open_buffer(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file')
        with dx_p.open() as f:
            content = f.read()
        self.assertEqual(content, 'data')
        with dx_p.open(mode='rb') as f:
            content = f.read()
        self.assertEqual(content, b'data')
        self.assertEqual(dx_p.read_object(), b'data')

    def test_read_fail_on_closed_buffer(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file')
        f = dx_p.open()
        f.close()
        with pytest.raises(ValueError, match='closed file'):
            f.read()

    def test_read_on_open_dx_file(self):
        self.setup_temporary_project()
        dxpy.new_dxfile(name='temp_file',
                        folder='/',
                        project=self.proj_id)
        dx_p = DXPath('dx://' + self.project + ':/temp_file')
        with pytest.raises(dx.DNAnexusError, match='Cannot read from file'):
            dx_p.open().read()

    def test_write_read_over_files(self):
        self.setup_temporary_project()
        data = b'''\
line1
line2
line3
line4
'''
        dx_p = DXPath('dx://' + self.project + ':temp_file')
        with stor.settings.use(self.DX_WAIT_SETTINGS):
            with dx_p.open(mode='wb') as f:
                f.write(data)
        # open().read() should return str for r
        self.assertEqual(dx_p.open('r').read(), data.decode('ascii'))
        # open().read() should return bytes for rb
        self.assertEqual(dx_p.open('rb').read(), data)
        self.assertEqual(dx_p.open().readlines(),
                         [l + '\n' for l in data.decode('ascii').split('\n')][:-1])
        for i, line in enumerate(dx_p.open(), 1):
            self.assertEqual(line, 'line%d\n' % i)

        self.assertEqual(next(dx_p.open()), 'line1\n')
        self.assertEqual(next(iter(dx_p.open())), 'line1\n')

    def test_write_multiple_wo_context_manager(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project + ':/temp_file')
        obj = dx_p.open(mode='wb')
        obj.write(b'hello')
        obj.write(b' world')
        with stor.settings.use(self.DX_WAIT_SETTINGS):
            obj.close()
        self.assertEqual(b'hello world', dx_p.read_object())

    def test_write_multiple_flush_multiple_upload(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project + ':/temp_file')
        with stor.settings.use(self.DX_WAIT_SETTINGS):
            with dx_p.open(mode='wb') as obj:
                obj.write(b'hello')
                obj.flush()
                obj.write(b' world')
                obj.flush()
        self.assertEqual(dx_p.open().read(), 'hello world')

    def test_read_dir_fail(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/file'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        with pytest.raises(exceptions.NotFoundError, match='No data object'):
            dx_p.open().read()
        dx_p = DXPath('dx://' + self.project)
        with pytest.raises(ValueError, match='Can only.*file paths not project paths'):
            dx_p.open().read()
        with pytest.raises(ValueError, match='not a project'):
            dx_p.read_object()

    def test_write_to_project_fail(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project)
        with pytest.raises(ValueError, match='Can only.*file paths not project paths'):
            dx_p.open(mode='wb')
        with pytest.raises(ValueError, match='Cannot write to project'):
            dx_p.write_object(b'data')

    def test_write_w_settings_no_timeout(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project + ':/temp_file')
        with stor.settings.use({'dx': {'wait_on_close': 0}}):
            with dx_p.open(mode='wb') as obj:
                obj.write(b'hello world')
        with pytest.raises(dx.DNAnexusError, match='closed'):
            dx_p.open().read()
        time.sleep(30)  # wait for file to go to closed
        self.assertEqual(dx_p.open().read(), 'hello world')

    def test_write_w_settings_big_timeout(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project + ':/temp_file')
        with stor.settings.use({'dx': {'wait_on_close': 30}}):
            with dx_p.open(mode='wb') as obj:
                obj.write(b'hello world')
        self.assertEqual(dx_p.open().read(), 'hello world')

    def test_valid_and_invalid_encoding_for_dnanexus(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project + ':/temp_file')
        with pytest.raises(ValueError, match='encoding is always assumed to be utf-8'):
            dx_p.open(encoding="ascii")
        with dx_p.open('w', encoding="utf-8") as fp:
            fp.write('text')


class TestDXOBSFile(SharedOBSFileCases, unittest.TestCase):
    drive = 'dx://project:'  # project is required for DX paths
    path_class = DXPath
    normal_path = DXPath('dx://project:/obj')

    def setUp(self):
        super(TestDXOBSFile, self).setUp()
        patcher = mock.patch('stor.obs.OBSFile._wait_on_close')
        self.addCleanup(patcher.stop)
        patcher.start()

    def test_makedirs_p_does_nothing(self):
        # skipping dumb test in superClass SharedOBSFileCases...
        # because our project doesn't exist on DNAnexus and makedirs_p DOES do something
        # on DNAnexus. Hence, why not to have dumb tests.
        pass


class TestCanonicalProject(DXTestCase):
    def test_no_project(self):
        dx_p = DXPath('dx://Random_Project:/')
        with pytest.raises(dx.ProjectNotFoundError, match='no projects'):
            dx_p.canonical_project

    def test_unique_project(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://'+self.project)
        self.assertTrue(utils.is_valid_dxid(dx_p.canonical_project, 'project'))

    def test_duplicate_projects(self):
        self.setup_temporary_project()
        test_proj = dxb.DXProject()
        test_proj.new(self.project)
        self.addCleanup(test_proj.destroy)
        dx_p = DXPath('dx://' + self.project)
        with pytest.raises(dx.MultipleObjectsSameNameError, match='Found more than one project'):
            dx_p.canonical_project

    def test_canonical_path(self):
        self.setup_temporary_project()
        f = self.setup_file('/folder_file')
        dx_p = DXPath('dx://' + self.proj_id + ':' + f.get_id())
        self.assertEqual(dx_p.canonical_project, self.proj_id)
        self.assertEqual(dx_p.canonical_resource, f.get_id())
        self.assertEqual(dx_p.canonical_path, dx_p)
        self.assertEqual(dx_p.virtual_project, self.project)
        self.assertEqual(dx_p.virtual_resource, 'folder_file')
        dx_virtual_p = DXPath('dx://' + self.project + ':/folder_file')
        self.assertEqual(dx_p.virtual_path, dx_virtual_p)

    def test_canonical_path_on_dir(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        dx_p.makedirs_p()
        self.assertEqual(dx_p.canonical_project, self.proj_id)
        with pytest.raises(exceptions.NotFoundError, match='No data object was found'):
            dx_p.canonical_path


class TestCanonicalResource(DXTestCase):
    def test_no_resource(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project + ':/random.txt')
        with pytest.raises(exceptions.NotFoundError, match='No data object was found'):
            dx_p.canonical_resource

    def test_unique_resource(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_file.txt'])
        dx_p = DXPath('dx://'+self.project + ':/temp_file.txt')
        self.assertTrue(utils.is_valid_dxid(dx_p.canonical_resource, 'file'))

    def test_duplicate_resource(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_folder/folder_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file.txt')
        with pytest.raises(dx.MultipleObjectsSameNameError, match='Multiple objects found'):
            dx_p.canonical_resource

    def test_project(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project)
        self.assertIsNone(dx_p.canonical_resource)

    def test_virtual_on_virtual(self):
        self.setup_temporary_project()
        self.setup_files(['/folder_file'])
        dx_p = DXPath('dx://' + self.proj_id + ':/folder_file')
        self.assertEqual(dx_p.virtual_path, dx_p)
        self.assertEqual(dx_p.virtual_resource, 'folder_file')
        self.assertEqual(dx_p.virtual_project, self.project)


class TestListDir(DXTestCase):
    def test_listdir_project(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_file.txt'])
        dx_p = DXPath('dx://'+self.project)
        results = dx_p.listdir()
        self.assert_dx_lists_equal(results, [
            'dx://'+self.project+':/temp_folder',
            'dx://'+self.project+':/temp_file.txt'
        ])

    def test_listdir_file(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_file.txt'])
        dx_p = DXPath('dx://'+self.project+':/temp_file.txt')
        with pytest.raises(exceptions.NotFoundError, match='specified folder'):
            dx_p.listdir()

    def test_listdir_empty_folder(self):
        self.setup_temporary_project()
        self.project_handler.new_folder('/temp_folder')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        results = dx_p.listdir()
        self.assertEqual(results, [])

    def test_listdir_folder_w_file(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_folder/temp_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        results = dx_p.listdir()
        self.assert_dx_lists_equal(results, [
            'dx://' + self.project + ':/temp_folder/temp_file.txt',
            'dx://' + self.project + ':/temp_folder/folder_file.txt'
        ])

    def test_listdir_absent_folder(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project + ':/random_folder')
        with pytest.raises(exceptions.NotFoundError, match='specified folder'):
            dx_p.listdir()

    def test_listdir_folder_share_filename(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_folder'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        results = dx_p.listdir()
        self.assert_dx_lists_equal(results, [
            'dx://' + self.project + ':/temp_folder/folder_file.txt'
        ])

    def test_listdir_canonical(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_file.txt'])
        dx_p = DXPath('dx://' + self.project)
        results = dx_p.listdir(canonicalize=True)
        self.assertIn('dx://'+self.proj_id+':/temp_folder', results)
        self.assertEqual(len(results), 2)

    def test_listdir_on_canonical_project(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_file.txt'])
        dx_p = DXPath('dx://' + self.proj_id)
        results = dx_p.listdir()
        self.assert_dx_lists_equal(results, [
            'dx://' + self.project + ':/temp_folder',
            'dx://' + self.project + ':/temp_file.txt'
        ])

    def test_listdir_on_canonical_resource(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_file.txt').canonical_path
        with pytest.raises(exceptions.NotFoundError, match='specified folder'):
            dx_p.listdir()

    def test_listdir_iter_project(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_file.txt'])
        dx_p = DXPath('dx://' + self.project)
        results = list(dx_p.listdir_iter())
        self.assert_dx_lists_equal(results, [
            'dx://' + self.project + ':/temp_folder',
            'dx://' + self.project + ':/temp_file.txt'
        ])

    def test_listdir_iter_canon_on_canon(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_folder/random/temp_file.txt'])
        dx_p = DXPath('dx://' + self.proj_id + ':/temp_folder')
        results = list(dx_p.listdir_iter(canonicalize=True))
        self.assertIn('dx://' + self.proj_id + ':/temp_folder/random', results)
        self.assertEqual(len(results), 2)


class TestList(DXTestCase):
    def test_list_project(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_file.txt'])
        dx_p = DXPath('dx://' + self.project)
        results = dx_p.list()
        self.assert_dx_lists_equal(results, [
            'dx://'+self.project+':/temp_folder/folder_file.txt',
            'dx://'+self.project+':/temp_file.txt'
        ])

    def test_list_file(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_file.txt'])
        dx_p = DXPath('dx://'+self.project+':/temp_file.txt')
        results = dx_p.list()
        self.assertEqual(results, [])

    def test_list_empty_folder(self):
        self.setup_temporary_project()
        self.project_handler.new_folder('/temp_folder')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        results = dx_p.list()
        self.assertEqual(results, [])

    def test_list_folder_w_files(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_folder/temp_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        results = dx_p.list()
        self.assert_dx_lists_equal(results, [
            'dx://' + self.project + ':/temp_folder/temp_file.txt',
            'dx://' + self.project + ':/temp_folder/folder_file.txt'
        ])

    def test_list_absent_folder(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project + ':/random_folder')
        results = dx_p.list()
        self.assertEqual(results, [])

    def test_list_folder_share_filename(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_folder'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        results = dx_p.list()
        self.assert_dx_lists_equal(results, [
            'dx://' + self.project + ':/temp_folder/folder_file.txt'
        ])

    def test_list_canonical(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_file.txt'])
        dx_p = DXPath('dx://' + self.project)
        results = dx_p.list(canonicalize=True)
        self.assertTrue(all(self.proj_id in result for result in results))
        self.assertEqual(len(results), 2)

    def test_list_limit(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_file.txt'])
        dx_p = DXPath('dx://' + self.project)
        results = dx_p.list(limit=1)
        self.assertEqual(len(results), 1)

    def test_list_starts_with(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_file.txt'])
        dx_p = DXPath('dx://' + self.project)
        results = dx_p.list(starts_with='temp_folder')
        self.assert_dx_lists_equal(results, [
            'dx://'+self.project+':/temp_folder/folder_file.txt'
        ])

    def test_list_w_condition(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_file.txt'])
        dx_p = DXPath('dx://' + self.project)
        results = dx_p.list(starts_with='temp_folder', condition=lambda res: len(res) == 1)
        self.assert_dx_lists_equal(results, [
            'dx://' + self.project + ':/temp_folder/folder_file.txt'
        ])

    def test_list_fail_w_condition(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_file.txt'])
        dx_p = DXPath('dx://' + self.project)
        with pytest.raises(exceptions.ConditionNotMetError, match='not met'):
            dx_p.list(condition=lambda res: len(res) == 1)

    def test_list_w_category(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_file.txt'])
        dx_p = DXPath('dx://' + self.project)
        dxpy.new_dxworkflow(title='Workflow', project=self.proj_id)
        results = dx_p.list(classname='file')
        self.assert_dx_lists_equal(results, [
            'dx://' + self.project + ':/temp_folder/folder_file.txt',
            'dx://' + self.project + ':/temp_file.txt'
        ])

    def test_list_on_canonical_project(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_file.txt'])
        dx_p = DXPath('dx://' + self.proj_id)
        results = dx_p.list()
        self.assert_dx_lists_equal(results, [
            'dx://' + self.project + ':/temp_folder/folder_file.txt',
            'dx://' + self.project + ':/temp_file.txt'
        ])

    def test_list_on_canonical_resource(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_file.txt').canonical_path
        results = dx_p.list()
        self.assertEqual(results, [])

    def test_list_iter_project(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_file.txt'])
        dx_p = DXPath('dx://' + self.project)
        results = list(dx_p.list_iter())
        self.assert_dx_lists_equal(results, [
            'dx://' + self.project + ':/temp_folder/folder_file.txt',
            'dx://' + self.project + ':/temp_file.txt'
        ])

    def test_list_iter_canon_on_canon(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_folder/random/temp_file.txt'])
        dx_p = DXPath('dx://' + self.proj_id + ':/temp_folder')
        results = list(dx_p.list_iter(canonicalize=True))
        self.assertTrue(all(self.proj_id in result for result in results))
        self.assertTrue(all('temp_folder' not in result for result in results))
        self.assertEqual(len(results), 2)


class TestWalkFiles(DXTestCase):
    def test_pattern_w_prefix(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_file.txt'])
        dx_p = DXPath('dx://' + self.project)
        results = list(dx_p.walkfiles(pattern='fold*'))
        self.assert_dx_lists_equal(results, [
            'dx://'+self.project+':/temp_folder/folder_file.txt'
        ])

    def test_pattern_w_suffix(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.csv',
                          '/temp_file.txt'])
        dx_p = DXPath('dx://'+self.project)
        results = list(dx_p.walkfiles(pattern='*.txt'))
        self.assert_dx_lists_equal(results, [
            'dx://' + self.project + ':/temp_file.txt'
        ])

    def test_pattern_w_prefix_suffix(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.csv',
                          '/temp_file.txt'])
        dx_p = DXPath('dx://'+self.project)
        results = list(dx_p.walkfiles(pattern='*file*'))
        self.assert_dx_lists_equal(results, [
            'dx://' + self.project + ':/temp_file.txt',
            'dx://' + self.project + ':/temp_folder/folder_file.csv'
        ])

    def test_pattern_share_folder_match(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.csv',
                          '/temp_folder.txt'])
        dx_p = DXPath('dx://'+self.project)
        results = list(dx_p.walkfiles(pattern='temp_folder*'))
        self.assert_dx_lists_equal(results, [
            'dx://' + self.project + ':/temp_folder.txt'
        ])

    def test_pattern_no_match(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.csv'])
        dx_p = DXPath('dx://'+self.project)
        results = list(dx_p.walkfiles(pattern='*temp*'))
        self.assertEqual(results, [])


class TestStat(DXTestCase):
    def test_stat_folder(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.csv'])
        with pytest.raises(ValueError, match='Invalid operation'):
            DXPath('dx://'+self.project+':/temp_folder/').stat()
        with pytest.raises(exceptions.NotFoundError, match='No data object was found'):
            DXPath('dx://'+self.project+':/temp_folder').stat()

    def test_stat_project_error(self):
        self.setup_temporary_project()  # creates project with name in self.project
        test_proj = dxb.DXProject()
        test_proj.new(self.project)  # creates duplicate project
        self.addCleanup(test_proj.destroy)
        with pytest.raises(dx.MultipleObjectsSameNameError, match='Found more than one project'):
            DXPath('dx://'+self.project+':').stat()
        with pytest.raises(dx.MultipleObjectsSameNameError, match='Found more than one project'):
            DXPath('dx://'+self.project+':/').stat()
        with pytest.raises(exceptions.NotFoundError, match='Found no projects'):
            DXPath('dx://Random_Proj:').stat()

    def test_stat_virtual_project(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://'+self.project)
        response = dx_p.stat()
        self.assertIn('region', response)  # only projects have regions
        dx_p = DXPath('dx://'+self.project+':')
        response = dx_p.stat()
        self.assertIn('region', response)

    def test_stat_canonical_project(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://'+self.proj_id+':')
        response = dx_p.stat()
        self.assertIn('region', response)

    def test_stat_file(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_file.txt'])
        dx_p = DXPath('dx://'+self.project+':/temp_file.txt')
        response = dx_p.stat()
        self.assertIn('folder', response)  # only files have folders
        dx_p = DXPath('dx://'+self.project+':/temp_folder/folder_file.txt')
        response = dx_p.stat()
        self.assertIn('folder', response)

    def test_stat_canonical_resource(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_file.txt'])
        dx_p = DXPath('dx://'+self.project+':/temp_file.txt').canonical_path
        response = dx_p.stat()
        self.assertIn('folder', response)  # only files have folders


class TestExists(DXTestCase):
    def test_false_file(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://'+self.project+':/random.txt')
        result = dx_p.exists()
        self.assertFalse(result)

    def test_false_folder(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project + ':/random_folder')
        result = dx_p.exists()
        self.assertFalse(result)

    def test_true_file(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file.txt')
        result = dx_p.exists()
        self.assertTrue(result)

    def test_true_empty_dir(self):
        self.setup_temporary_project()
        self.project_handler.new_folder('/temp_folder')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        result = dx_p.exists()
        self.assertTrue(result)
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/')
        result = dx_p.exists()
        self.assertTrue(result)
        self.assertFalse(dx_p.isfile())

    def test_true_dir_with_object(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        result = dx_p.exists()
        self.assertTrue(result)

    def test_project_does_not_exist(self):
        dx_p = DXPath('dx://random_project:/')
        result = dx_p.exists()
        self.assertFalse(result)

    def test_true_canonical_project(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.proj_id)
        result = dx_p.exists()
        self.assertTrue(result)

    def test_false_canonical_project(self):
        dx_p = DXPath('dx://project-123456789012345678901234')
        result = dx_p.exists()
        self.assertFalse(result)

    def test_true_canonical_path(self):
        self.setup_temporary_project()
        fh = self.setup_file('/folder_file.txt')
        dx_p = DXPath('dx://' + self.proj_id + ':/' + fh.get_id())
        result = dx_p.exists()
        self.assertTrue(result)

    def test_false_canonical_path(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.proj_id + ':/file-123456789012345678901234')
        result = dx_p.exists()
        self.assertFalse(result)
        fh = self.setup_file('/folder_file.txt')
        dx_p = DXPath('dx://project-123456789012345678901234:/' + fh.get_id())
        result = dx_p.exists()
        self.assertFalse(result)

    def test_true_mixed_path(self):
        self.setup_temporary_project()
        self.setup_files(['/folder_file.txt'])
        dx_p = DXPath('dx://' + self.proj_id + ':/folder_file.txt')
        result = dx_p.exists()
        self.assertTrue(result)

    def test_false_mixed_path(self):
        self.setup_temporary_project()
        self.setup_files(['/folder_file.txt'])
        dx_p = DXPath('dx://' + self.proj_id + ':/random_file')
        result = dx_p.exists()
        self.assertFalse(result)
        dx_p = DXPath('dx://project-123456789012345678901234:/folder_file.txt')
        result = dx_p.exists()
        self.assertFalse(result)


class TestGlob(DXTestCase):
    def test_suffix_pattern(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_folder/temp_file.csv'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        results = dx_p.glob('*.txt')
        self.assert_dx_lists_equal(results, [
            'dx://' + self.project + ':/temp_folder/folder_file.txt'
        ])

    def test_prefix_pattern(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_folder/file.csv'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        results = dx_p.glob('file*')
        self.assert_dx_lists_equal(results, [
            'dx://' + self.project + ':/temp_folder/file.csv'
        ])

    def test_valid_pattern(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_folder/temp_file.csv'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        results = dx_p.glob('*fi*le*')
        self.assert_dx_lists_equal(results, [
            'dx://' + self.project + ':/temp_folder/folder_file.txt',
            'dx://' + self.project + ':/temp_folder/temp_file.csv'
        ])

    def test_valid_pattern_wo_wildcard(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_folder/temp_file.csv'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        results = dx_p.glob('file')
        self.assertEqual(results, [])

    def test_pattern_no_file_match(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.csv',
                          '/random_file.txt'])
        dx_p = DXPath('dx://'+self.project)
        results = dx_p.glob('*temp*')
        self.assertEqual(results, [])

    def test_glob_cond_met(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        results = dx_p.glob('*fi*le*', condition=lambda res: len(res) == 1)
        self.assert_dx_lists_equal(results, [
            'dx://' + self.project + ':/temp_folder/folder_file.txt'
        ])

    def test_cond_no_met(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_folder/temp_file.csv'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        with pytest.raises(exceptions.ConditionNotMetError, match='not met'):
            dx_p.glob('*fi*le*', condition=lambda res: len(res) == 1)


class TestTempUrl(DXTestCase):
    def test_fail_on_project(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project)
        with pytest.raises(ValueError, match='DX Projects'):
            dx_p.temp_url()

    def test_fail_on_folder(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/')
        with pytest.raises(ValueError, match='Invalid operation'):
            dx_p.temp_url()
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        with pytest.raises(exceptions.NotFoundError, match='No data object was found'):
            dx_p.temp_url()

    def test_on_file(self):
        self.setup_temporary_project()
        self.setup_file('/temp_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_file.txt')
        actual_url = dx_p.temp_url()
        self.assertIn('dl.dnanex.us', actual_url)
        self.assertIn('temp_file.txt', actual_url)

    def test_on_file_canonical(self):
        self.setup_temporary_project()
        self.setup_file('/temp_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_file.txt').canonical_path
        actual_url = dx_p.temp_url()
        self.assertIn('dl.dnanex.us', actual_url)
        self.assertIn('temp_file.txt', actual_url)
        actual_url = dx_p.temp_url(filename='')
        self.assertIn('dl.dnanex.us', actual_url)
        self.assertNotIn('temp_file.txt', actual_url)

    def test_on_file_named_timed(self):
        self.setup_temporary_project()
        self.setup_file('/temp_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_file.txt')
        actual_url = dx_p.temp_url(filename='random.txt', lifetime=1)
        self.assertIn('dl.dnanex.us', actual_url)
        self.assertIn('random.txt', actual_url)
        url = requests.get(actual_url)
        self.assertIn('attachment', url.headers['content-disposition'])
        self.assertIn('random.txt', url.headers['content-disposition'])
        # to allow for max of 2s for link to expire
        time.sleep(2)
        r = requests.get(actual_url)
        assert not r.ok

    def _test_proxy_url(self, proxy_path):
        with stor.settings.use({"dx": {"file_proxy_url": proxy_path}}):
            dx_p = DXPath(f'dx://{self.project}:/temp_file.txt')
            actual_url = dx_p.temp_url()
            assert actual_url == dx_p.to_url()
            dx_p_canonical = DXPath(f'dx://{self.project}:/temp_file.txt').canonical_path
            actual_url2 = dx_p_canonical.temp_url()
            assert actual_url == actual_url2
            dx_p_mixed = DXPath(f'dx://{dx_p_canonical.canonical_project}:/temp_file.txt')
            # use filename version
            actual_url_mixed = dx_p_mixed.temp_url(filename='temp_file.txt')
            assert actual_url_mixed == actual_url
            for pth in [dx_p, dx_p_canonical, dx_p_mixed]:
                with pytest.raises(ValueError, match='filename MUST match object name'):
                    dx_p.temp_url(filename='another_name.txt')

    def test_on_file_with_proxy_url(self):
        self.setup_temporary_project()
        self.setup_file('/temp_file.txt')
        self._test_proxy_url("https://myproxy.example.com/dnax-gateway")
        # trailing slash
        self._test_proxy_url("https://myproxy.example.com/dnax-gateway/")
        # no path - no slash
        self._test_proxy_url("https://myproxy.example.com")
        # no component - slash
        self._test_proxy_url("https://myproxy.example.com/")
        # non-https
        self._test_proxy_url("http://myproxy.example.com/")

    def test_invalid_file_proxy_url_errors(self):
        with stor.settings.use({"dx": {"file_proxy_url": 'htp://some-invalid-path'}}):
            with pytest.raises(ValueError, match='``file_proxy_url`` must be an http.s. path'):
                stor.Path('dx://proj:/path').temp_url()


class TestRemove(DXTestCase):
    def test_remove_file(self):
        self.setup_temporary_project()
        f = self.setup_file('/temp_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_file.txt')
        dx_p.remove()
        with self.assertRaises(dxpy.exceptions.ResourceNotFound):
            f.describe()

    def test_fail_remove_folder(self):
        self.setup_temporary_project()
        self.project_handler.new_folder('/temp_folder')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        with pytest.raises(exceptions.NotFoundError, match='No data object was found'):
            dx_p.remove()

    def test_fail_remove_project(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project)
        with pytest.raises(ValueError, match='can only be called on single object'):
            dx_p.remove()

    def test_fail_remove_nonexistent_file(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project + ':/temp_file.txt')
        with pytest.raises(exceptions.NotFoundError, match='No data object was found'):
            dx_p.remove()

    def test_fail_rmtree_file(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_file.txt')
        with pytest.raises(exceptions.NotFoundError, match='No folders were found'):
            dx_p.rmtree()

    def test_rmtree_folder(self):
        self.setup_temporary_project()
        self.project_handler.new_folder('/temp_folder')
        f = self.setup_file('/temp_folder/temp_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        dx_p.rmtree()
        with self.assertRaises(dxpy.exceptions.ResourceNotFound):
            f.describe()
        proj_path = DXPath('dx://' + self.project)
        self.assertNotIn(dx_p, proj_path.listdir())

    def test_rmtree_project(self):
        self.setup_temporary_project()
        self.project_handler.new_folder('/temp_folder')
        self.setup_files(['/temp_file.txt',
                          '/folder2/file'])
        proj_path = DXPath('dx://' + self.project)
        proj_path.rmtree()
        self.assertEqual(len(proj_path.listdir()), 0)
        self.assertTrue(proj_path.exists())

    def test_fail_remove_nonexistent_project(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://RandomProject:/')
        with pytest.raises(exceptions.NotFoundError, match='Found no projects'):
            dx_p.rmtree()


class TestMakedirsP(DXTestCase):
    def test_makedirs_p_project(self):
        with pytest.raises(ValueError, match='Cannot create a project'):
            DXPath('dx://project:').makedirs_p()

    def test_makedirs_p_project_exists(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project)
        dx_p.makedirs_p()
        self.assertTrue(dx_p.exists())

    def test_makedirs_p_folder(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        dx_p.makedirs_p()
        proj_path = DXPath('dx://' + self.project)
        result = proj_path.listdir()
        self.assert_dx_lists_equal(result, [dx_p])

    def test_makedirs_p_nested_folder(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/nested_folder')
        dx_p.makedirs_p()
        folder_path = DXPath('dx://' + self.project + ':/temp_folder')
        proj_path = DXPath('dx://' + self.project)
        result = proj_path.listdir()
        self.assert_dx_lists_equal(result, [folder_path])
        result = folder_path.listdir()
        self.assert_dx_lists_equal(result, [dx_p])

    def test_makedirs_p_folder_exists(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        dx_p.makedirs_p()
        proj_path = DXPath('dx://' + self.project)
        result = proj_path.listdir()
        self.assert_dx_lists_equal(result, [dx_p])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/nested_folder')
        dx_p.makedirs_p()
        folder_path = DXPath('dx://' + self.project + ':/temp_folder')
        result = folder_path.listdir()
        self.assert_dx_lists_equal(result, [dx_p])


class TestDownloadObjects(DXTestCase):
    def test_local_paths(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/file1.txt')
        self.setup_file('/temp_folder/file2.txt')
        self.setup_file('/temp_folder/folder/file3.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        posix_folder_p = Path('./{test_folder}'.format(
            test_folder=self.project))
        self.addCleanup(posix_folder_p.rmtree)
        r = dx_p.download_objects(self.project,
                                  ['file1.txt',
                                   'file2.txt',
                                   'folder/file3.txt'])
        self.assertEquals(r, {
            'file1.txt': self.project + '/file1.txt',
            'file2.txt': self.project + '/file2.txt',
            'folder/file3.txt': self.project + '/folder/file3.txt'
        })

    def test_absolute_paths(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/file1.txt')
        self.setup_file('/temp_folder/file2.txt')
        self.setup_file('/temp_folder/folder/file3.txt')
        dx_p = DXPath('dx://' + self.project)
        posix_folder_p = Path('./{test_folder}'.format(
            test_folder=self.project))
        self.addCleanup(posix_folder_p.rmtree)
        r = dx_p.download_objects(self.project, [
            'dx://' + self.project + ':/temp_folder/file1.txt',
            'dx://' + self.project + ':/temp_folder/file2.txt',
            'dx://' + self.project + ':/temp_folder/folder/file3.txt'])
        self.assertEquals(r, {
            'dx://' + self.project + ':/temp_folder/file1.txt':
                self.project + '/temp_folder/file1.txt',
            'dx://' + self.project + ':/temp_folder/file2.txt':
                self.project + '/temp_folder/file2.txt',
            'dx://' + self.project + ':/temp_folder/folder/file3.txt':
                self.project + '/temp_folder/folder/file3.txt'
        })

    def test_absolute_paths_not_child_of_download_path(self):
        dx_p = DXPath('dx://project:/folder')
        with pytest.raises(ValueError, match='child'):
            dx_p.download_objects('output_dir', [
                'dx://project:/bad/e/f.txt',
                'dx://project:/bad/e/f/g.txt'
            ])


class TestCopy(DXTestCase):
    def test_clone_move_project_fail(self):
        self.setup_temporary_project()
        self.setup_file('/folder_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/folder_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_copy_project_fail.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_copy_project_fail.TempProj:/')
        with pytest.raises(ValueError, match='Cannot clone project'):
            proj_p._clone(dx_p)
        with pytest.raises(ValueError, match='Cannot move project'):
            proj_p._move(dx_p)

    def test_clone_within_project_fail(self):
        self.setup_temporary_project()
        self.setup_file('/folder_file.txt')
        self.setup_file('/folder_file2.txt')
        dx_p = DXPath('dx://' + self.project + ':/folder_file.txt')
        dx_p2 = DXPath('dx://' + self.project + ':/folder_file2.txt')
        with pytest.raises(dx.DNAnexusError, match='Cannot clone'):
            dx_p._clone(dx_p2)

    def test_move_diff_project_fail(self):
        self.setup_temporary_project()
        self.setup_file('/folder_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_move_diff_project_fail.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_move_diff_project_fail.TempProj:/folder_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/folder_file.txt')
        with pytest.raises(dx.DNAnexusError, match='Cannot move'):
            dx_p._move(proj_p)

    def test_dx_to_posix_file(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file.txt')
        posix_folder_p = Path('./{test_folder}'.format(
            test_folder=self.project))
        self.addCleanup(posix_folder_p.rmtree)
        posix_p = Path('./{test_folder}/{path}'.format(
            test_folder=self.project, path='random.txt'))
        dx_p.copy(posix_p)
        self.assertTrue(posix_p.exists())

    def test_dx_to_posix_folder(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file.txt')
        posix_folder_p = Path('./{test_folder}/'.format(
            test_folder=self.project))  # with trailing slash
        self.addCleanup(posix_folder_p.rmtree)
        dx_p.copy(posix_folder_p)
        posix_p = Path('./{test_folder}/{path}'.format(
            test_folder=self.project, path='folder_file.txt'))
        self.assertTrue(posix_p.exists())

    def test_dx_to_posix_file_folder_no_ext(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file.txt')
        posix_folder_p = Path('./{test_folder}'.format(
            test_folder=self.project))
        self.addCleanup(posix_folder_p.rmtree)
        posix_p = Path('./{test_folder}/{path}'.format(
            test_folder=self.project, path='random'))
        dx_p.copy(posix_p)  # when folder without ext doesn't exist
        self.assertTrue(posix_p.exists())
        self.assertFalse(posix_p.isdir())
        false_posix_p = Path('./{test_folder}/{path}'.format(
            test_folder=self.project, path='random/folder_file.txt'))
        self.assertFalse(false_posix_p.exists())
        dx_p.copy(posix_folder_p)  # when folder without ext already exists
        true_posix_p = Path('./{test_folder}/{path}'.format(
            test_folder=self.project, path='folder_file.txt'))
        self.assertTrue(true_posix_p.exists())

    def test_posix_to_dx_file(self):
        self.setup_temporary_project()
        self.setup_posix_files(['random.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file.txt')
        posix_p = Path('./{test_folder}/{path}'.format(
            test_folder=self.project, path='random.txt'))
        posix_p.copy(dx_p)
        self.assertTrue(dx_p.exists())

    def test_posix_to_dx_fail(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project + ':/')
        posix_p = Path('./{test_folder}/{path}'.format(
            test_folder=self.project, path='random'))
        with pytest.raises(exceptions.NotFoundError, match='provide a valid source'):
            posix_p.copy(dx_p)

    def test_posix_to_existing_dx(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/file.txt')
        self.setup_posix_files(['/rand/file.txt'])
        dx_folder_p = DXPath('dx://' + self.project + ':/temp_folder')
        posix_p = Path('./{test_folder}/{path}'.format(
            test_folder=self.project, path='rand/file.txt'))
        posix_p.copy(dx_folder_p)
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/file.txt')
        self.assertTrue(dx_p.exists())
        time.sleep(10)  # to give the newly uploaded file time to go to closed state
        with stor.open(dx_p, 'r') as uploaded_file:
            self.assertEqual(uploaded_file.read(), 'data0')

    def test_posix_to_dx_folder(self):
        self.setup_temporary_project()
        self.setup_posix_files(['/rand/random.txt'])
        dx_folder_p = DXPath('dx://' + self.project + ':/temp_folder/')  # with trailing slash
        posix_p = Path('./{test_folder}/{path}'.format(
            test_folder=self.project, path='rand/random.txt'))
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/random.txt')
        posix_p.copy(dx_folder_p)
        self.assertTrue(dx_p.exists())

    def test_posix_to_dx_file_folder_no_ext(self):
        self.setup_temporary_project()
        self.setup_posix_files(['random.txt'])
        posix_p = Path('./{test_folder}/{path}'.format(
            test_folder=self.project, path='random.txt'))
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file')
        posix_p.copy(dx_p)  # when folder without ext doesn't exist
        self.assertTrue(dx_p.exists())
        self.assertFalse(dx_p.isdir())
        false_dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file/random.txt')
        self.assertFalse(false_dx_p.exists())
        dx_folder_p = DXPath('dx://' + self.project + ':/temp_folder')
        posix_p.copy(dx_folder_p)  # when folder without ext already exists
        true_dx_p = DXPath('dx://' + self.project + ':/temp_folder/random.txt')
        self.assertTrue(true_dx_p.exists())

    def test_dx_dir_to_posix_error(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file.txt')
        posix_folder_p = Path('./{test_folder}'.format(
            test_folder=self.project))
        self.addCleanup(posix_folder_p.rmtree)
        posix_p = Path('./{test_folder}/{path}'.format(
            test_folder=self.project, path='random.txt'))
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        with pytest.raises(exceptions.NotFoundError, match='No data object'):
            dx_p.copy(posix_p)
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/')
        with pytest.raises(ValueError, match='Invalid operation'):
            dx_p.copy(posix_p)

    def test_dx_to_dx_file(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_to_dx_file.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_dx_to_dx_file.TempProj:/random.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file.txt')
        dx_p.copy(proj_p)
        self.assertTrue(proj_p.exists())

    def test_dx_to_dx_folder(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_to_dx_folder.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_dx_to_dx_folder.TempProj:/random/')  # with trailing slash
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file.txt')
        dx_p.copy(proj_p)
        expected_p = DXPath('dx://test_dx_to_dx_folder.TempProj:/random/folder_file.txt')
        self.assertTrue(expected_p.exists())

    def test_dx_to_dx_file_folder_no_ext(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_to_dx_file_folder_no_ext.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_dx_to_dx_file_folder_no_ext.TempProj:/random/file')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file.txt')
        dx_p.copy(proj_p)  # when folder without ext doesn't exist
        expected_p = DXPath('dx://test_dx_to_dx_file_folder_no_ext.TempProj:/random/file')
        self.assertTrue(expected_p.exists())
        self.assertFalse(expected_p.isdir())
        false_dx_p = DXPath('dx://test_dx_to_dx_file_folder_no_ext.TempProj:/random/file'
                            '/folder_file.txt')
        self.assertFalse(false_dx_p.exists())
        proj_p.remove()  # since same file cannot be copied to project twice
        proj_p = DXPath('dx://test_dx_to_dx_file_folder_no_ext.TempProj:/random')
        dx_p.copy(proj_p)  # when folder without ext already exists
        true_dx_p = DXPath('dx://test_dx_to_dx_file_folder_no_ext.TempProj:/random/'
                           'folder_file.txt')
        self.assertTrue(true_dx_p.exists())

    def test_dx_to_dx_within_project_fail(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file.txt')

        new_dx_p = DXPath('dx://' + self.project + ':/another_folder/file.txt')
        with pytest.raises(dx.DNAnexusError, match='same project'):
            dx_p.copy(new_dx_p, raise_if_same_project=True)

        new_dx_p = DXPath('dx://' + self.project + ':/another_folder/')
        with pytest.raises(dx.DNAnexusError, match='same project'):
            dx_p.copy(new_dx_p, raise_if_same_project=True)

    def test_dx_to_dx_same_project_exist_dest(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/another_folder/random_file.txt',
                          '/another_folder/folder_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file.txt')
        new_dx_p = DXPath('dx://' + self.project + ':/another_folder/random_file.txt')
        dx_p.copy(new_dx_p)
        self.assertTrue(new_dx_p.exists())
        new_dx_p.copy(dx_p)  # restore to original state
        new_dx_p = DXPath('dx://' + self.project + ':/another_folder')
        dx_p.copy(new_dx_p)
        self.assertTrue(new_dx_p.exists())

    def test_dx_to_same_dx_pass(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file.txt')
        dx_p.copy(dx_p)
        self.assertTrue(dx_p.exists())

    def test_dx_to_dx_diff_project_exist_file(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt'])
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_to_dx_diff_project_fail.TempProj')
        proj_handler.new_folder('/folder2')
        with dxpy.new_dxfile(name='folder_file.txt',
                             folder='/folder2',
                             project=proj_handler.get_id()) as f:
            f.write(b'data')
        # to allow for max of 20s for file state to go to closed
        f.wait_on_close(20)
        with dxpy.new_dxfile(name='dest_file.txt',
                             project=proj_handler.get_id()) as f:
            f.write(b'data')
        f.wait_on_close(20)
        self.addCleanup(proj_handler.destroy)
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file.txt')

        proj_p = DXPath('dx://test_dx_to_dx_diff_project_fail.TempProj:/dest_file.txt')
        dx_p.copy(proj_p)
        self.assertTrue(proj_p.exists())

        proj_p = DXPath('dx://test_dx_to_dx_diff_project_fail.TempProj:/folder2')
        dx_p.copy(proj_p)
        self.assertTrue(proj_p.exists())

    def test_dx_to_dx_within_project_pass(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/file.txt')
        new_dx_p = DXPath('dx://' + self.project + ':/another_folder/fold_file.txt')
        stor.copy(dx_p, new_dx_p)
        self.assertTrue(new_dx_p.exists())
        self.assertFalse(dx_p.exists())
        new_folder_dx_p = DXPath('dx://' + self.project + ':/another_folder/')
        with pytest.raises(exceptions.NotFoundError, match='No data object'):
            dx_p.copy(new_folder_dx_p)
        new_dx_p.copy(dx_p)  # restoring to original state
        dx_p.copy(new_folder_dx_p)
        new_dx_p = DXPath('dx://' + self.project + ':/another_folder/file.txt')
        self.assertFalse(dx_p.exists())
        self.assertTrue(new_dx_p.exists())

    def test_dx_to_other_obs(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/file.txt')
        obs_p = Path('swift://tenant/container/folder/file.txt')
        with pytest.raises(ValueError, match='cannot copy'):
            dx_p.copy(obs_p)

    def test_other_obs_to_dx(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/file.txt')
        obs_p = Path('swift://tenant/container/folder/file.txt')
        with pytest.raises(ValueError, match='cannot copy'):
            obs_p.copy(dx_p)

    def test_dx_canonical_to_dx_file(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/temp_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_canonical_to_dx_file.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_dx_canonical_to_dx_file.TempProj:/random.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/temp_file.txt'
                      ).canonical_path
        dx_p.copy(proj_p)
        self.assertTrue(proj_p.exists())


class TestCopyTree(DXTestCase):
    def test_clonetree_within_project_fail(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file.txt')
        self.setup_file('/temp_folder2/folder_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        dx_p2 = DXPath('dx://' + self.project + ':/temp_folder2')
        with pytest.raises(dx.DNAnexusError, match='Cannot clonetree'):
            dx_p._clonetree(dx_p2)

    def test_move_diff_project_fail(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folderfolder_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_move_diff_project_fail.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_move_diff_project_fail.TempProj:/temp_folder2')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        with pytest.raises(dx.DNAnexusError, match='Cannot movetree'):
            dx_p._movetree(proj_p)

    def test_move_root_within_project_fail(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt'])
        dx_p = DXPath('dx://' + self.project)
        dx_p2 = DXPath('dx://' + self.project + ':/temp_folder')
        with pytest.raises(dx.DNAnexusError, match='Cannot move root folder'):
            dx_p._movetree(dx_p2)

    def test_nonexistent_dir(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file.txt')
        with pytest.raises(exceptions.NotFoundError, match='No project or directory was found'):
            dx_p.copytree(dx_p)

    def test_dx_file_to_posix(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/file.txt')
        posix_p = Path('./{test_folder}/'.format(
            test_folder=self.project))
        with pytest.raises(dx.DNAnexusError, match='not found'):
            dx_p.copytree(posix_p)

    def test_dx_dir_to_posix(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file.txt')
        self.setup_file('/temp_folder/another_folder/temp_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/')
        posix_folder_p = Path('./{test_folder}/'.format(
            test_folder=self.project))
        self.addCleanup(posix_folder_p.rmtree)
        dx_p.copytree(posix_folder_p)
        posix_p = Path('./{test_folder}/{path}'.format(
            test_folder=self.project, path='folder_file.txt'))
        self.assertTrue(posix_p.exists())

        posix_folder_p_2 = Path('./{test_folder}/folder.txt'.format(
            test_folder=self.project))
        dx_p.copytree(posix_folder_p_2)
        posix_p = Path('./{test_folder}/{path}'.format(
            test_folder=self.project, path='folder.txt/another_folder/temp_file.txt'))
        self.assertTrue(posix_p.exists())

        with pytest.raises(dx.DNAnexusError, match='already exists'):
            dx_p.copytree(posix_folder_p_2)  # folder already exists

    def test_posix_file_to_dx(self):
        self.setup_temporary_project()
        self.setup_posix_files(['random.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/')
        posix_p = Path('./{test_folder}/{path}'.format(
            test_folder=self.project, path='random.txt'))
        with pytest.raises(OSError, match='Not a directory'):
            posix_p.copytree(dx_p)

    def test_posix_dir_to_dx(self):
        self.setup_temporary_project()
        self.setup_posix_files(['/folder/file.txt',
                                '/folder/file2.txt'])
        Path('./{test_folder}/{path}'.format(
            test_folder=self.project, path='/folder/folder2')).makedirs_p()
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        posix_p = Path('./{test_folder}/{path}'.format(
            test_folder=self.project, path='folder/'))
        posix_p.copytree(dx_p)
        dx_file_p = DXPath('dx://' + self.project + ':/temp_folder/file.txt')
        self.assertTrue(dx_file_p.exists())
        dx_folder_p = DXPath('dx://' + self.project + ':/temp_folder/folder2')
        self.assertTrue(dx_folder_p.exists())

    def test_posix_dir_to_dx_nested(self):
        self.setup_temporary_project()
        self.setup_posix_files(['/folder/file.txt',
                                '/folder/file2.txt'])
        Path('./{test_folder}/{path}'.format(
            test_folder=self.project, path='/folder/folder2')).makedirs_p()
        posix_p = Path('./{test_folder}/{path}'.format(
            test_folder=self.project, path='folder/'))
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/')
        posix_p.copytree(dx_p)
        dx_file_p = DXPath('dx://' + self.project + ':/temp_folder/folder/file.txt')
        self.assertTrue(dx_file_p.exists())
        dx_folder_p = DXPath('dx://' + self.project + ':/temp_folder/folder/folder2')
        self.assertTrue(dx_folder_p.exists())

    def test_posix_dir_to_dx_existing(self):
        self.setup_temporary_project()
        self.setup_files(['/existing_folder/file'])
        self.setup_posix_files(['/folder/file.txt',
                                '/folder/file2.txt'])
        Path('./{test_folder}/{path}'.format(
            test_folder=self.project, path='/folder/folder2')).makedirs_p()
        posix_p = Path('./{test_folder}/{path}'.format(
            test_folder=self.project, path='folder'))
        dx_p = DXPath('dx://' + self.project + ':/existing_folder')
        posix_p.copytree(dx_p)
        dx_file_p = DXPath('dx://' + self.project + ':/existing_folder/folder/file.txt')
        self.assertTrue(dx_file_p.exists())
        dx_folder_p = DXPath('dx://' + self.project + ':/existing_folder/folder/folder2')
        self.assertTrue(dx_folder_p.exists())

    def test_posix_dir_to_dx_fail(self):
        self.setup_temporary_project()
        self.setup_posix_files(['/folder/file.txt',
                                '/folder/file2.txt'])
        self.setup_files(['/temp_folder/folder/file'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')  # already exists
        posix_p = Path('./{test_folder}/{path}'.format(
            test_folder=self.project, path='folder/'))
        with pytest.raises(exceptions.TargetExistsError, match='will not cause duplicate folders'):
            posix_p.copytree(dx_p)

    def test_dx_to_other_obs(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/')
        obs_p = Path('swift://tenant/container/folder/')
        with pytest.raises(ValueError, match='cannot copy'):
            dx_p.copytree(obs_p)

    def test_other_obs_to_dx(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/')
        obs_p = Path('swift://tenant/container/folder/')
        with pytest.raises(ValueError, match='cannot copy'):
            obs_p.copytree(dx_p)

    def test_dx_dir_to_dx_dir(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file')
        self.setup_file('/temp_folder/another_folder/temp_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_dir_to_dx_dir.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_dx_dir_to_dx_dir.TempProj:/random.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        dx_p.copytree(proj_p)
        proj_file_p = DXPath('dx://test_dx_dir_to_dx_dir.TempProj:/random.txt/folder_file')
        self.assertTrue(proj_p.isdir())
        self.assertTrue(proj_file_p.exists())

    def test_dx_dir_to_dx_existing_dir(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file')
        self.setup_file('/temp_folder/another_folder/temp_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_dir_to_dx_existing_dir.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_handler.new_folder('/folder2')
        proj_p = DXPath('dx://test_dx_dir_to_dx_existing_dir.TempProj:/folder2')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        dx_p.copytree(proj_p)
        proj_file_p = DXPath('dx://test_dx_dir_to_dx_existing_dir.TempProj:/folder2/'
                             'temp_folder/another_folder/temp_file.txt')
        self.assertTrue(proj_file_p.exists())

    def test_dx_dir_to_dx_dir_w_slash(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file')
        self.setup_file('/temp_folder/another_folder/temp_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_dir_to_dx_dir_w_slash.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_dx_dir_to_dx_dir_w_slash.TempProj:/folder2/')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        dx_p.copytree(proj_p)
        proj_file_p = DXPath('dx://test_dx_dir_to_dx_dir_w_slash.TempProj:/folder2/'
                             'temp_folder/another_folder/temp_file.txt')
        self.assertTrue(proj_file_p.exists())

    def test_dx_to_same_dx_pass(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        dx_p.copytree(dx_p)
        self.assertTrue(dx_p.exists())

    def test_dx_to_existing_dx_dest_fail(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file'])
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_to_existing_dx_dest_fail.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_dx_to_existing_dx_dest_fail.TempProj:/folder2')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        proj_handler.new_folder('/folder2/temp_folder', parents=True)
        with pytest.raises(exceptions.TargetExistsError, match='Destination path'):
            dx_p.copytree(proj_p)

    def test_dx_dir_to_dx_root(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/another_folder/temp_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_dir_to_dx_root.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_dx_dir_to_dx_root.TempProj')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        dx_p.copytree(proj_p)
        proj_file_p = DXPath('dx://test_dx_dir_to_dx_root.TempProj:/'
                             'temp_folder/another_folder/temp_file.txt')
        self.assertTrue(proj_file_p.exists())

    def test_dx_root_to_dx_root(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_root_to_dx_root.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_dx_root_to_dx_root.TempProj:/')
        dx_p = DXPath('dx://' + self.project)
        dx_p.copytree(proj_p)
        proj_file_p = DXPath('dx://test_dx_root_to_dx_root.TempProj:/{}/'
                             'temp_folder/folder_file'.format(self.project))
        self.assertTrue(proj_file_p.exists())

    def test_dx_root_to_dx_dir(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file')
        self.setup_file('/temp_folder/another_folder/temp_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_root_to_dx_dir.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_dx_root_to_dx_dir.TempProj:/folder')
        dx_p = DXPath('dx://' + self.project)
        dx_p.copytree(proj_p)
        proj_file_p = DXPath('dx://test_dx_root_to_dx_dir.TempProj:/folder/'
                             'temp_folder/folder_file')
        self.assertTrue(proj_file_p.exists())

    def test_dx_root_to_existing_dx_dir(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file')
        self.setup_file('/temp_folder/another_folder/temp_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_root_to_existing_dx_dir.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_handler.new_folder('/folder')
        proj_p = DXPath('dx://test_dx_root_to_existing_dx_dir.TempProj:/folder')
        dx_p = DXPath('dx://' + self.project)
        dx_p.copytree(proj_p)
        proj_file_p = DXPath('dx://test_dx_root_to_existing_dx_dir.TempProj:/folder/{}/'
                             'temp_folder/folder_file'.format(self.project))
        self.assertTrue(proj_file_p.exists())

    def test_dx_root_to_dx_dir_w_slash(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file')
        self.setup_file('/temp_folder/another_folder/temp_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_root_to_dx_dir_w_slash.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_dx_root_to_dx_dir_w_slash.TempProj:/folder/')
        dx_p = DXPath('dx://' + self.project)
        dx_p.copytree(proj_p)
        proj_file_p = DXPath('dx://test_dx_root_to_dx_dir_w_slash.TempProj:/folder/{}/'
                             'temp_folder/folder_file'.format(self.project))
        self.assertTrue(proj_file_p.exists())

    def test_dx_dir_to_dx_dir_same_project_fail(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file',
                          '/another_folder/temp_folder/temp_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')

        new_dx_p = DXPath('dx://' + self.project + ':/new_folder')
        with pytest.raises(dx.DNAnexusError, match='same project'):
            dx_p.copytree(new_dx_p, raise_if_same_project=True)

        new_dx_p = DXPath('dx://' + self.project + ':/another_folder')
        with pytest.raises(exceptions.TargetExistsError, match='duplicate folders'):
            dx_p.copytree(new_dx_p)

    def test_dx_dir_to_dx_dir_same_project(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        new_dx_p = DXPath('dx://' + self.project + ':/new_folder/folder2')
        stor.copytree(dx_p, new_dx_p)
        self.assertTrue(new_dx_p.isdir())
        new_file_dx_p = DXPath('dx://' + self.project + ':/new_folder/folder2/folder_file')
        self.assertTrue(new_file_dx_p.exists())

    def test_dx_dir_to_dx_root_same_project(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/another_folder/temp_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/another_folder')
        to_dx_p = DXPath('dx://' + self.project)
        dx_p.copytree(to_dx_p)
        new_dx_p = DXPath('dx://' + self.project + ':/another_folder/temp_file.txt')
        self.assertTrue(new_dx_p.exists())

    def test_dx_root_to_dx_dir_same_project(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file'])
        to_dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        dx_p = DXPath('dx://' + self.project)
        with pytest.raises(dx.DNAnexusError, match='Cannot move root folder'):
            dx_p.copytree(to_dx_p)


class TestUpload(DXTestCase):
    def test_upload_files_existing(self):
        self.setup_temporary_project()
        self.setup_file('/folder/file2.txt')
        self.setup_posix_files(['/folder/file.txt',
                                '/folder/file2.txt'])
        posix_p = Path('./{test_folder}/{path}'.format(
            test_folder=self.project, path='folder'))
        dx_folder_p = DXPath('dx://' + self.project + ':/')
        files_to_upload = []
        files_to_upload.append(stor.obs.OBSUploadObject(posix_p / 'file.txt', '/folder/file.txt'))
        files_to_upload.append(stor.obs.OBSUploadObject(posix_p / 'file.txt', '/folder/file2.txt'))
        with LogCapture('stor.dx') as log:
            dx_folder_p.upload(files_to_upload)
            assert 'will not cause duplicate file objects' in log.records[-1].getMessage()

        dx_p = DXPath('dx://' + self.project + ':/folder/file2.txt')
        self.assertTrue(dx_p.exists())
        with stor.open(dx_p, 'r') as uploaded_file:
            self.assertEqual(uploaded_file.read(), 'data')


class TestGetSize(DXTestCase):
    def test_project(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project)
        size = dx_p.getsize()
        self.assertEqual(size, 0)

    def test_file(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file')
        size = dx_p.getsize()
        self.assertEqual(size, 4)

    def test_folder(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        with pytest.raises(exceptions.NotFoundError, match='No data object was found'):
            dx_p.getsize()


class TestRaiseError(unittest.TestCase):
    def test_error_403(self):
        content = {
            'error': {
                'type': 'Unauthorized',
                'message': 'Permission denied'
            }
        }
        dx_error = dxpy.DXAPIError(content, 403)
        result = dx._dx_error_to_descriptive_exception(dx_error)
        self.assertEqual(type(result), exceptions.UnauthorizedError)
        self.assertEqual(str(result),
                         'Either use `dx login --token {your_dx_token} --save` or set '
                         'DX_AUTH_TOKEN environment variable. Unauthorized - Permission denied')
        self.assertEqual(result.caught_exception, dx_error)

    def test_error_404(self):
        content = {
            'error': {
                'type': 'Not Found',
                'message': 'Resource missing'
            }
        }
        dx_error = dxpy.DXAPIError(content, 404)
        result = dx._dx_error_to_descriptive_exception(dx_error)
        self.assertEqual(type(result), exceptions.NotFoundError)
        self.assertEqual(str(result), 'Not Found - Resource missing')
        self.assertEqual(result.caught_exception, dx_error)

    def test_error_409(self):
        content = {
            'error': {
                'type': 'Conflict',
                'message': 'Conflict error mssg'
            }
        }
        dx_error = dxpy.DXAPIError(content, 409)
        result = dx._dx_error_to_descriptive_exception(dx_error)
        self.assertEqual(type(result), exceptions.ConflictError)
        self.assertEqual(str(result), 'Conflict - Conflict error mssg')
        self.assertEqual(result.caught_exception, dx_error)

    def test_error_checksum_mismatch(self):
        dx_error = dxpy.DXFileError('DXChecksumMismatchError - mismatch')
        result = dx._dx_error_to_descriptive_exception(dx_error)
        self.assertEqual(type(result), dx.InconsistentUploadDownloadError)
        self.assertEqual(str(result), 'DXChecksumMismatchError - mismatch')
        self.assertEqual(result.caught_exception, dx_error)

    def test_random_error(self):
        dx_error = Exception('Random dxpy error')
        result = dx._dx_error_to_descriptive_exception(dx_error)
        self.assertEqual(type(result), dx.DNAnexusError)
        self.assertEqual(str(result), 'Random dxpy error')
        self.assertEqual(result.caught_exception, dx_error)


class TestLoginAuth(DXTestCase):
    def test_login_auth(self):
        def mock_header(r, security_context):
            auth_header = security_context["auth_token_type"] + " " + \
                security_context["auth_token"]
            r.headers[b'Authorization'] = auth_header.encode()
            return r

        self.setup_temporary_project()
        self.test_dir = DXPath('dx://' + self.project + ':test')
        # dxpy.AUTH_HELPER gets set upon login and called with each api which we mock out here
        with stor.settings.use({'dx': {'auth_token': ''}}):
            with mock.patch('dxpy.AUTH_HELPER') as mock_auth:
                mock_auth.security_context = {
                    'auth_token_type': 'Bearer',
                    'auth_token': 'PUBLIC'
                }
                mock_auth.side_effect = lambda x: mock_header(x, mock_auth.security_context)
                with pytest.raises(exceptions.NotFoundError, match='no projects'):
                    self.test_dir.makedirs_p()
                self.assertEqual(mock_auth.call_count, 1)
        self.test_dir.makedirs_p()
        self.assertTrue(self.test_dir.isdir())


class TestContentType(DXTestCase):
    @mock.patch.object(DXPath, 'stat')
    def test_content_type(self, mock_stat):
        mock_stat.return_value = {
            u'archivalState': u'live',
            u'class': u'file',
            u'created': 1563329954000,
            u'createdBy': {u'user': u'user-someuser'},
            u'folder': u'/',
            u'hidden': False,
            u'id': u'file-234092349023490290239398',
            u'links': [],
            u'media': u'text/plain',
            u'modified': 1563329955907,
            u'name': u'somefile.txt',
            u'project': u'project-123456823409234902309423',
            u'size': 6,
            u'sponsored': False,
            u'state': u'closed',
            u'tags': [],
            u'types': []
        }
        self.assertEqual(DXPath('dx://P:/C/T').content_type, 'text/plain')
        mock_stat.return_value = {}
        self.assertEqual(DXPath('dx://P:/C/T').content_type, '')
