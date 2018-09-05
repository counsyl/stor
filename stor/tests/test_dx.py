import pytest
import time
from tempfile import NamedTemporaryFile
import unittest
import vcr

import dxpy
import dxpy.bindings as dxb
import freezegun
import mock
import six.moves.urllib as urllib

import stor
from stor import exceptions
from stor import NamedTemporaryDirectory
from stor import Path
from stor import settings
from stor import swift
from stor import utils
from stor.dx import DXPath, DXVirtualPath, DXCanonicalPath
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


class TestResource(unittest.TestCase):

    def test_resource_none_w_project(self):
        dx_p = DXPath('dx://project:/')
        self.assertIsNone(dx_p.resource)

    def test_resource_object(self):
        dx_p = DXPath('dx://project:/obj')
        self.assertEqual(dx_p.resource, '/obj')

    def test_resource_trailing_slash(self):
        dx_p = DXPath('dx://project:/dir/')
        self.assertEqual(dx_p.resource, '/dir/')

    def test_resource_nested_obj(self):
        dx_p = DXPath('dx://project:/nested/obj.txt')
        self.assertEqual(dx_p.resource, '/nested/obj.txt')

    def test_resource_nested_dir(self):
        dx_p = DXPath('dx://project:/nested/dir/')
        self.assertEqual(dx_p.resource, '/nested/dir/')


@unittest.skip("skipping")
class TestDXFile(DXTestCase):  # TODO

    def test_read_on_open_file(self):
        d = dxpy.bindings.dxfile_functions.new_dxfile()
        self.assertEqual(d.describe()['state'], 'open')

        dx_p = DXPath('dx://{}/{}'.format(self.project, d.name))
        with self.assertRaisesRegexp(ValueError, 'not in closed state'):
            dx_p.read_object()

        d.remove()

    def test_read_success_on_closed_file(self):
        dx_p = DXPath('dx://{}/{}'.format(self.project, self.file_handler.name))
        self.assertEqual(dx_p.read_object(), b'data')
        self.assertEqual(dx_p.open().read(), 'data')

    def test_iterating_over_files(self):
        data = b'''\
line1
line2
line3
line4
'''
        with dxpy.bindings.dxfile_functions.new_dxfile() as d:
            d.write(data)
        d.state = 'closed'
        dx_p = DXPath('dx://{}/{}'.format(self.project, d.name))
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

    def test_write_multiple_w_context_manager(self, mock_upload):
        dx_p = DXPath('dx://{}/{}'.format(self.project, self.file_handler.name))
        with dx_p.open(mode='wb') as obj:
            obj.write(b'hello')
            obj.write(b' world')
        self.assertIn(b'hello world', dx_p.read_object())

    @mock.patch('time.sleep', autospec=True)
    @mock.patch.object(DXPath, 'upload', autospec=True)
    def test_write_multiple_flush_multiple_upload(self, mock_upload):
        dx_p = DXPath('dx://project/obj')
        with NamedTemporaryFile(delete=False) as ntf1,\
                NamedTemporaryFile(delete=False) as ntf2,\
                NamedTemporaryFile(delete=False) as ntf3:
            with mock.patch('tempfile.NamedTemporaryFile', autospec=True) as ntf:
                ntf.side_effect = [ntf1, ntf2, ntf3]
                with dx_p.open(mode='wb') as obj:
                    obj.write(b'hello')
                    obj.flush()
                    obj.write(b' world')
                    obj.flush()
                u1, u2, u3 = mock_upload.call_args_list
                u1[0][1][0].source == ntf1.name
                u2[0][1][0].source == ntf2.name
                u3[0][1][0].source == ntf3.name
                u1[0][1][0].object_name == dx_p.resource
                u2[0][1][0].object_name == dx_p.resource
                u3[0][1][0].object_name == dx_p.resource
                self.assertEqual(open(ntf1.name).read(), 'hello')
                self.assertEqual(open(ntf2.name).read(), 'hello world')
                # third call happens because we don't care about checking for
                # additional file change
                self.assertEqual(open(ntf3.name).read(), 'hello world')


@unittest.skip("skipping")
class TestDXShared(SharedOBSFileCases, DXTestCase):
    drive = 'dx://'
    path_class = DXPath
    normal_path = DXPath('dx://project:/obj')


class TestCanonicalProject(DXTestCase):
    def test_no_project(self):
        dx_p = DXPath('dx://Random_Project:/')
        with pytest.raises(dx.ProjectNotFoundError, match='No projects'):
            dx_p.canonical_project

    def test_unique_project(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://'+self.project)
        self.assertTrue(utils.is_valid_dxid(dx_p.canonical_project, 'project'))

    def test_duplicate_projects(self):
        self.setup_temporary_project()
        test_proj = dxb.DXProject()
        test_proj.new(self.new_proj_name())
        dx_p = DXPath('dx://' + self.project)
        with pytest.raises(dx.DuplicateProjectError, match='Duplicate projects'):
            dx_p.canonical_project
        test_proj.destroy()


class TestCanonicalResource(DXTestCase):
    def test_no_resource(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project + ':/random.txt')
        with pytest.raises(dx.NotFoundError, match='does not exist'):
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
        with pytest.raises(dx.DuplicateError, match='not unique'):
            dx_p.canonical_resource


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
        results = dx_p.listdir()
        self.assertEqual(results, [])

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
        with pytest.raises(dx.NotFoundError, match='specified folder'):
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
        results = dx_p.listdir()
        self.assertEqual(results, [])

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
            results = dx_p.list(condition=lambda res: len(res) == 1)

    def test_list_w_category(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_file.txt'])
        dx_p = DXPath('dx://' + self.project)
        dxpy.new_dxworkflow(title='Workflow', project=self.proj_id)
        results = dx_p.list(category='file')
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
        self.setup_files(['/temp_folder/folder_file.csv',
                          '/random_file.txt'])
        dx_p = DXPath('dx://'+self.project)
        results = list(dx_p.walkfiles(pattern='*temp*'))
        self.assertEqual(results, [])


class TestStat(DXTestCase):
    def test_stat_folder(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.csv'])
        with pytest.raises(ValueError, match='ending in folders'):
            DXPath('dx://'+self.project+':/temp_folder/').stat()
        with pytest.raises(ValueError, match='ending in folders'):
            DXPath('dx://'+self.project+':/temp_folder').stat()

    def test_stat_project_error(self):
        self.setup_temporary_project()  # creates project with name in self.project
        test_proj = dxb.DXProject()
        test_proj.new(self.new_proj_name())  # creates duplicate project

        with pytest.raises(dx.DuplicateProjectError, match='Duplicate projects'):
            DXPath('dx://'+self.project+':').stat()
        with pytest.raises(dx.DuplicateProjectError, match='Duplicate projects'):
            DXPath('dx://'+self.project+':/').stat()
        with pytest.raises(dx.NotFoundError, match='No projects'):
            DXPath('dx://Random_Proj:').stat()

        test_proj.destroy()

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
    def test_false(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://'+self.project+':/random.txt')
        result = dx_p.exists()
        self.assertFalse(result)

    def test_false_no_folder(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/random_folder/folder_file.txt')
        result = dx_p.exists()
        self.assertFalse(result)

    def test_raises_on_duplicate(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_folder/folder_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file.txt')
        with pytest.raises(dx.DuplicateError, match='not unique'):
            dx_p.exists()

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
        with pytest.raises(dx.DXError, match='DX Projects'):
            dx_p.temp_url()

    def test_fail_on_folder(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        with pytest.raises(dx.DXError, match='DXPaths ending in folders'):
            dx_p.temp_url()

    def test_on_file(self):
        self.setup_temporary_project()
        with dxpy.new_dxfile(name='temp_file.txt',
                             project=self.proj_id) as f:
            f.write('data')
        while f._get_state().lower() != 'closed':
            time.sleep(1)  # to allow for the file state to go to closed after calling close()
        dx_p = DXPath('dx://' + self.project + ':/temp_file.txt')
        result = dx_p.temp_url()
        self.assertIn('dl.dnanex.us', result)

    def test_on_file_canonical(self):
        self.setup_temporary_project()
        with dxpy.new_dxfile(name='temp_file.txt',
                             project=self.proj_id) as f:
            f.write('data')
        while f._get_state().lower() != 'closed':
            time.sleep(1)  # to allow for the file state to go to closed after calling close()
        dx_p = DXPath('dx://' + self.project + ':/temp_file.txt').canonical_path
        result = dx_p.temp_url()
        self.assertIn('dl.dnanex.us', result)

    def test_on_file_named_timed(self):  # TODO
        self.setup_temporary_project()
        with dxpy.new_dxfile(name='temp_file.txt',
                             project=self.proj_id) as f:
            f.write('data')
        while f._get_state().lower() != 'closed':
            time.sleep(1)  # to allow for the file state to go to closed after calling close()
        dx_p = DXPath('dx://' + self.project + ':/temp_file.txt')
        result = dx_p.temp_url(filename='random.txt', lifetime=1)
        self.assertIn('dl.dnanex.us', result)
        self.assertIn('random.txt', result)
        url = urllib.request.urlopen(result)
        self.assertIn('attachment', url.headers['content-disposition'])
        self.assertIn('random.txt', url.headers['content-disposition'])
        time.sleep(2)  # for link to expire
        with pytest.raises(urllib.error.HTTPError):
            urllib.request.urlopen(result)
