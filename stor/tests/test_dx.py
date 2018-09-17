import pytest
import time
from tempfile import NamedTemporaryFile
import unittest

import dxpy
import dxpy.bindings as dxb
import mock
import six.moves.urllib as urllib

from stor import exceptions
from stor import NamedTemporaryDirectory
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
        from stor.dx import DXVirtualPath
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
        self.addCleanup(test_proj.destroy)
        dx_p = DXPath('dx://' + self.project)
        with pytest.raises(dx.DuplicateProjectError, match='Duplicate projects'):
            dx_p.canonical_project


class TestCanonicalResource(DXTestCase):
    def test_no_resource(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project + ':/random.txt')
        with pytest.raises(dx.NotFoundError, match='No data object was found'):
            dx_p.canonical_resource

    def test_unique_resource(self):
        self.setup_temporary_project()
        self.setup_file('/temp_file.txt')
        dx_p = DXPath('dx://'+self.project + ':/temp_file.txt')
        self.assertTrue(utils.is_valid_dxid(dx_p.canonical_resource, 'file'))

    def test_duplicate_resource(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_folder/folder_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file.txt')
        with pytest.raises(dx.DuplicateError, match='Multiple objects found'):
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
        self.setup_file('/temp_file.txt')
        dx_p = DXPath('dx://'+self.project+':/temp_file.txt')
        with pytest.raises(dx.NotFoundError, match='specified folder'):
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
        self.setup_file('/temp_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_file.txt').canonical_path
        with pytest.raises(dx.NotFoundError, match='specified folder'):
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
        self.setup_file('/temp_file.txt')
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
        self.setup_file('/temp_file.txt')
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
        self.setup_file('/temp_folder/folder_file.csv')
        with pytest.raises(ValueError, match='Invalid operation'):
            DXPath('dx://'+self.project+':/temp_folder/').stat()
        with pytest.raises(dx.NotFoundError, match='No data object was found'):
            DXPath('dx://'+self.project+':/temp_folder').stat()

    def test_stat_project_error(self):
        self.setup_temporary_project()  # creates project with name in self.project
        test_proj = dxb.DXProject()
        test_proj.new(self.new_proj_name())  # creates duplicate project
        self.addCleanup(test_proj.destroy)
        with pytest.raises(dx.DuplicateProjectError, match='Duplicate projects'):
            DXPath('dx://'+self.project+':').stat()
        with pytest.raises(dx.DuplicateProjectError, match='Duplicate projects'):
            DXPath('dx://'+self.project+':/').stat()
        with pytest.raises(dx.NotFoundError, match='No projects'):
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

    def test_false_folder_w_file(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/random_folder/folder_file.txt')
        result = dx_p.exists()
        self.assertFalse(result)

    def test_raises_on_duplicate(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_folder/folder_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file.txt')
        with pytest.raises(dx.DuplicateError, match='Multiple objects found'):
            dx_p.exists()

    def test_true_file_folder_share_name(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_folder'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        result = dx_p.exists()
        self.assertTrue(result)

    def test_true_file(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file.txt')
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

    def test_true_dir_with_object(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file.txt')
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
        with pytest.raises(ValueError, match='DX Projects'):
            dx_p.temp_url()

    def test_fail_on_folder(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/')
        with pytest.raises(ValueError, match='Invalid operation'):
            dx_p.temp_url()
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        with pytest.raises(dx.NotFoundError, match='No data object was found'):
            dx_p.temp_url()

    def test_on_file(self):
        self.setup_temporary_project()
        self.setup_file('/temp_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_file.txt')
        result = dx_p.temp_url()
        self.assertIn('dl.dnanex.us', result)

    def test_on_file_canonical(self):
        self.setup_temporary_project()
        self.setup_file('/temp_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_file.txt').canonical_path
        result = dx_p.temp_url()
        self.assertIn('dl.dnanex.us', result)

    def test_on_file_named_timed(self):
        self.setup_temporary_project()
        self.setup_file('/temp_file.txt')
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
        with pytest.raises(dx.NotFoundError, match='No data object was found'):
            dx_p.remove()

    def test_fail_remove_project(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project)
        with pytest.raises(ValueError, match='must point to single data object'):
            dx_p.remove()

    def test_fail_remove_nonexistent_file(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://' + self.project + ':/temp_file.txt')
        with pytest.raises(dx.NotFoundError, match='No data object was found'):
            dx_p.remove()

    def test_fail_rmtree_file(self):
        self.setup_temporary_project()
        self.setup_file('/temp_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_file.txt')
        with pytest.raises(dx.NotFoundError, match='No folders were found'):
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
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_rmtree_project.TempProj')
        proj_path = DXPath('dx://test_rmtree_project.TempProj')
        proj_path.rmtree()
        with self.assertRaises(dxpy.exceptions.ResourceNotFound):
            proj_handler.destroy()

    def test_fail_remove_nonexistent_project(self):
        self.setup_temporary_project()
        dx_p = DXPath('dx://RandomProject:/')
        with pytest.raises(dx.NotFoundError, match='No projects were found'):
            dx_p.rmtree()


class TestMakedirsP(DXTestCase):
    def test_makedirs_p_project(self):
        with pytest.raises(ValueError, match='Cannot create a project'):
            DXPath('dx://project:').makedirs_p()

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


class TestCopy(DXTestCase):
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
        with pytest.raises(dx.NotFoundError, match='No data object'):
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
        proj_handler.new('test_dx_to_dx_file.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_dx_to_dx_file.TempProj:/random/')  # with trailing slash
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file.txt')
        dx_p.copy(proj_p)
        expected_p = DXPath('dx://test_dx_to_dx_file.TempProj:/random/folder_file.txt')
        self.assertTrue(expected_p.exists())

    def test_dx_to_dx_file_folder_no_ext(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_to_dx_file.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_dx_to_dx_file.TempProj:/random/file')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file.txt')
        dx_p.copy(proj_p)  # when folder without ext doesn't exist
        expected_p = DXPath('dx://test_dx_to_dx_file.TempProj:/random/file')
        self.assertTrue(expected_p.exists())
        self.assertFalse(expected_p.isdir())
        false_dx_p = DXPath('dx://test_dx_to_dx_file.TempProj:/random/file/folder_file.txt')
        self.assertFalse(false_dx_p.exists())
        proj_p.remove()  # since same file cannot be copied to project twice
        proj_p = DXPath('dx://test_dx_to_dx_file.TempProj:/random')
        dx_p.copy(proj_p)  # when folder without ext already exists
        true_dx_p = DXPath('dx://test_dx_to_dx_file.TempProj:/random/folder_file.txt')
        self.assertTrue(true_dx_p.exists())

    def test_dx_to_dx_within_project_fail(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/another_folder/random_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/folder_file.txt')

        new_dx_p = DXPath('dx://' + self.project + ':/another_folder/file.txt')
        with pytest.raises(dx.DNAnexusError, match='same project'):
            dx_p.copy(new_dx_p)

        new_dx_p = DXPath('dx://' + self.project + ':/another_folder/')
        with pytest.raises(dx.DNAnexusError, match='same project'):
            dx_p.copy(new_dx_p)

        new_dx_p = DXPath('dx://' + self.project + ':/another_folder/random_file.txt')
        with pytest.raises(dx.TargetExistsError, match='duplicate'):
            dx_p.copy(new_dx_p, move_within_project=True)

    def test_dx_to_dx_within_project_pass(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/file.txt',
                          '/another_folder/random_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/file.txt')
        new_dx_p = DXPath('dx://' + self.project + ':/another_folder/fold_file.txt')
        dx_p.copy(new_dx_p, move_within_project=True)
        self.assertTrue(new_dx_p.exists())
        dx_p = DXPath(dx_p)  # to avoid cached describe values
        self.assertFalse(dx_p.exists())
        new_folder_dx_p = DXPath('dx://' + self.project + ':/another_folder/')
        with pytest.raises(dx.NotFoundError, match='No data object'):
            dx_p.copy(new_folder_dx_p, move_within_project=True)
        new_dx_p.copy(dx_p, move_within_project=True)  # restoring to original state
        dx_p.copy(new_folder_dx_p, move_within_project=True)
        dx_p = DXPath(dx_p)
        self.assertTrue(new_dx_p.exists())
        self.assertFalse(dx_p.exists())

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
        proj_handler.new('test_dx_to_dx_file.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_dx_to_dx_file.TempProj:/random.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/temp_file.txt'
                      ).canonical_path
        dx_p.copy(proj_p)
        self.assertTrue(proj_p.exists())


class TestCopyTree(DXTestCase):
    def test_dx_file_to_posix(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/file.txt')
        posix_p = Path('./{test_folder}/'.format(
            test_folder=self.project))
        with pytest.raises(dx.NotFoundError, match='No folder or project was found'):
            dx_p.copytree(posix_p)

    def test_dx_dir_to_posix(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file.txt',
                          '/temp_folder/another_folder/temp_file.txt'])
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
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/')
        posix_p = Path('./{test_folder}/{path}'.format(
            test_folder=self.project, path='folder/'))
        posix_p.copytree(dx_p)
        dx_file_p = DXPath('dx://' + self.project + ':/temp_folder/file.txt')
        self.assertTrue(dx_file_p.exists())

        dx_p_2 = DXPath('dx://' + self.project + ':/temp_folder/another')
        posix_p.copytree(dx_p_2)
        dx_file_p = DXPath('dx://' + self.project + ':/temp_folder/another/file.txt')
        self.assertTrue(dx_file_p.exists())

        dx_p_3 = DXPath('dx://' + self.project + ':/temp_folder')  # already exists
        with pytest.raises(dx.TargetExistsError, match='will not cause duplicate'):
            posix_p.copytree(dx_p_3)

    def test_dx_to_other_obs(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/')
        obs_p = Path('swift://tenant/container/folder/')
        with pytest.raises(ValueError, match='cannot copy'):
            dx_p.copytree(obs_p)

    def test_other_obs_to_dx(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/')
        obs_p = Path('swift://tenant/container/folder/')
        with pytest.raises(ValueError, match='cannot copy'):
            obs_p.copytree(dx_p)

    def test_dx_dir_to_dx_dir(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file')
        self.setup_file('/temp_folder/another_folder/temp_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_to_dx_file.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_dx_to_dx_file.TempProj:/random.txt')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        dx_p.copytree(proj_p)
        proj_file_p = DXPath('dx://test_dx_to_dx_file.TempProj:/random.txt/folder_file')
        self.assertTrue(proj_p.isdir())
        self.assertTrue(proj_file_p.exists())

    def test_dx_dir_to_dx_existing_dir(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file')
        self.setup_file('/temp_folder/another_folder/temp_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_to_dx_file.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_handler.new_folder('/folder2')
        proj_p = DXPath('dx://test_dx_to_dx_file.TempProj:/folder2')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        dx_p.copytree(proj_p)
        proj_file_p = DXPath('dx://test_dx_to_dx_file.TempProj:/folder2/'
                             'temp_folder/another_folder/temp_file.txt')
        self.assertTrue(proj_file_p.exists())

    def test_dx_dir_to_dx_dir_w_slash(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file')
        self.setup_file('/temp_folder/another_folder/temp_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_to_dx_file.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_dx_to_dx_file.TempProj:/folder2/')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        dx_p.copytree(proj_p)
        proj_file_p = DXPath('dx://test_dx_to_dx_file.TempProj:/folder2/'
                             'temp_folder/another_folder/temp_file.txt')
        self.assertTrue(proj_file_p.exists())

    def test_dx_to_existing_dx_dest_fail(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file')
        self.setup_file('/temp_folder/temp_folder/temp_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_to_dx_file.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_dx_to_dx_file.TempProj:/folder2')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        proj_handler.new_folder('/folder2/temp_folder', parents=True)
        with pytest.raises(dx.TargetExistsError, match='Destination path'):
            dx_p.copytree(proj_p)

    def test_dx_dir_to_dx_root(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file')
        self.setup_file('/temp_folder/another_folder/temp_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_to_dx_file.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_dx_to_dx_file.TempProj:/')
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        dx_p.copytree(proj_p)
        proj_file_p = DXPath('dx://test_dx_to_dx_file.TempProj:/'
                             'temp_folder/another_folder/temp_file.txt')
        self.assertTrue(proj_file_p.exists())

    def test_dx_root_to_dx_root(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file')
        self.setup_file('/temp_folder/another_folder/temp_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_to_dx_file.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_dx_to_dx_file.TempProj:/')
        dx_p = DXPath('dx://' + self.project)
        dx_p.copytree(proj_p)
        proj_file_p = DXPath('dx://test_dx_to_dx_file.TempProj:/{}/'
                             'temp_folder/folder_file'.format(self.project))
        self.assertTrue(proj_file_p.exists())

    def test_dx_root_to_dx_dir(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file')
        self.setup_file('/temp_folder/another_folder/temp_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_to_dx_file.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_dx_to_dx_file.TempProj:/folder')
        dx_p = DXPath('dx://' + self.project)
        dx_p.copytree(proj_p)
        proj_file_p = DXPath('dx://test_dx_to_dx_file.TempProj:/folder/'
                             'temp_folder/folder_file')
        self.assertTrue(proj_file_p.exists())

    def test_dx_root_to_existing_dx_dir(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file')
        self.setup_file('/temp_folder/another_folder/temp_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_to_dx_file.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_handler.new_folder('/folder')
        proj_p = DXPath('dx://test_dx_to_dx_file.TempProj:/folder')
        dx_p = DXPath('dx://' + self.project)
        dx_p.copytree(proj_p)
        proj_file_p = DXPath('dx://test_dx_to_dx_file.TempProj:/folder/{}/'
                             'temp_folder/folder_file'.format(self.project))
        self.assertTrue(proj_file_p.exists())

    def test_dx_root_to_dx_dir_w_slash(self):
        self.setup_temporary_project()
        self.setup_file('/temp_folder/folder_file')
        self.setup_file('/temp_folder/another_folder/temp_file.txt')
        proj_handler = dxpy.DXProject()
        proj_handler.new('test_dx_to_dx_file.TempProj')
        self.addCleanup(proj_handler.destroy)
        proj_p = DXPath('dx://test_dx_to_dx_file.TempProj:/folder/')
        dx_p = DXPath('dx://' + self.project)
        dx_p.copytree(proj_p)
        proj_file_p = DXPath('dx://test_dx_to_dx_file.TempProj:/folder/{}/'
                             'temp_folder/folder_file'.format(self.project))
        self.assertTrue(proj_file_p.exists())

    def test_dx_dir_to_dx_dir_same_project_fail(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file',
                          '/temp_folder/another_folder/temp_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        new_dx_p = DXPath('dx://' + self.project + ':/new_folder')
        with pytest.raises(dx.DNAnexusError, match='same project'):
            dx_p.copytree(new_dx_p)

    def test_dx_dir_to_dx_dir_same_project(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file',
                          '/temp_folder/another_folder/temp_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        new_dx_p = DXPath('dx://' + self.project + ':/new_folder')
        dx_p.copytree(new_dx_p, move_within_project=True)
        self.assertTrue(new_dx_p.isdir())
        new_file_dx_p = DXPath('dx://' + self.project + ':/new_folder/folder_file')
        self.assertTrue(new_file_dx_p.exists())

    def test_dx_dir_to_dx_root_same_project(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/another_folder/folder_file',
                          '/temp_folder/another_folder/temp_file.txt'])
        dx_p = DXPath('dx://' + self.project + ':/temp_folder/another_folder')
        to_dx_p = DXPath('dx://' + self.project)
        dx_p.copytree(to_dx_p, move_within_project=True)
        new_dx_p = DXPath('dx://' + self.project + ':/another_folder/temp_file.txt')
        self.assertTrue(new_dx_p.exists())

    def test_dx_root_to_dx_dir_same_project(self):
        self.setup_temporary_project()
        self.setup_files(['/temp_folder/folder_file'])
        to_dx_p = DXPath('dx://' + self.project + ':/temp_folder')
        dx_p = DXPath('dx://' + self.project)
        with pytest.raises(dx.DNAnexusError, match='Cannot move root folder'):
            dx_p.copytree(to_dx_p, move_within_project=True)
