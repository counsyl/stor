import logging
import ntpath
import os
from tempfile import NamedTemporaryFile
import unittest

import dxpy
import freezegun
import mock
from testfixtures import LogCapture

import stor
from stor import exceptions
from stor import NamedTemporaryDirectory
from stor import Path
from stor import settings
from stor import swift
from stor import utils
from stor.dx import DXPath
import stor.dx as dx
from stor.test import DXTestCase
from stor.tests.shared_obs import SharedOBSFileCases


class TestBasicPathMethods(unittest.TestCase):
    def test_name(self):
        p = Path('dx://project/path/to/resource')
        self.assertEquals(p.name, 'resource')

    def test_parent(self):
        p = Path('dx://project/path/to/resource')
        self.assertEquals(p.parent, 'dx://project/path/to')

    def test_dirname(self):
        p = Path('dx://project/path/to/resource')
        self.assertEquals(p.dirname(), 'dx://project/path/to')

    def test_dirname_top_level(self):
        p1 = Path('dx://')
        self.assertEquals(p1.dirname(), 'dx://')

        p2 = Path('dx://a')
        self.assertEquals(p2.dirname(), 'dx://')

    def test_basename(self):
        p = Path('dx://project/path/to/resource')
        self.assertEquals(p.basename(), 'resource')

    def test_to_url(self):
        p = Path('dx://project/path/to/resource')

        #TODO(akumar) to_url may not make sense for dx paths since DNAnexus uses S3 in backend.
        #TODO(akumar) Remove this test if so...

        # self.assertEquals(p.to_url(),
        #                   'https://mybucket.s3.amazonaws.com/path/to/resource')

    def test_failed_new(self):
        with self.assertRaises(ValueError):
            DXPath('/bad/dx/path')

    def test_successful_new(self):
        p = DXPath('dx://project/path')
        self.assertEquals(p, 'dx://project/path')

    def test_success_dx_id(self):
        # dnanexus always has 24-chr key
        # TODO (akumar) does initializing with project id actually initialize with abs dx path?
        p = DXPath('dx://project-123456789012345678901234')
        self.assertEqual(len(p.basename()), 32)  # 'project-' and 24-chr key

        p = DXPath('dx://project-123456789012345678901234/file-123456789012345678901234')
        self.assertEqual(len(p.dirname()), 32)  # 'project-' and 24-chr key
        self.assertEqual(len(p.basename()), 29)  # 'file-' and 24-chr key

    def test_failed_dx_id(self):
        with self.assertRaises(ValueError):
            DXPath('dx://project-1234')
        with self.assertRaises(ValueError):
            DXPath('dx://project-123456789012345678901234/file-1234')


class TestRepr(unittest.TestCase):
    def test_repr(self):
        dx_p = DXPath('dx://t/c/p')
        self.assertEquals(eval(repr(dx_p)), dx_p)


class TestPathManipulations(unittest.TestCase):
    def test_add(self):
        dx_p = DXPath('dx://a')
        dx_p = dx_p + 'b' + Path('c')
        self.assertTrue(isinstance(dx_p, DXPath))
        self.assertEquals(dx_p, 'dx://abc')

    def test_div(self):
        dx_p = DXPath('dx://t')
        dx_p = dx_p / 'c' / Path('p')
        self.assertTrue(isinstance(dx_p, DXPath))
        self.assertEquals(dx_p, 'dx://t/c/p')


class TestProject(unittest.TestCase):
    def test_project_none(self):
        dx_p = DXPath('dx://')
        self.assertIsNone(dx_p.project)

    def test_project_exists(self):
        dx_p = DXPath('dx://project')
        self.assertEquals(dx_p.project, 'project')


class TestResource(unittest.TestCase):
    def test_resource_none_no_project(self):
        dx_p = DXPath('dx://')
        self.assertIsNone(dx_p.resource)

    def test_resource_none_w_project(self):
        dx_p = DXPath('dx://project/')
        self.assertIsNone(dx_p.resource)

    def test_resource_object(self):
        dx_p = DXPath('dx://bucket/obj')
        self.assertEquals(dx_p.resource, 'obj')

    def test_resource_single_dir(self):
        dx_p = DXPath('dx://project/dir/')
        self.assertEquals(dx_p.resource, 'dir/')

    def test_resource_nested_obj(self):
        dx_p = DXPath('dx://project/nested/obj')
        self.assertEquals(dx_p.resource, 'nested/obj')

    def test_resource_nested_dir(self):
        dx_p = DXPath('dx://project/nested/dir/')
        self.assertEquals(dx_p.resource, 'nested/dir/')


class TestGetDXConnectionCreds(unittest.TestCase):
    def test_login_token(self):
        dx_p = DXPath('dx://tenant/')
        with settings.use({'dx': {'dx_login_token': ''}}):
            #TODO(akumar) move errors to dx class
            with self.assertRaises(swift.ConfigurationError):
                dx_p._get_dx_connection_vars()


class TestDXFile(DXTestCase):

    def test_read_on_open_file(self):
        d = dxpy.bindings.dxfile_functions.new_dxfile()
        self.assertEqual(d.describe()['state'], 'open')

        dx_p = DXPath('dx://{}/{}'.format(self.project, d.name))
        with self.assertRaisesRegexp(ValueError, 'not in closed state'):
            dx_p.read_object()

        d.remove()

    def test_read_success_on_closed_file(self):
        dx_p = DXPath('dx://{}/{}'.format(self.project, self.file_handler.name))
        self.assertEquals(dx_p.read_object(), b'data')
        self.assertEquals(dx_p.open().read(), 'data')

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
        self.assertEquals(dx_p.open('r').read(), data.decode('ascii'))
        # open().read() should return bytes for rb
        self.assertEquals(dx_p.open('rb').read(), data)
        self.assertEquals(dx_p.open().readlines(),
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


class TestDXShared(SharedOBSFileCases, DXTestCase):
    drive = 'dx://'
    path_class = DXPath
    normal_path = DXPath('dx://project/obj')


class TestTempURL(DXTestCase):
    def test_success(self):
        dx_p = DXPath('dx://{}/{}'.format(self.project, self.file_handler.name))
        temp_url = dx_p.temp_url()
        #TODO (akumar) what is the expected_url? DXFile.get_download_url doesn't seem to work
        # expected = 'https://swift.com/v1/tenant/container/obj?temp_url_sig=3b1adff9452165103716d308da692e6ec9c2d55f&temp_url_expires=1456229100&inline'  # nopep8
        self.assertIn(str(self.file_handler.name / self.project), temp_url)


class TestList(DXTestCase):

    def test_no_project_error(self):
        dx_p = DXPath('dx://')
        with self.assertRaises(ValueError):
            list(dx_p.list())

    def test_list_project(self):
        mock_list = self.mock_dx_conn.get_iterator
        mock_list.return_value = ({}, [{
            'name': 'path/to/resource1'
        }, {
            'name': 'path/to/resource2'
        }])
        dx_p = DXPath('dx://test-project')
        results = dx_p.list()

        self.assertDXListsEqual(results, [
            'dx://test-project/path/to/resource1',
            'dx://test-project/path/to/resourc2'
        ])

    @mock.patch('time.sleep', autospec=True)
    def test_list_unavailable(self, mock_sleep):
        mock_list = self.mock_dx_conn.get_iterator
        mock_list.side_effect = [
            DXException('unavaiable', http_status=503),
            ({}, [{
                'name': 'path/to/resource1'
            }, {
                'name': 'path/to/resource2'
            }])
        ]

        dx_p = DXPath('dx://project/path')
        results = dx_p.list()
        self.assertDXListsEqual(results, [
            'dx://project/path/to/resource1',
            'dx://project/path/to/resource2'
        ])

        # Verify that list was retried one time
        # self.assertEquals(len(mock_list.call_args_list), 2)

    @mock.patch('time.sleep', autospec=True)
    def test_list_unauthorized(self, mock_sleep):
        mock_list = self.mock_dx_conn.get_iterator
        mock_list.side_effect = DXException(
            'unauthorized', http_status=403, http_response_headers={'X-Trans-Id': 'transactionid'})

        dx_p = DXPath('dx://project/path')
        # TODO(akumar) move errors to dx class
        with self.assertRaises(swift.UnauthorizedError):
            try:
                dx_p.list()
            except swift.UnauthorizedError as exc:
                self.assertIn('X-Trans-Id: transactionid', str(exc))
                self.assertIn('X-Trans-Id: transactionid', repr(exc))
                raise

    def test_listdir(self):
        mock_list = self.mock_dx_conn.get_iterator
        mock_list.return_value = ({}, [{
            'subdir': 'path/to/resource1/'
        }, {
            'name': 'path/to/resource1'
        }, {
            'name': 'path/to/resource2'
        }, {
            'name': 'path/to/resource3'
        }])

        dx_p = DXPath('dx://project/path/to')
        results = list(dx_p.listdir())
        self.assertDXListsEqual(results, [
            'dx://project/path/to/resource1',
            'dx://project/path/to/resource2',
            'dx://project/path/to/resource3'
        ])


    @mock.patch('os.path', ntpath)
    def test_list_windows(self):
        mock_list = self.mock_dx_conn.get_iterator
        mock_list.return_value = ({}, [{
            'name': 'path/to/resource1'
        }, {
            'name': 'path/to/resource2'
        }, {
            'name': 'path/to/resource3'
        }, {
            'name': 'path/to/resource4'
        }])

        dx_p = DXPath('dx://project/path')
        results = list(dx_p.list())
        self.assertDXListsEqual(results, [
            'dx://project/path/to/resource1',
            'dx://project/path/to/resource2',
            'dx://project/path/to/resource3',
            'dx://project/path/to/resource4'
        ])

    def test_list_limit(self):
        mock_list = self.mock_dx_conn.get_iterator
        mock_list.return_value = ({}, [{
            'name': 'path/to/resource1'
        }])

        dx_p = DXPath('dx://project/path')
        results = list(dx_p.list(limit=1))
        self.assertEquals(results, [
            'dx://project/path/to/resource1'
        ])

    def test_list_starts_with(self):
        mock_list = self.mock_dx_conn.get_iterator
        mock_list.return_value = ({}, [{
            'name': 'r1'
        }, {
            'name': 'r2'
        }])
        dx_p = DXPath('dx://project/r')
        results = list(dx_p.list(starts_with='prefix'))
        self.assertDXListsEqual(results, [
            'dx://project/r1',
            'dx://project/r2'
        ])
        mock_list.assert_called_once_with('project',
                                          prefix='r/prefix',
                                          limit=None,
                                          full_listing=True)

    def test_list_starts_with_no_resource(self):
        mock_list = self.mock_dx_conn.get_iterator
        mock_list.return_value = ({}, [{
            'name': 'r1'
        }, {
            'name': 'r2'
        }])
        dx_p = DXPath('dx://project')
        results = list(dx_p.list(starts_with='prefix'))
        self.assertDXListsEqual(results, [
            'dx://project/r1',
            'dx://project/r2'
        ])
        mock_list.assert_called_once_with('container',
                                          prefix='prefix',
                                          limit=None,
                                          full_listing=True)


class TestWalkFiles(DXTestCase):
    def test_no_pattern_w_dir_markers(self):
        mock_list = self.mock_dx_conn.get_iterator
        mock_list.return_value = ({}, [{
            'name': 'my/obj1',
            'content_type': 'application/directory'
        }, {
            'name': 'my/obj2',
            'content_type': 'text/directory'
        }, {
            'name': 'my/obj3',
            'content_type': 'application/octet-stream'
        }, {
            'name': 'my/obj4',
            'content_type': 'application/octet-stream'
        }])

        f = list(DXPath('dx://project/').walkfiles())
        self.assertEquals(set(f), set([
            DXPath('dx://project/my/obj3'),
            DXPath('dx://project/my/obj4')
        ]))

    def test_w_pattern_w_dir_markers(self):
        mock_list = self.mock_dx_conn.get_iterator
        mock_list.return_value = ({}, [{
            'name': 'my/obj1',
            'content_type': 'application/directory'
        }, {
            'name': 'my/obj2',
            'content_type': 'text/directory'
        }, {
            'name': 'my/obj3',
            'content_type': 'application/octet-stream'
        }, {
            'name': 'my/obj4.sh',
            'content_type': 'application/octet-stream'
        }, {
            'name': 'my/other/obj5.sh',
            'content_type': 'application/octet-stream'
        }, {
            'name': 'my/dirwithpattern.sh/obj6',
            'content_type': 'application/octet-stream'
        }])

        f = list(DXPath('dx://project').walkfiles('*.sh'))
        self.assertEquals(set(f), set([
            DXPath('dx://project/my/obj4.sh'),
            DXPath('dx://project/my/other/obj5.sh')
        ]))


@mock.patch.object(DXPath, 'list', autospec=True)
class TestGlob(DXTestCase):
    def test_valid_pattern(self, mock_list): 
        dx_p = DXPath('dx://project')
        dx_p.glob('pattern*')
        mock_list.assert_called_once_with(mock.ANY, starts_with='pattern')

    def test_valid_pattern_wo_wildcard(self, mock_list):
        dx_p = DXPath('dx://project')
        dx_p.glob('pattern')
        mock_list.assert_called_once_with(mock.ANY, starts_with='pattern')

    def test_multi_glob_pattern(self, mock_list):
        dx_p = DXPath('dx://project')
        with self.assertRaises(ValueError):
            dx_p.glob('*invalid_pattern*', condition=None)

    def test_invalid_glob_pattern(self, mock_list):
        dx_p = DXPath('dx://project')
        with self.assertRaises(ValueError):
            dx_p.glob('invalid_*pattern', condition=None)

    # TODO(akumar) add tests for retries


# TODO(akumar) insert mocks here
class TestStat(DXTestCase):
    def test_stat_failure(self):
        with self.assertRaises(ValueError):
            DXPath('dx://Test_Project:/Test_Folder/').stat()
        with self.assertRaises(ValueError):
            DXPath('dx://Test_Project:/Test_File_No_Ext').stat()
        with self.assertRaises(dx.DuplicateError):
            DXPath('dx://Duplicate_Project:').stat()
        with self.assertRaises(dx.NotFoundError):
            DXPath('dx://Random_Proj:/').stat()

    def test_stat_project(self):
        dx_p = DXPath('dx://Test_Project:/')
        response = dx_p.stat()
        self.assertIn('region', response)  # only projects have regions

    def test_stat_file(self):
        dx_p = DXPath('dx://Test_Project:/Test_File.txt')
        response = dx_p.stat()
        self.assertIn('folder', response)  # only files have folders
