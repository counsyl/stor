import cStringIO
import gzip
import logging
import ntpath
import os
from tempfile import NamedTemporaryFile
import unittest

import freezegun
import mock
from swiftclient.exceptions import ClientException
from swiftclient.service import SwiftError
from testfixtures import LogCapture

import stor
from stor import exceptions
from stor import NamedTemporaryDirectory
from stor import Path
from stor import settings
from stor import swift
from stor import utils
from stor.swift import SwiftPath
from stor.test import SwiftTestCase
from stor.tests.shared import assert_same_data


def _service_404_exception():
    return ClientException('dummy', http_status=404)


def _make_stat_response(stat_response=None):
    "Fills in response for stat service to avoid mocking up as much data"
    defaults = {
        'headers': {},
        'container': None,
        'success': True,
        'action': '',
        'items': [
            ('ETag', 'dummy'),
            ('Size', 0)
        ],
        'object': None
    }
    if stat_response:
        defaults.update(**stat_response)
    return [defaults]


class TestPathcedGetAuthKeystone(unittest.TestCase):
    @mock.patch('stor.swift.real_get_auth_keystone', autospec=True)
    def test_patched_get_auth_keystone(self, mock_get_real_auth_keystone):
        mock_get_real_auth_keystone.side_effect = Exception
        os_options = {'auth_token': 'token'}
        with self.assertRaises(Exception):
            swift.patched_get_auth_keystone('auth_url', 'user', 'key', os_options)
        mock_get_real_auth_keystone.assert_called_once_with(
            'auth_url', 'user', 'key', os_options)
        # Verify the os_options were modified to pop the auth token
        self.assertEquals(os_options, {})


class TestBasicPathMethods(unittest.TestCase):
    def test_name(self):
        p = Path('swift://tenant/container/path/to/resource')
        self.assertEquals(p.name, 'resource')

    def test_parent(self):
        p = Path('swift://tenant/container/path/to/resource')
        self.assertEquals(p.parent, 'swift://tenant/container/path/to')

    def test_dirname(self):
        p = Path('swift://tenant/container/path/to/resource')
        self.assertEquals(p.dirname(), 'swift://tenant/container/path/to')

    def test_basename(self):
        p = Path('swift://tenant/container/path/to/resource')
        self.assertEquals(p.basename(), 'resource')


class TestManifestUtilities(unittest.TestCase):
    def test_generate_save_and_read_manifest(self):
        with NamedTemporaryDirectory() as tmp_d:
            manifest_contents = ['obj1', 'dir/obj2']
            utils.generate_and_save_data_manifest(tmp_d, manifest_contents)
            read_contents = utils.get_data_manifest_contents(tmp_d)

            self.assertEquals(set(manifest_contents), set(read_contents))


class TestNew(SwiftTestCase):
    def test_failed_new(self):
        with self.assertRaises(ValueError):
            SwiftPath('/bad/swift/path')

    def test_successful_new(self):
        swift_p = SwiftPath('swift://tenant/container/path')
        self.assertEquals(swift_p, 'swift://tenant/container/path')


class TestRepr(SwiftTestCase):
    def test_repr(self):
        swift_p = SwiftPath('swift://t/c/p')
        self.assertEquals(eval(repr(swift_p)), swift_p)


class TestPathManipulations(SwiftTestCase):
    def test_add(self):
        swift_p = SwiftPath('swift://a')
        swift_p = swift_p + 'b' + Path('c')
        self.assertTrue(isinstance(swift_p, SwiftPath))
        self.assertEquals(swift_p, 'swift://abc')

    def test_div(self):
        swift_p = SwiftPath('swift://t')
        swift_p = swift_p / 'c' / Path('p')
        self.assertTrue(isinstance(swift_p, SwiftPath))
        self.assertEquals(swift_p, 'swift://t/c/p')


class TestTenant(SwiftTestCase):
    def test_tenant_none(self):
        swift_p = SwiftPath('swift://')
        self.assertIsNone(swift_p.tenant)

    def test_tenant_wo_container(self):
        swift_p = SwiftPath('swift://tenant')
        self.assertEquals(swift_p.tenant, 'tenant')

    def test_tenant_w_container(self):
        swift_p = SwiftPath('swift://tenant/container')
        self.assertEquals(swift_p.tenant, 'tenant')


class TestContainer(SwiftTestCase):
    def test_container_none(self):
        swift_p = SwiftPath('swift://tenant/')
        self.assertIsNone(swift_p.container)

    def test_container_wo_resource(self):
        swift_p = SwiftPath('swift://tenant/container')
        self.assertEquals(swift_p.container, 'container')

    def test_container_w_resource(self):
        swift_p = SwiftPath('swift://tenant/container/resource/path')
        self.assertEquals(swift_p.container, 'container')


class TestResource(SwiftTestCase):
    def test_no_container(self):
        swift_p = SwiftPath('swift://tenant/')
        self.assertIsNone(swift_p.resource)

    def test_w_container_no_resource(self):
        swift_p = SwiftPath('swift://tenant/container/')
        self.assertIsNone(swift_p.resource)

    def test_resource_single_object(self):
        swift_p = SwiftPath('swift://tenant/container/obj')
        self.assertEquals(swift_p.resource, 'obj')

    def test_resource_single_dir_w_slash(self):
        swift_p = SwiftPath('swift://tenant/container/dir/')
        self.assertEquals(swift_p.resource, 'dir/')

    def test_resource_nested_dir_wo_slash(self):
        swift_p = SwiftPath('swift://tenant/container/nested/dir')
        self.assertEquals(swift_p.resource, 'nested/dir')


class TestGetSwiftConnectionOptions(SwiftTestCase):
    def test_wo_username(self):
        swift_p = SwiftPath('swift://tenant/')
        with settings.use({'swift': {'username': ''}}):
            with self.assertRaises(swift.ConfigurationError):
                swift_p._get_swift_connection_options()

    def test_wo_password(self):
        swift_p = SwiftPath('swift://tenant/')
        with settings.use({'swift': {'password': ''}}):
            with self.assertRaises(swift.ConfigurationError):
                swift_p._get_swift_connection_options()

    def test_w_update_settings(self):
        new_settings = {
            'swift': {
                'auth_url': 'new_auth_url',
                'password': 'new_password',
                'username': 'new_username'
            }
        }
        with settings.use(new_settings):
            swift_p = SwiftPath('swift://tenant/')
            options = swift_p._get_swift_connection_options()
        self.assertEquals(options['os_auth_url'], 'new_auth_url')
        self.assertEquals(options['os_username'], 'new_username')
        self.assertEquals(options['os_password'], 'new_password')
        self.assertEquals(options['os_tenant_name'], 'tenant')


@mock.patch.object(SwiftPath, '_get_swift_connection_options',
                   autospec=True)
class TestGetSwiftService(SwiftTestCase):
    def test_get_swift_service(self, mock_get_swift_connection_options):
        self.disable_get_swift_service_mock()

        mock_get_swift_connection_options.return_value = {'option': 'value'}
        swift_p = SwiftPath('swift://tenant/')
        swift_p._get_swift_service()
        self.mock_swift_service.assert_called_once_with({'option': 'value'})


@mock.patch.object(SwiftPath, '_get_swift_connection_options',
                   autospec=True)
class TestGetSwiftConnection(SwiftTestCase):
    def test_get_swift_connection(self, mock_get_swift_connection_options):
        mock_get_swift_connection_options.return_value = {'option': 'value'}
        swift_p = SwiftPath('swift://tenant/')
        swift_p._get_swift_connection()
        self.mock_swift_get_conn.assert_called_once_with({'option': 'value'})


class TestSwiftFile(SwiftTestCase):
    def setUp(self):
        super(TestSwiftFile, self).setUp()
        settings.update({'swift': {'num_retries': 5}})

    def test_invalid_buffer_mode(self):
        swift_f = SwiftPath('swift://tenant/container/obj').open()
        swift_f.mode = 'invalid'
        with self.assertRaisesRegexp(ValueError, 'buffer'):
            swift_f._buffer

    def test_invalid_flush_mode(self):
        self.mock_swift_conn.get_object.return_value = ('header', 'data')
        swift_p = SwiftPath('swift://tenant/container/obj')
        obj = swift_p.open()
        with self.assertRaisesRegexp(TypeError, 'flush'):
            obj.flush()

    def test_name(self):
        swift_p = SwiftPath('swift://tenant/container/obj')
        obj = swift_p.open()
        self.assertEquals(obj.name, swift_p)

    def test_context_manager_on_closed_file(self):
        self.mock_swift_conn.get_object.return_value = ('header', 'data')
        swift_p = SwiftPath('swift://tenant/container/obj')
        obj = swift_p.open()
        obj.close()

        with self.assertRaisesRegexp(ValueError, 'closed file'):
            with obj:
                pass  # pragma: no cover

    def test_invalid_mode(self):
        swift_p = SwiftPath('swift://tenant/container/obj')
        with self.assertRaisesRegexp(ValueError, 'invalid mode'):
            swift_p.open(mode='invalid')

    def test_invalid_io_op(self):
        # now invalid delegates are considered invalid on instantiation
        with self.assertRaisesRegexp(AttributeError, 'no attribute'):
            class MyFile(object):
                closed = False
                _buffer = cStringIO.StringIO()
                invalid = swift._delegate_to_buffer('invalid')

    def test_read_on_closed_file(self):
        self.mock_swift_conn.get_object.return_value = ('header', 'data')
        swift_p = SwiftPath('swift://tenant/container/obj')
        obj = swift_p.open()
        obj.close()

        with self.assertRaisesRegexp(ValueError, 'closed file'):
            obj.read()

    def test_read_invalid_mode(self):
        swift_p = SwiftPath('swift://tenant/container/obj')
        with self.assertRaisesRegexp(TypeError, 'mode.*read'):
            swift_p.open(mode='wb').read()

    def test_read_success(self):
        self.mock_swift_conn.get_object.return_value = ('header', 'data')

        swift_p = SwiftPath('swift://tenant/container/obj')
        self.assertEquals(swift_p.open().read(), 'data')

    def test_iterating_over_files(self):
        data = '''\
line1
line2
line3
line4
'''
        self.mock_swift_conn.get_object.return_value = ('header', data)

        swift_p = SwiftPath('swift://tenant/container/obj')
        self.assertEquals(swift_p.open().read(), data)
        self.assertEquals(swift_p.open().readlines(),
                          [l + '\n' for l in data.split('\n')][:-1])
        for i, line in enumerate(swift_p.open(), 1):
            self.assertEqual(line, 'line%d\n' % i)

        self.assertEqual(next(swift_p.open()), 'line1\n')
        self.assertEqual(swift_p.open().next(), 'line1\n')
        self.assertEqual(iter(swift_p.open()).next(), 'line1\n')

    @mock.patch('time.sleep', autospec=True)
    def test_read_success_on_second_try(self, mock_sleep):
        self.mock_swift_conn.get_object.side_effect = [
            ClientException('dummy', 'dummy', http_status=404),
            ('header', 'data')
        ]
        swift_p = SwiftPath('swift://tenant/container/obj')
        obj = swift_p.open()
        self.assertEquals(obj.read(), 'data')
        self.assertEquals(len(mock_sleep.call_args_list), 1)

    def test_write_invalid_args(self):
        swift_p = SwiftPath('swift://tenant/container/obj')
        obj = swift_p.open(mode='r')
        with self.assertRaisesRegexp(TypeError, 'mode.*write'):
            obj.write('hello')

    @mock.patch('time.sleep', autospec=True)
    @mock.patch.object(SwiftPath, 'upload', autospec=True)
    def test_write_use_slo_multiple_and_close(self, mock_upload, mock_sleep):
        with NamedTemporaryFile(delete=False) as fp:
            with mock.patch('tempfile.NamedTemporaryFile',
                            autospec=True) as ntf_mock:
                ntf_mock.side_effect = [fp]
                swift_p = SwiftPath('swift://tenant/container/obj')
                obj = swift_p.open(mode='wb', swift_upload_options={
                    'use_slo': 'test_value'
                })
                obj.write('hello')
                obj.write(' world')
                obj.close()
            upload, = mock_upload.call_args_list
            self.assertEquals(upload[0][1][0].source, fp.name)
            self.assertEquals(upload[0][1][0].object_name, swift_p.resource)
            self.assertEquals(upload[1]['use_slo'], 'test_value')
            self.assertEqual(open(fp.name).read(), 'hello world')

    @mock.patch('time.sleep', autospec=True)
    @mock.patch.object(SwiftPath, 'upload', autospec=True)
    def test_write_multiple_w_context_manager(self, mock_upload, mock_sleep):
        swift_p = SwiftPath('swift://tenant/container/obj')
        with swift_p.open(mode='wb') as obj:
            obj.write('hello')
            obj.write(' world')
        upload_call, = mock_upload.call_args_list

    @mock.patch('time.sleep', autospec=True)
    @mock.patch.object(SwiftPath, 'upload', autospec=True)
    def test_write_multiple_flush_multiple_upload(self, mock_upload,
                                                  mock_sleep):
        swift_p = SwiftPath('swift://tenant/container/obj')
        with NamedTemporaryFile(delete=False) as ntf1,\
                NamedTemporaryFile(delete=False) as ntf2,\
                NamedTemporaryFile(delete=False) as ntf3:
            with mock.patch('tempfile.NamedTemporaryFile', autospec=True) as ntf:
                ntf.side_effect = [ntf1, ntf2, ntf3]
                with swift_p.open(mode='wb') as obj:
                    obj.write('hello')
                    obj.flush()
                    obj.write(' world')
                    obj.flush()
                u1, u2, u3 = mock_upload.call_args_list
                u1[0][1][0].source == ntf1.name
                u2[0][1][0].source == ntf2.name
                u3[0][1][0].source == ntf3.name
                u1[0][1][0].object_name == swift_p.resource
                u2[0][1][0].object_name == swift_p.resource
                u3[0][1][0].object_name == swift_p.resource
                self.assertEqual(open(ntf1.name).read(), 'hello')
                self.assertEqual(open(ntf2.name).read(), 'hello world')
                # third call happens because we don't care about checking for
                # additional file change
                self.assertEqual(open(ntf3.name).read(), 'hello world')

    @mock.patch('time.sleep', autospec=True)
    @mock.patch.object(SwiftPath, 'upload', autospec=True)
    def test_close_no_writes(self, mock_upload, mock_sleep):
        swift_p = SwiftPath('swift://tenant/container/obj')
        obj = swift_p.open(mode='wb')
        obj.close()

        self.assertFalse(mock_upload.called)

    def test_works_with_gzip(self):
        gzip_path = stor.join(stor.dirname(__file__),
                              'file_data', 's_3_2126.bcl.gz')
        text = stor.open(gzip_path).read()
        with mock.patch.object(SwiftPath, 'read_object', autospec=True) as read_mock:
            read_mock.return_value = text
            swift_file = stor.open('swift://A/C/s_3_2126.bcl.gz')

            with gzip.GzipFile(fileobj=swift_file) as swift_file_fp:
                with gzip.open(gzip_path) as gzip_fp:
                    assert_same_data(swift_file_fp, gzip_fp)
            swift_file = stor.open('swift://A/C/s_3_2126.bcl.gz')
            with gzip.GzipFile(fileobj=swift_file) as swift_file_fp:
                with gzip.open(gzip_path) as gzip_fp:
                    # after seeking should still be same
                    swift_file_fp.seek(3)
                    gzip_fp.seek(3)
                    assert_same_data(swift_file_fp, gzip_fp)


class TestTempURL(SwiftTestCase):
    @freezegun.freeze_time('2016-02-23 12:00:00')
    def test_success(self):
        temp_settings = {
            'swift': {
                'temp_url_key': 'temp_key',
                'auth_url': 'https://swift.com/auth/v1/'
            }
        }
        with settings.use(temp_settings):
            temp_url = SwiftPath('swift://tenant/container/obj').temp_url()
        expected = 'https://swift.com/v1/tenant/container/obj?temp_url_sig=3b1adff9452165103716d308da692e6ec9c2d55f&temp_url_expires=1456229100&inline'  # nopep8
        self.assertEquals(temp_url, expected)

    @freezegun.freeze_time('2016-02-23 12:00:00')
    def test_success_w_inline(self):
        temp_settings = {
            'swift': {
                'temp_url_key': 'temp_key',
                'auth_url': 'https://swift.com/auth/v1/'
            }
        }
        with settings.use(temp_settings):
            temp_url = SwiftPath('swift://tenant/container/obj').temp_url(inline=False)
            expected = 'https://swift.com/v1/tenant/container/obj?temp_url_sig=3b1adff9452165103716d308da692e6ec9c2d55f&temp_url_expires=1456229100'  # nopep8
            self.assertEquals(temp_url, expected)
            temp_url = SwiftPath('swift://tenant/container/obj').temp_url(inline=True)
            expected = 'https://swift.com/v1/tenant/container/obj?temp_url_sig=3b1adff9452165103716d308da692e6ec9c2d55f&temp_url_expires=1456229100&inline'  # nopep8
            self.assertEquals(temp_url, expected)

    @freezegun.freeze_time('2016-02-23 12:00:00')
    def test_success_w_inline_and_filename(self):
        temp_settings = {
            'swift': {
                'temp_url_key': 'temp_key',
                'auth_url': 'https://swift.com/auth/v1/'
            }
        }
        with settings.use(temp_settings):
            temp_url = SwiftPath('swift://tenant/container/obj').temp_url(inline=True, filename='file')  # nopep8
        expected = 'https://swift.com/v1/tenant/container/obj?temp_url_sig=3b1adff9452165103716d308da692e6ec9c2d55f&temp_url_expires=1456229100&inline&filename=file'  # nopep8
        self.assertEquals(temp_url, expected)

    def test_no_obj(self):
        temp_settings = {
            'swift': {
                'temp_url_key': 'temp_key',
                'auth_url': 'https://swift.com/auth/v1/'
            }
        }
        with settings.use(temp_settings):
            with self.assertRaisesRegexp(ValueError, 'on object'):
                SwiftPath('swift://tenant/container').temp_url()

    def test_no_auth_url(self):
        temp_settings = {
            'swift': {
                'temp_url_key': 'temp_key',
                'auth_url': ''
            }
        }
        with settings.use(temp_settings):
            with self.assertRaisesRegexp(ValueError, 'auth url'):
                SwiftPath('swift://tenant/container/obj').temp_url()

    def test_no_temp_url_key(self):
        temp_settings = {
            'swift': {
                'temp_url_key': '',
                'auth_url': 'https://swift.com/auth/v1/'
            }
        }
        with settings.use(temp_settings):
            with self.assertRaisesRegexp(ValueError, 'temporary url key'):
                SwiftPath('swift://tenant/container/obj').temp_url()


class TestList(SwiftTestCase):
    def setUp(self):
        super(TestList, self).setUp()
        settings.update({'swift': {'num_retries': 5}})

    def test_error(self):
        mock_list = self.mock_swift_conn.get_container
        mock_list.side_effect = ValueError

        swift_p = SwiftPath('swift://tenant/container')
        with self.assertRaises(ValueError):
            list(swift_p.list())
        mock_list.assert_called_once_with('container', prefix=None,
                                          limit=None, full_listing=True)

    @mock.patch('time.sleep', autospec=True)
    def test_list_condition_not_met(self, mock_sleep):
        mock_list = self.mock_swift_conn.get_container
        mock_list.return_value = ({}, [{
            'name': 'path/to/resource1'
        }, {
            'name': 'path/to/resource2'
        }])

        swift_p = SwiftPath('swift://tenant/container/path')
        with self.assertRaises(exceptions.ConditionNotMetError):
            swift_p.list(condition=lambda results: len(results) == 3)

        # Verify that list was retried at least once
        self.assertTrue(len(mock_list.call_args_list) > 1)

    @mock.patch('time.sleep', autospec=True)
    def test_list_unavailable(self, mock_sleep):
        mock_list = self.mock_swift_conn.get_container
        mock_list.side_effect = [
            ClientException('unavaiable', http_status=503),
            ({}, [{
                'name': 'path/to/resource1'
            }, {
                'name': 'path/to/resource2'
            }])
        ]

        swift_p = SwiftPath('swift://tenant/container/path')
        results = swift_p.list()
        self.assertSwiftListResultsEqual(results, [
            'swift://tenant/container/path/to/resource1',
            'swift://tenant/container/path/to/resource2'
        ])

        # Verify that list was retried one time
        self.assertEquals(len(mock_list.call_args_list), 2)

    def test_list_authentication_error(self):
        mock_list = self.mock_swift_conn.get_container
        mock_list.side_effect = ClientException(
            'Unauthorized. Check username, password and tenant name/id.',
            http_status=None)

        swift_p = SwiftPath('swift://tenant/container/path')
        with self.assertRaises(swift.AuthenticationError):
            swift_p.list()

    @mock.patch('time.sleep', autospec=True)
    def test_list_unauthorized(self, mock_sleep):
        mock_list = self.mock_swift_conn.get_container
        mock_list.side_effect = ClientException('unauthorized',
                                                http_status=403)

        swift_p = SwiftPath('swift://tenant/container/path')
        with self.assertRaises(swift.UnauthorizedError):
            swift_p.list()

    @mock.patch('time.sleep', autospec=True)
    def test_list_condition_not_met_custom_retry_logic(self, mock_sleep):
        mock_list = self.mock_swift_conn.get_container
        mock_list.return_value = ({}, [{
            'name': 'path/to/resource1'
        }, {
            'name': 'path/to/resource2'
        }])

        swift_p = SwiftPath('swift://tenant/container/path')
        with self.assertRaises(exceptions.ConditionNotMetError):
            swift_p.list(
                condition=lambda results: len(results) == 3,
                num_retries=5,
                initial_retry_sleep=100,
                retry_sleep_function=lambda t, attempt: t + 1)

        # Verify the dynamic retry
        self.assertTrue(len(mock_list.call_args_list), 5)
        self.assertEquals(mock_sleep.call_args_list, [
            mock.call(100),
            mock.call(101),
            mock.call(102),
            mock.call(103),
            mock.call(104)
        ])

    @mock.patch('time.sleep', autospec=True)
    def test_list_condition_met_on_second_try(self, mock_sleep):
        mock_list = self.mock_swift_conn.get_container
        mock_list.side_effect = [
            ({}, [{
                'name': 'path/to/resource1'
            }]),
            ({}, [{
                'name': 'path/to/resource1'
            }, {
                'name': 'path/to/resource2'
            }])
        ]

        swift_p = SwiftPath('swift://tenant/container/path')
        results = swift_p.list(condition=lambda results: len(results) > 1)
        self.assertSwiftListResultsEqual(results, [
            'swift://tenant/container/path/to/resource1',
            'swift://tenant/container/path/to/resource2'
        ])

        # Verify that list was retried once
        self.assertEquals(len(mock_list.call_args_list), 2)

    @mock.patch('time.sleep', autospec=True)
    def test_list_has_paths_condition_met_on_second_try(self, mock_sleep):
        mock_list = self.mock_swift_conn.get_container
        mock_list.side_effect = [
            ({}, [{
                'name': 'path/to/resource1'
            }]),
            ({}, [{
                'name': 'path/to/resource1'
            }, {
                'name': 'path/to/resource2'
            }])
        ]

        swift_p = SwiftPath('swift://tenant/container/path')
        expected_paths = set(['swift://tenant/container/path/to/resource1',
                              'swift://tenant/container/path/to/resource2'])
        results = swift_p.list(condition=lambda results: expected_paths.issubset(results))
        self.assertSwiftListResultsEqual(results, [
            'swift://tenant/container/path/to/resource1',
            'swift://tenant/container/path/to/resource2'
        ])

        # Verify that list was retried once
        self.assertEquals(len(mock_list.call_args_list), 2)

    def test_listdir(self):
        mock_list = self.mock_swift_conn.get_container
        mock_list.return_value = ({}, [{
            'subdir': 'path/to/resource1/'
        }, {
            'name': 'path/to/resource1'
        }, {
            'name': 'path/to/resource2'
        }, {
            'name': 'path/to/resource3'
        }])

        swift_p = SwiftPath('swift://tenant/container/path/to')
        results = list(swift_p.listdir())
        self.assertSwiftListResultsEqual(results, [
            'swift://tenant/container/path/to/resource1',
            'swift://tenant/container/path/to/resource2',
            'swift://tenant/container/path/to/resource3'
        ])
        mock_list.assert_called_once_with('container',
                                          limit=None,
                                          prefix='path/to/',
                                          full_listing=True,
                                          delimiter='/')

    def test_listdir_ignore_segment_containers(self):
        mock_list = self.mock_swift_conn.get_container
        mock_list.return_value = ({}, [{
            'subdir': 'path/to/resource1/'
        }, {
            'name': 'path/to/resource1'
        }, {
            'name': 'path/to/resource2'
        }, {
            'name': 'path/to/resource3'
        }])

        swift_p = SwiftPath('swift://tenant/container_segments/path/to')
        results = list(swift_p.listdir(ignore_segment_containers=True))
        # Note that objects in segment containers will not be ignored, so
        # all results should be returned
        self.assertSwiftListResultsEqual(results, [
            'swift://tenant/container_segments/path/to/resource1',
            'swift://tenant/container_segments/path/to/resource2',
            'swift://tenant/container_segments/path/to/resource3'
        ])
        mock_list.assert_called_once_with('container_segments',
                                          limit=None,
                                          prefix='path/to/',
                                          full_listing=True,
                                          delimiter='/')

    def test_listdir_on_container(self):
        mock_list = self.mock_swift_conn.get_container
        mock_list.return_value = ({}, [{
            'subdir': 'resource1/'
        }, {
            'name': 'resource1'
        }, {
            'name': 'resource2'
        }, {
            'name': 'resource3'
        }])

        swift_p = SwiftPath('swift://tenant/container/')
        results = list(swift_p.listdir())
        self.assertSwiftListResultsEqual(results, [
            'swift://tenant/container/resource1',
            'swift://tenant/container/resource2',
            'swift://tenant/container/resource3'
        ])
        mock_list.assert_called_once_with('container',
                                          limit=None,
                                          prefix=None,
                                          full_listing=True,
                                          delimiter='/')

    def test_listdir_on_tenant_allow_segment_containers(self):
        mock_list = self.mock_swift_conn.get_account
        mock_list.return_value = ({}, [{
            'name': 'container1'
        }, {
            'name': 'container2_segments'
        }, {
            'name': '.segments_container3'
        }])

        swift_p = SwiftPath('swift://tenant/')
        results = list(swift_p.listdir(ignore_segment_containers=False))
        self.assertSwiftListResultsEqual(results, [
            'swift://tenant/container1',
            'swift://tenant/container2_segments',
            'swift://tenant/.segments_container3'
        ])
        mock_list.assert_called_once_with(limit=None,
                                          prefix=None,
                                          full_listing=True)

    def test_listdir_on_tenant_ignore_segment_containers(self):
        mock_list = self.mock_swift_conn.get_account
        mock_list.return_value = ({}, [{
            'name': 'container1'
        }, {
            'name': 'container2_segments'
        }, {
            'name': '.segments_container3'
        }])

        swift_p = SwiftPath('swift://tenant/')
        results = list(swift_p.listdir(ignore_segment_containers=True))
        self.assertSwiftListResultsEqual(results, [
            'swift://tenant/container1'
        ])
        mock_list.assert_called_once_with(limit=None,
                                          prefix=None,
                                          full_listing=True)

    @mock.patch('os.path', ntpath)
    def test_list_windows(self):
        mock_list = self.mock_swift_conn.get_container
        mock_list.return_value = ({}, [{
            'name': 'path/to/resource1'
        }, {
            'name': 'path/to/resource2'
        }, {
            'name': 'path/to/resource3'
        }, {
            'name': 'path/to/resource4'
        }])

        swift_p = SwiftPath('swift://tenant/container/path')
        results = list(swift_p.list())
        self.assertSwiftListResultsEqual(results, [
            'swift://tenant/container/path/to/resource1',
            'swift://tenant/container/path/to/resource2',
            'swift://tenant/container/path/to/resource3',
            'swift://tenant/container/path/to/resource4'
        ])
        mock_list.assert_called_once_with('container',
                                          limit=None,
                                          prefix='path',
                                          full_listing=True)

    def test_list_multiple_return(self):
        mock_list = self.mock_swift_conn.get_container
        mock_list.return_value = ({}, [{
            'name': 'path/to/resource1'
        }, {
            'name': 'path/to/resource2'
        }, {
            'name': 'path/to/resource3'
        }, {
            'name': 'path/to/resource4'
        }])

        swift_p = SwiftPath('swift://tenant/container/path')
        results = list(swift_p.list())
        self.assertSwiftListResultsEqual(results, [
            'swift://tenant/container/path/to/resource1',
            'swift://tenant/container/path/to/resource2',
            'swift://tenant/container/path/to/resource3',
            'swift://tenant/container/path/to/resource4'
        ])
        mock_list.assert_called_once_with('container',
                                          limit=None,
                                          prefix='path',
                                          full_listing=True)

    def test_list_limit(self):
        mock_list = self.mock_swift_conn.get_container
        mock_list.return_value = ({}, [{
            'name': 'path/to/resource1'
        }])

        swift_p = SwiftPath('swift://tenant/container/path')
        results = list(swift_p.list(limit=1))
        self.assertEquals(results, [
            'swift://tenant/container/path/to/resource1'
        ])
        mock_list.assert_called_once_with('container',
                                          limit=1,
                                          prefix='path',
                                          full_listing=False)

    def test_list_containers(self):
        mock_list = self.mock_swift_conn.get_account
        mock_list.return_value = ({}, [{
            'name': 'container1'
        }, {
            'name': 'container2'
        }])
        swift_p = SwiftPath('swift://tenant')
        results = list(swift_p.list())
        self.assertSwiftListResultsEqual(results, [
            'swift://tenant/container1',
            'swift://tenant/container2'
        ])
        mock_list.assert_called_once_with(prefix=None,
                                          full_listing=True,
                                          limit=None)

    def test_list_starts_with(self):
        mock_list = self.mock_swift_conn.get_container
        mock_list.return_value = ({}, [{
            'name': 'r1'
        }, {
            'name': 'r2'
        }])
        swift_p = SwiftPath('swift://tenant/container/r')
        results = list(swift_p.list(starts_with='prefix'))
        self.assertSwiftListResultsEqual(results, [
            'swift://tenant/container/r1',
            'swift://tenant/container/r2'
        ])
        mock_list.assert_called_once_with('container',
                                          prefix='r/prefix',
                                          limit=None,
                                          full_listing=True)

    def test_list_starts_with_no_resource(self):
        mock_list = self.mock_swift_conn.get_container
        mock_list.return_value = ({}, [{
            'name': 'r1'
        }, {
            'name': 'r2'
        }])
        swift_p = SwiftPath('swift://tenant/container')
        results = list(swift_p.list(starts_with='prefix'))
        self.assertSwiftListResultsEqual(results, [
            'swift://tenant/container/r1',
            'swift://tenant/container/r2'
        ])
        mock_list.assert_called_once_with('container',
                                          prefix='prefix',
                                          limit=None,
                                          full_listing=True)

    @mock.patch('time.sleep', autospec=True)
    def test_list_w_condition_and_use_manifest(self, mock_sleep):
        self.mock_swift_conn.get_object.return_value = ('header', 'my/obj1\nmy/obj2\nmy/obj3\n')
        mock_list = self.mock_swift_conn.get_container
        mock_list.return_value = ({}, [{
            'name': 'my/obj1'
        }, {
            'name': 'my/obj2'
        }, {
            'name': 'my/obj3'
        }])

        swift_p = SwiftPath('swift://tenant/container/')
        results = swift_p.list(use_manifest=True, condition=lambda results: len(results) == 3)
        self.assertEquals(set(results), set([
            'swift://tenant/container/my/obj1',
            'swift://tenant/container/my/obj2',
            'swift://tenant/container/my/obj3'
        ]))

    @mock.patch('time.sleep', autospec=True)
    def test_list_use_manifest(self, mock_sleep):
        self.mock_swift_conn.get_object.return_value = ('header', 'my/obj1\nmy/obj2\nmy/obj3\n')
        mock_list = self.mock_swift_conn.get_container
        mock_list.return_value = ({}, [{
            'name': 'my/obj1'
        }, {
            'name': 'my/obj2'
        }, {
            'name': 'my/obj3'
        }])

        swift_p = SwiftPath('swift://tenant/container/')
        results = swift_p.list(use_manifest=True)
        self.assertEquals(set(results), set([
            'swift://tenant/container/my/obj1',
            'swift://tenant/container/my/obj2',
            'swift://tenant/container/my/obj3'
        ]))

    @mock.patch('time.sleep', autospec=True)
    def test_list_use_manifest_validation_err(self, mock_sleep):
        self.mock_swift_conn.get_object.return_value = ('header', 'my/obj1\nmy/obj2\nmy/obj3\n')
        mock_list = self.mock_swift_conn.get_container
        mock_list.return_value = ({}, [{
            'name': 'my/obj1'
        }, {
            'name': 'my/obj2'
        }])

        swift_p = SwiftPath('swift://tenant/container/')
        with self.assertRaises(exceptions.ConditionNotMetError):
            swift_p.list(use_manifest=True)


class TestWalkFiles(SwiftTestCase):
    def test_no_pattern_w_dir_markers(self):
        mock_list = self.mock_swift_conn.get_container
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

        f = list(SwiftPath('swift://tenant/container').walkfiles())
        self.assertEquals(set(f), set([
            SwiftPath('swift://tenant/container/my/obj3'),
            SwiftPath('swift://tenant/container/my/obj4')
        ]))

    def test_w_pattern_w_dir_markers(self):
        mock_list = self.mock_swift_conn.get_container
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

        f = list(SwiftPath('swift://tenant/container').walkfiles('*.sh'))
        self.assertEquals(set(f), set([
            SwiftPath('swift://tenant/container/my/obj4.sh'),
            SwiftPath('swift://tenant/container/my/other/obj5.sh')
        ]))


@mock.patch.object(SwiftPath, 'list', autospec=True)
class TestGlob(SwiftTestCase):
    def test_valid_pattern(self, mock_list):
        swift_p = SwiftPath('swift://tenant/container')
        swift_p.glob('pattern*')
        mock_list.assert_called_once_with(mock.ANY, starts_with='pattern',
                                          num_retries=0)

    def test_valid_pattern_wo_wildcard(self, mock_list):
        swift_p = SwiftPath('swift://tenant/container')
        swift_p.glob('pattern')
        mock_list.assert_called_once_with(mock.ANY, starts_with='pattern',
                                          num_retries=0)

    def test_multi_glob_pattern(self, mock_list):
        swift_p = SwiftPath('swift://tenant/container')
        with self.assertRaises(ValueError):
            swift_p.glob('*invalid_pattern*', condition=None)

    def test_invalid_glob_pattern(self, mock_list):
        swift_p = SwiftPath('swift://tenant/container')
        with self.assertRaises(ValueError):
            swift_p.glob('invalid_*pattern', condition=None)

    @mock.patch('time.sleep', autospec=True)
    def test_cond_not_met(self, mock_list, mock_sleep):
        mock_list.return_value = [
            SwiftPath('swift://tenant/container1'),
            SwiftPath('swift://tenant/container2')
        ]
        swift_p = SwiftPath('swift://tenant/container')
        with self.assertRaises(exceptions.ConditionNotMetError):
            swift_p.glob('pattern*',
                         condition=lambda results: len(results) > 2,
                         num_retries=3)

        # Verify that global was tried three times
        self.assertEquals(len(mock_list.call_args_list), 3)

    def test_glob_condition_met(self, mock_list):
        mock_list.return_value = [
            SwiftPath('swift://tenant/container1'),
            SwiftPath('swift://tenant/container2')
        ]
        swift_p = SwiftPath('swift://tenant/container')
        paths = swift_p.glob('pattern*',
                             condition=lambda results: len(results) == 2)
        self.assertSwiftListResultsEqual(paths, [
            SwiftPath('swift://tenant/container1'),
            SwiftPath('swift://tenant/container2')
        ])


@mock.patch.object(SwiftPath, 'list', autospec=True)
class TestFirst(SwiftTestCase):
    def test_none(self, mock_list):
        mock_list.return_value = []

        swift_p = SwiftPath('swift://tenant/container')
        result = swift_p.first()
        self.assertIsNone(result)

    def test_w_results(self, mock_list):
        mock_list.return_value = [
            SwiftPath('swift://tenant/container/path1'),
            SwiftPath('swift://tenant/container/path2'),
            SwiftPath('swift://tenant/container/path3'),
            SwiftPath('swift://tenant/container/path4'),
        ]

        swift_p = SwiftPath('swift://tenant/container/path')
        result = swift_p.first()
        self.assertEquals(result, 'swift://tenant/container/path1')


class TestExists(SwiftTestCase):
    def test_false(self):
        self.mock_swift.stat.side_effect = _service_404_exception()
        mock_list = self.mock_swift_conn.get_container
        mock_list.return_value = ({}, [])

        swift_p = SwiftPath('swift://tenant/container')
        result = swift_p.exists()
        self.assertFalse(result)
        mock_list.assert_called_once_with('container', full_listing=False,
                                          limit=1, prefix=None)

    def test_false_404(self):
        self.mock_swift.stat.side_effect = _service_404_exception()
        mock_list = self.mock_swift_conn.get_container
        mock_list.side_effect = ClientException('not found', http_status=404)

        swift_p = SwiftPath('swift://tenant/container')
        result = swift_p.exists()
        self.assertFalse(result)
        mock_list.assert_called_once_with('container', full_listing=False,
                                          limit=1, prefix=None)

    def test_raises_on_non_404_error(self):
        self.mock_swift.stat.side_effect = _service_404_exception()
        mock_list = self.mock_swift_conn.get_container
        mock_list.side_effect = ClientException('fail', http_status=504)

        swift_p = SwiftPath('swift://tenant/container')
        with self.assertRaises(swift.SwiftError):
            swift_p.exists()
        mock_list.assert_called_once_with('container', full_listing=False,
                                          limit=1, prefix=None)

    def test_true_file(self):
        self.mock_swift.stat.return_value = _make_stat_response()
        swift_p = SwiftPath('swift://tenant/container/path')
        result = swift_p.exists()
        self.assertTrue(result)
        self.assertFalse(self.mock_swift_conn.get_container.call_args_list)

    def test_true_directory_with_no_dir_object(self):
        swift_p = SwiftPath('swift://tenant/container/dirname')
        self.mock_swift.stat.side_effect = _service_404_exception()
        mock_list = self.mock_swift_conn.get_container
        mock_list.return_value = ({}, [
            {'name': 'container/dirname/file1.txt'},
        ])
        result = swift_p.exists()
        self.assertTrue(result)
        mock_list.assert_called_with('container', full_listing=False,
                                     limit=1, prefix='dirname/')

    def test_directory_does_not_exist(self):
        mock_list = self.mock_swift_conn.get_container
        self.mock_swift.stat.side_effect = _service_404_exception()
        mock_list.return_value = ({}, [])
        # key is we append trailing slash so only *could* match if there were a
        # file within the directory
        result = Path('swift://A/B/C/D')
        self.assertFalse(result.exists())
        mock_list.assert_called_with('B', full_listing=False,
                                     limit=1, prefix='C/D/')

    @mock.patch.object(SwiftPath, 'exists', autospec=True)
    def test_non_existent_container_or_tenant(self, mock_exists):
        mock_exists.return_value = True
        self.assertTrue(SwiftPath('swift://AUTH_final').isdir())
        self.assertTrue(SwiftPath('swift://AUTH_final/container').isdir())

    @mock.patch.object(SwiftPath, 'exists', autospec=True)
    def test_existing_container_or_tenant(self, mock_exists):
        mock_exists.return_value = False
        self.assertFalse(SwiftPath('swift://AUTH_final').isdir())
        self.assertFalse(SwiftPath('swift://AUTH_final/container').isdir())


class TestDownloadObject(SwiftTestCase):
    def test_container(self):
        swift_p = SwiftPath('swift://tenant/container')
        with self.assertRaisesRegexp(ValueError, 'path'):
            swift_p.download_object('file')

    def test_success(self):
        self.mock_swift.download.return_value = [{}]
        swift_p = SwiftPath('swift://tenant/container/d')
        swift_p.download_object('file.txt')

        download_kwargs = self.mock_swift.download.call_args_list[0][1]
        self.assertEquals(len(download_kwargs), 3)
        self.assertEquals(download_kwargs['container'], 'container')
        self.assertEquals(download_kwargs['objects'], ['d'])
        self.assertEquals(download_kwargs['options'], {'out_file': 'file.txt'})

    def test_raises_inconsistent_error(self):
        self.mock_swift.download.side_effect = SwiftError(
            'Error downloading /s_2_2110.bcl: '
            'md5sum != etag, b7a3d2ab6051e9390124f4df6040d9f5 != 15e3051e5558749f434acaeef2d5526d'
        )
        with settings.use({'swift': {'num_retries': 0}}):
            with self.assertRaises(swift.InconsistentDownloadError):
                swift_p = SwiftPath('swift://tenant/container/d')
                swift_p.download_object('file.txt')


class TestDownloadObjects(SwiftTestCase):
    def test_tenant(self):
        swift_p = SwiftPath('swift://tenant')
        with self.assertRaisesRegexp(ValueError, 'tenant'):
            swift_p.download_objects('output_dir', [])

    def test_local_paths(self):
        self.mock_swift.download.return_value = [{
            'object': 'd/e/f.txt',
            'path': 'output_dir/e/f.txt'
        }, {
            'object': 'd/e/f/g.txt',
            'path': 'output_dir/e/f/g.txt'
        }]
        swift_p = SwiftPath('swift://tenant/container/d')
        r = swift_p.download_objects('output_dir', ['e/f.txt', 'e/f/g.txt'])
        self.assertEquals(r, {
            'e/f.txt': 'output_dir/e/f.txt',
            'e/f/g.txt': 'output_dir/e/f/g.txt'
        })

        download_kwargs = self.mock_swift.download.call_args_list[0][1]
        self.assertEquals(len(download_kwargs), 3)
        self.assertEquals(download_kwargs['container'], 'container')
        self.assertEquals(sorted(download_kwargs['objects']),
                          sorted(['d/e/f.txt', 'd/e/f/g.txt']))
        self.assertEquals(download_kwargs['options'], {
            'prefix': 'd/',
            'out_directory': 'output_dir',
            'remove_prefix': True,
            'skip_identical': True,
            'shuffle': True
        })

    def test_absolute_paths(self):
        self.mock_swift.download.return_value = [{
            'object': 'd/e/f.txt',
            'path': 'output_dir/e/f.txt'
        }, {
            'object': 'd/e/f/g.txt',
            'path': 'output_dir/e/f/g.txt'
        }]
        download_settings = {
            'swift:download': {
                'skip_identical': True,
                'shuffle': False
            }
        }

        swift_p = SwiftPath('swift://tenant/container/d')
        with settings.use(download_settings):
            r = swift_p.download_objects(
                'output_dir',
                [
                    'swift://tenant/container/d/e/f.txt',
                    'swift://tenant/container/d/e/f/g.txt'
                ])
        self.assertEquals(r, {
            'swift://tenant/container/d/e/f.txt': 'output_dir/e/f.txt',
            'swift://tenant/container/d/e/f/g.txt': 'output_dir/e/f/g.txt'
        })

        download_kwargs = self.mock_swift.download.call_args_list[0][1]
        self.assertEquals(len(download_kwargs), 3)
        self.assertEquals(download_kwargs['container'], 'container')
        self.assertEquals(sorted(download_kwargs['objects']),
                          sorted(['d/e/f.txt', 'd/e/f/g.txt']))
        self.assertEquals(download_kwargs['options'], {
            'prefix': 'd/',
            'out_directory': 'output_dir',
            'remove_prefix': True,
            'skip_identical': True,
            'shuffle': False
        })

    def test_absolute_paths_not_child_of_download_path(self):
        swift_p = SwiftPath('swift://tenant/container/d')
        with self.assertRaisesRegexp(ValueError, 'child'):
            swift_p.download_objects('output_dir', [
                'swift://tenant/container/bad/e/f.txt',
                'swift://tenant/container/bad/e/f/g.txt'
            ])

    def test_raises_inconsistent_error(self):
        self.mock_swift.download.side_effect = SwiftError(
            'Error downloading /s_2_2110.bcl: '
            'read_length != content_length, 20 != 10'
        )
        with settings.use({'swift': {'num_retries': 0}}):
            with self.assertRaises(swift.InconsistentDownloadError):
                swift_p = SwiftPath('swift://tenant/container/bad')
                swift_p.download_objects('output_dir', [
                    'swift://tenant/container/bad/e/f.txt',
                    'swift://tenant/container/bad/e/f/g.txt'
                ])


class TestGetProgressLogger(unittest.TestCase):
    def test_success(self):
        l = swift.get_progress_logger()
        expected = logging.getLogger('stor.swift.progress')
        self.assertEquals(l, expected)


class TestDownload(SwiftTestCase):
    def setUp(self):
        super(TestDownload, self).setUp()
        settings.update({'swift': {'num_retries': 5}})

    def test_download_tenant(self):
        swift_p = SwiftPath('swift://tenant')
        with self.assertRaisesRegexp(ValueError, 'tenant'):
            swift_p.download('output_dir')

    def test_download_container(self):
        self.mock_swift.download.return_value = []
        download_settings = {
            'swift:download': {
                'skip_identical': True,
                'shuffle': False
            }
        }

        swift_p = SwiftPath('swift://tenant/container')
        with settings.use(download_settings):
            swift_p.download('output_dir')
        self.mock_swift.download.assert_called_once_with(
            'container',
            options={
                'prefix': None,
                'out_directory': 'output_dir',
                'remove_prefix': True,
                'skip_identical': True,
                'shuffle': False
            })

    @freezegun.freeze_time('2016-4-5')
    def test_progress_logging(self):
        self.mock_swift.download.return_value = [
            {
                'action': 'download_object',
                'read_length': 100
            }
            for i in range(20)
        ]
        self.mock_swift.download.return_value.append({'action': 'random_action'})

        swift_p = SwiftPath('swift://tenant/container')
        with LogCapture('stor.swift.progress') as progress_log:
            swift_p.download('output_dir')
            progress_log.check(
                ('stor.swift.progress', 'INFO', 'starting download'),
                ('stor.swift.progress', 'INFO', '10\t0:00:00\t0.00 MB\t0.00 MB/s'),  # nopep8
                ('stor.swift.progress', 'INFO', '20\t0:00:00\t0.00 MB\t0.00 MB/s'),  # nopep8
                ('stor.swift.progress', 'INFO', 'download complete - 20\t0:00:00\t0.00 MB\t0.00 MB/s'),  # nopep8
            )

    def test_download_resource(self):
        self.mock_swift.download.return_value = []

        swift_p = SwiftPath('swift://tenant/container/r/')
        swift_p.download('output_dir')
        self.mock_swift.download.assert_called_once_with(
            'container',
            options={
                'prefix': 'r/',
                'out_directory': 'output_dir',
                'remove_prefix': True,
                'skip_identical': True,
                'shuffle': True
            })

    def test_download_resource_wo_slash(self):
        self.mock_swift.download.return_value = []

        swift_p = SwiftPath('swift://tenant/container/r')
        swift_p.download('output_dir')
        self.mock_swift.download.assert_called_once_with(
            'container',
            options={
                'prefix': 'r/',
                'out_directory': 'output_dir',
                'remove_prefix': True,
                'skip_identical': True,
                'shuffle': True
            })

    def test_download_w_identical(self):
        # Raise a 304 to simulate the download being identical
        self.mock_swift.download.return_value = [{
            'error': ClientException('', http_status=304)
        }]

        swift_p = SwiftPath('swift://tenant/container')
        swift_p.download('output_dir')
        self.mock_swift.download.assert_called_once_with(
            'container',
            options={
                'prefix': None,
                'out_directory': 'output_dir',
                'remove_prefix': True,
                'skip_identical': True,
                'shuffle': True
            })

    @mock.patch('time.sleep', autospec=True)
    def test_download_w_condition(self, mock_sleep):
        # Simulate the condition not being met the first call
        self.mock_swift.download.side_effect = [
            [{}, {}],
            [{}, {}, {}]
        ]

        swift_p = SwiftPath('swift://tenant/container')
        swift_p.download('output_dir',
                         condition=lambda results: len(results) == 3)
        self.assertEquals(len(self.mock_swift.download.call_args_list), 2)

    def test_download_correct_thread_options(self):
        self.disable_get_swift_service_mock()
        download_settings = {
            'swift:download': {
                'object_threads': 20,
                'container_threads': 30
            }
        }

        swift_p = SwiftPath('swift://tenant/container/path')
        with settings.use(download_settings):
            swift_p.download('output_dir')

        options_passed = self.mock_swift_service.call_args[0][0]
        self.assertEquals(options_passed['object_dd_threads'], 20)
        self.assertEquals(options_passed['container_threads'], 30)

    @mock.patch('time.sleep', autospec=True)
    @mock.patch.object(SwiftPath, 'list', autospec=True)
    def test_download_w_condition_and_use_manifest(self, mock_list, mock_sleep):
        self.mock_swift_conn.get_object.return_value = ('header', 'my/obj1\nmy/obj2\nmy/obj3\n')
        self.mock_swift.download.return_value = [{
            'action': 'download_object',
            'object': 'my/obj1',
            'success': True
        }, {
            'action': 'download_object',
            'object': 'my/obj2',
            'success': True
        }, {
            'action': 'download_object',
            'object': 'my/obj3',
            'success': True
        }]

        swift_p = SwiftPath('swift://tenant/container')
        swift_p.download('output_dir',
                         use_manifest=True,
                         condition=lambda results: len(results) == 3)
        self.mock_swift.download.assert_called_once_with(
            'container',
            options={
                'prefix': None,
                'out_directory': 'output_dir',
                'remove_prefix': True,
                'skip_identical': True,
                'shuffle': True
            })

        # Verify that list was called with a data manifest as well
        mock_list.assert_called_once_with(mock.ANY, use_manifest=True)

    @mock.patch('time.sleep', autospec=True)
    @mock.patch.object(SwiftPath, 'list', autospec=True)
    def test_download_w_use_manifest(self, mock_list, mock_sleep):
        self.mock_swift_conn.get_object.return_value = ('header', 'my/obj1\nmy/obj2\nmy/obj3\n')
        self.mock_swift.download.return_value = [{
            'action': 'download_object',
            'object': 'my/obj1',
            'success': True
        }, {
            'action': 'download_object',
            'object': 'my/obj2',
            'success': True
        }, {
            'action': 'download_object',
            'object': 'my/obj3',
            'success': True
        }]

        swift_p = SwiftPath('swift://tenant/container')
        swift_p.download('output_dir', use_manifest=True)
        self.mock_swift.download.assert_called_once_with(
            'container',
            options={
                'prefix': None,
                'out_directory': 'output_dir',
                'remove_prefix': True,
                'skip_identical': True,
                'shuffle': True
            })

        # Verify that list was called with a data manifest as well
        mock_list.assert_called_once_with(mock.ANY, use_manifest=True)

    @mock.patch('time.sleep', autospec=True)
    @mock.patch.object(SwiftPath, 'list', autospec=True)
    def test_download_w_use_manifest_validation_err(self, mock_list, mock_sleep):
        self.mock_swift_conn.get_object.return_value = ('header', 'my/obj1\nmy/obj2\nmy/obj3\n')
        self.mock_swift.download.return_value = [{
            'action': 'download_object',
            'object': 'my/obj1',
            'success': True
        }, {
            'action': 'download_object',
            'object': 'my/obj2',
            'success': True
        }]

        swift_p = SwiftPath('swift://tenant/container')
        with self.assertRaises(exceptions.ConditionNotMetError):
            swift_p.download('output_dir', use_manifest=True, num_retries=2)

        self.assertEquals(len(mock_sleep.call_args_list), 2)
        # Verify that list was called with a data manifest as well
        self.assertEquals(mock_list.call_args_list,
                          [mock.call(mock.ANY, use_manifest=True),
                           mock.call(mock.ANY, use_manifest=True),
                           mock.call(mock.ANY, use_manifest=True)])


@mock.patch('stor.utils.walk_files_and_dirs', autospec=True)
class TestUpload(SwiftTestCase):
    def test_abs_path(self, mock_walk_files_and_dirs):
        mock_walk_files_and_dirs.return_value = {
            '/abs_path/file1': 10
        }
        self.mock_swift.upload.return_value = []

        swift_p = SwiftPath('swift://tenant/container/path')
        swift_p.upload(['/abs_path/file1'])

        upload_args = self.mock_swift.upload.call_args_list[0][0]
        self.assertEquals(len(upload_args), 2)
        self.assertEquals(upload_args[0], 'container')
        self.assertEquals([o.source for o in upload_args[1]],
                          ['/abs_path/file1'])
        self.assertEquals([o.object_name for o in upload_args[1]],
                          ['path/abs_path/file1'])

    def test_relative_path(self, mock_walk_files_and_dirs):
        segment_size = settings.get()['swift:upload']['segment_size']

        mock_walk_files_and_dirs.return_value = {
            './relative_path/file1': 10
        }
        self.mock_swift.upload.return_value = []

        swift_p = SwiftPath('swift://tenant/container/path')
        swift_p.upload(['./relative_path/file1'])

        upload_args = self.mock_swift.upload.call_args_list[0][0]
        self.assertEquals(len(upload_args), 2)
        self.assertEquals(upload_args[0], 'container')
        self.assertEquals([o.source for o in upload_args[1]],
                          ['./relative_path/file1'])
        self.assertEquals([o.object_name for o in upload_args[1]],
                          ['path/relative_path/file1'])
        options = self.mock_swift.upload.call_args_list[0][1]['options']
        self.assertEquals(options, {
            'segment_size': segment_size,
            'leave_segments': True,
            'skip_identical': False,
            'changed': False,
            'segment_container': '.segments_container',
            'checksum': True,
            'use_slo': True,
        })

    @mock.patch('os.path', ntpath)
    def test_relative_windows_path(self, mock_walk_files_and_dirs):
        mock_walk_files_and_dirs.return_value = {
            r'.\relative_path\file1': 20
        }
        self.mock_swift.upload.return_value = []

        swift_p = SwiftPath('swift://tenant/container/path')
        swift_p.upload([r'.\relative_path\file1'])

        upload_args = self.mock_swift.upload.call_args_list[0][0]
        self.assertEquals(len(upload_args), 2)
        self.assertEquals(upload_args[0], 'container')
        self.assertEquals([o.source for o in upload_args[1]],
                          [r'.\relative_path\file1'])
        self.assertEquals([o.object_name for o in upload_args[1]],
                          ['path/relative_path/file1'])

    @mock.patch('time.sleep', autospec=True)
    def test_upload_put_object_error(self, mock_sleep, mock_walk_files_and_dirs):
        mock_walk_files_and_dirs.return_value = {
            'file1': 20,
            'file2': 30
        }
        self.mock_swift.upload.side_effect = ClientException(
            "put_object('HHGF5BCXX_160209_SN357_0342_A', "
            "'pileups/s_1_TAAAGGC/s_1_TAAAGGC.chr15-80450512.txt', ...) "
            "failure and no ability to reset contents for reupload.")

        swift_p = SwiftPath('swift://tenant/container/path')
        with settings.use({'swift': {'num_retries': 5}}):
            with self.assertRaisesRegexp(swift.FailedUploadError, 'put_object'):
                swift_p.upload(['upload'])

        self.assertEquals(len(self.mock_swift.upload.call_args_list), 6)

    def test_upload_to_dir(self, mock_walk_files_and_dirs):
        mock_walk_files_and_dirs.return_value = {
            'file1': 20,
            'file2': 30
        }
        self.mock_swift.upload.return_value = []

        upload_settings = {
            'swift:upload': {
                'segment_size': 1000,
                'use_slo': True,
                'leave_segments': True,
                'changed': True,
                'checksum': False,
                'skip_identical': True
            }
        }

        swift_p = SwiftPath('swift://tenant/container/path')
        with settings.use(upload_settings):
            swift_p.upload(['upload'])
        upload_args = self.mock_swift.upload.call_args_list[0][0]
        upload_kwargs = self.mock_swift.upload.call_args_list[0][1]

        self.assertEquals(len(upload_args), 2)
        self.assertEquals(upload_args[0], 'container')
        self.assertEquals(set([o.source for o in upload_args[1]]),
                          set(['file1', 'file2']))
        self.assertEquals(set([o.object_name for o in upload_args[1]]),
                          set(['path/file1', 'path/file2']))

        self.assertEquals(len(upload_kwargs), 1)
        self.assertEquals(upload_kwargs['options'], {
            'use_slo': True,
            'segment_container': '.segments_container',
            'leave_segments': True,
            'segment_size': 1000,
            'changed': True,
            'skip_identical': True,
            'checksum': False
        })

    @mock.patch('time.sleep', autospec=True)
    def test_upload_w_manifest(self, mock_sleep, mock_walk_files_and_dirs):
        mock_walk_files_and_dirs.side_effect = [
            {
                './file1': 20,
                './file2': 30
            },
            {
                './%s' % utils.DATA_MANIFEST_FILE_NAME: 10
            }
        ]
        self.mock_swift.upload.side_effect = [
            [{
                'success': True,
                'action': 'upload_object',
                'object': 'path/%s' % utils.DATA_MANIFEST_FILE_NAME,
                'path': './%s' % utils.DATA_MANIFEST_FILE_NAME
            }],
            [{
                'success': True,
                'action': 'upload_object',
                'object': 'path/file1',
                'path': './file1'
            }, {
                'success': True,
                'action': 'upload_object',
                'object': 'path/file2',
                'path': './file2'
            }],
        ]
        upload_settings = {
            'swift:upload': {
                'segment_size': 1000,
                'use_slo': True,
                'leave_segments': True,
                'changed': True,
                'checksum': False,
                'skip_identical': True
            }
        }

        with NamedTemporaryDirectory(change_dir=True), settings.use(upload_settings):
            swift_p = SwiftPath('swift://tenant/container/path')
            swift_p.upload(['.'], use_manifest=True)

        manifest_upload_args = self.mock_swift.upload.call_args_list[0][0]
        self.assertEquals(len(manifest_upload_args), 2)
        self.assertEquals(manifest_upload_args[0], 'container')
        self.assertEquals([o.source for o in manifest_upload_args[1]],
                          ['./%s' % utils.DATA_MANIFEST_FILE_NAME])
        self.assertEquals([o.object_name for o in manifest_upload_args[1]],
                          ['path/%s' % utils.DATA_MANIFEST_FILE_NAME])

        upload_args = self.mock_swift.upload.call_args_list[1][0]
        upload_kwargs = self.mock_swift.upload.call_args_list[1][1]
        self.assertEquals(len(upload_args), 2)
        self.assertEquals(upload_args[0], 'container')
        self.assertEquals(set([o.source for o in upload_args[1]]),
                          set(['./file1', './file2']))
        self.assertEquals(set([o.object_name for o in upload_args[1]]),
                          set(['path/file1', 'path/file2']))

        self.assertEquals(len(upload_kwargs), 1)
        self.assertEquals(upload_kwargs['options'], {
            'use_slo': True,
            'segment_container': '.segments_container',
            'leave_segments': True,
            'segment_size': 1000,
            'changed': True,
            'skip_identical': True,
            'checksum': False
        })

    @mock.patch('time.sleep', autospec=True)
    def test_upload_w_manifest_validation_err(self, mock_sleep, mock_walk_files_and_dirs):
        mock_walk_files_and_dirs.return_value = {
            'file1': 20,
            'file2': 10
        }
        self.mock_swift.upload.return_value = [{
            'success': True,
            'action': 'upload_object',
            'object': 'path/file1',
            'path': 'file1'
        }, {
            'success': False,  # Create an error in the upload results
            'action': 'upload_object',
            'object': 'path/file2',
            'path': 'file2'
        }]
        upload_settings = {
            'swift:upload': {
                'segment_size': 1000,
                'use_slo': True,
                'leave_segments': True,
                'changed': True,
                'checksum': False,
                'skip_identical': True
            }
        }

        with NamedTemporaryDirectory(change_dir=True):
            swift_p = SwiftPath('swift://tenant/container/path')
            with self.assertRaises(exceptions.ConditionNotMetError), settings.use(upload_settings):
                swift_p.upload(['.'], use_manifest=True, num_retries=2)
        self.assertEquals(len(mock_sleep.call_args_list), 2)

    def test_upload_to_container(self, mock_walk_files_and_dirs):
        mock_walk_files_and_dirs.return_value = {
            'file1': 20,
            'file2': 20
        }
        self.mock_swift.upload.return_value = []
        upload_settings = {
            'swift:upload': {
                'segment_size': 1000,
                'use_slo': True,
                'leave_segments': True,
                'changed': True
            }
        }

        swift_p = SwiftPath('swift://tenant/container')
        with settings.use(upload_settings):
            swift_p.upload(['upload'])
        upload_args = self.mock_swift.upload.call_args_list[0][0]
        upload_kwargs = self.mock_swift.upload.call_args_list[0][1]

        self.assertEquals(len(upload_args), 2)
        self.assertEquals(upload_args[0], 'container')
        self.assertEquals(set([o.source for o in upload_args[1]]),
                          set(['file1', 'file2']))
        self.assertEquals(set([o.object_name for o in upload_args[1]]),
                          set(['file1', 'file2']))

        self.assertEquals(len(upload_kwargs), 1)
        self.assertEquals(upload_kwargs['options'], {
            'segment_container': '.segments_container',
            'use_slo': True,
            'leave_segments': True,
            'segment_size': 1000,
            'changed': True,
            'skip_identical': False,
            'checksum': True
        })

    @freezegun.freeze_time('2016-4-5')
    def test_progress_logging(self, mock_walk_files_and_dirs):
        mock_walk_files_and_dirs.return_value = {
            'file%s' % i: 20
            for i in range(20)
        }
        self.mock_swift.upload.return_value = [
            {
                'action': 'upload_object',
                'path': 'file%s' % i
            }
            for i in range(20)
        ]
        self.mock_swift.upload.return_value.append({'action': 'random_action'})
        upload_settings = {
            'swift:upload': {
                'segment_size': 1000,
                'use_slo': True,
                'leave_segments': True,
                'changed': True
            }
        }

        swift_p = SwiftPath('swift://tenant/container')
        with LogCapture('stor.swift.progress') as progress_log:
            with settings.use(upload_settings):
                swift_p.upload(['upload'])
            progress_log.check(
                ('stor.swift.progress', 'INFO', 'starting upload of 20 objects'),
                ('stor.swift.progress', 'INFO', '10/20\t0:00:00\t0.00 MB\t0.00 MB/s'),  # nopep8
                ('stor.swift.progress', 'INFO', '20/20\t0:00:00\t0.00 MB\t0.00 MB/s'),  # nopep8
                ('stor.swift.progress', 'INFO', 'upload complete - 20/20\t0:00:00\t0.00 MB\t0.00 MB/s'),  # nopep8
            )

    def test_upload_to_tenant(self, mock_walk_files_and_dirs):
        mock_walk_files_and_dirs.return_value = {
            'file1': 10,
            'file2': 20
        }
        self.mock_swift.upload.return_value = []
        upload_settings = {
            'swift:upload': {
                'segment_size': 1000,
                'use_slo': True,
                'leave_segments': True,
                'changed': True
            }
        }

        swift_p = SwiftPath('swift://tenant')
        with self.assertRaisesRegexp(ValueError, 'must specify container'):
            with settings.use(upload_settings):
                swift_p.upload(['upload'])

    def test_upload_thread_options_correct(self, mock_walk_files_and_dirs):
        self.disable_get_swift_service_mock()
        upload_settings = {
            'swift:upload': {
                'segment_size': 1000,
                'use_slo': True,
                'leave_segments': True,
                'changed': True,
                'object_threads': 20,
                'segment_threads': 30
            }
        }

        swift_p = SwiftPath('swift://tenant/container/path')
        with settings.use(upload_settings):
            swift_p.upload([])

        options_passed = self.mock_swift_service.call_args[0][0]
        self.assertEquals(options_passed['object_uu_threads'], 20)
        self.assertEquals(options_passed['segment_threads'], 30)

    @mock.patch('time.sleep', autospec=True)
    def test_upload_w_condition_and_use_manifest(self, mock_sleep, mock_walk_files_and_dirs):
        mock_walk_files_and_dirs.side_effect = [
            {
                './%s' % utils.DATA_MANIFEST_FILE_NAME: 20,
                './file1': 30,
                './file2': 40
            },
            {
                './%s' % utils.DATA_MANIFEST_FILE_NAME: 20
            }
        ]
        self.mock_swift.upload.side_effect = [
            [{
                'success': True,
                'action': 'upload_object',
                'object': 'path/%s' % utils.DATA_MANIFEST_FILE_NAME,
                'path': './%s' % utils.DATA_MANIFEST_FILE_NAME
            }],
            [{
                'success': True,
                'action': 'upload_object',
                'object': 'path/file1',
                'path': './file1'
            }, {
                'success': True,
                'action': 'upload_object',
                'object': 'path/file2',
                'path': './file2'
            }],
        ]
        upload_settings = {
            'swift:upload': {
                'segment_size': 1000,
                'use_slo': True,
                'leave_segments': True,
                'changed': True,
                'checksum': False,
                'skip_identical': True
            }
        }

        with NamedTemporaryDirectory(change_dir=True), settings.use(upload_settings):
            swift_p = SwiftPath('swift://tenant/container/path')
            swift_p.upload(['.'],
                           use_manifest=True,
                           condition=lambda results: len(results) == 2)

        # Upload is called twice, the first time for the manifest and the second
        # time for the objects
        self.assertEquals(len(self.mock_swift.upload.call_args_list), 2)

        manifest_upload_args = self.mock_swift.upload.call_args_list[0][0]
        self.assertEquals(len(manifest_upload_args), 2)
        self.assertEquals(manifest_upload_args[0], 'container')
        self.assertEquals(set([o.source for o in manifest_upload_args[1]]),
                          set(['./%s' % utils.DATA_MANIFEST_FILE_NAME]))
        self.assertEquals(set([o.object_name for o in manifest_upload_args[1]]),
                          set(['path/%s' % utils.DATA_MANIFEST_FILE_NAME]))

        upload_args = self.mock_swift.upload.call_args_list[1][0]
        upload_kwargs = self.mock_swift.upload.call_args_list[1][1]
        self.assertEquals(len(upload_args), 2)
        self.assertEquals(upload_args[0], 'container')
        self.assertEquals(set([o.source for o in upload_args[1]]),
                          set(['./file1', './file2']))
        self.assertEquals(set([o.object_name for o in upload_args[1]]),
                          set(['path/file1', 'path/file2']))

        self.assertEquals(len(upload_kwargs), 1)
        self.assertEquals(upload_kwargs['options'], {
            'use_slo': True,
            'segment_container': '.segments_container',
            'leave_segments': True,
            'segment_size': 1000,
            'changed': True,
            'skip_identical': True,
            'checksum': False
        })

    def test_upload_w_use_manifest_multiple_uploads(self, mock_walk_files_and_dirs):
        with self.assertRaisesRegexp(ValueError, 'can only upload one directory'):
            SwiftPath('swift://tenant/container/path').upload(['.', '.'],
                                                              use_manifest=True)

    def test_upload_w_use_manifest_single_file(self, mock_walk_files_and_dirs):
        with self.assertRaisesRegexp(ValueError, 'can only upload one directory'):
            SwiftPath('swift://tenant/container/path').upload(['file'],
                                                              use_manifest=True)


class TestCopy(SwiftTestCase):
    @mock.patch.object(swift.SwiftPath, 'download_object', autospec=True)
    def test_copy_posix_file_destination(self, mockdownload_object):
        p = SwiftPath('swift://tenant/container/file_source.txt')
        p.copy('file_dest.txt')
        mockdownload_object.assert_called_once_with(p, Path(u'file_dest.txt'))

    @mock.patch.object(swift.SwiftPath, 'download_object', autospec=True)
    def test_copy_posix_dir_destination(self, mockdownload_object):
        p = SwiftPath('swift://tenant/container/file_source.txt')
        with NamedTemporaryDirectory() as tmp_d:
            p.copy(tmp_d)
            mockdownload_object.assert_called_once_with(p, Path(tmp_d) / 'file_source.txt')

    def test_copy_swift_destination(self):
        p = SwiftPath('swift://tenant/container/file_source')
        with self.assertRaisesRegexp(ValueError, 'OBS path'):
            p.copy('swift://tenant/container/file_dest')


class TestCopytree(SwiftTestCase):
    @mock.patch.object(swift.SwiftPath, 'download', autospec=True)
    def test_copytree_posix_destination(self, mock_download):
        p = SwiftPath('swift://tenant/container')
        with settings.use({'swift:download': {'object_threads': 100}}):
            p.copytree('path', num_retries=1)
        mock_download.assert_called_once_with(
            p,
            Path(u'path'),
            condition=None,
            use_manifest=False,
            num_retries=1)

    def test_copytree_swift_destination(self):
        p = SwiftPath('swift://tenant/container')
        with self.assertRaises(ValueError):
            p.copytree('swift://swift/path')

    @mock.patch('os.path', ntpath)
    def test_copytree_windows_destination(self):
        p = SwiftPath('swift://tenant/container')
        with self.assertRaisesRegexp(ValueError, 'not supported'):
            p.copytree(r'windows\path')


class TestStat(SwiftTestCase):
    def test_tenant(self):
        self.mock_swift.stat.return_value = _make_stat_response({
            'headers': {
                'content-length': '0',
                'x-account-storage-policy-3xreplica-container-count': '31',
                'x-account-object-count': '20081986',
                'connection': 'close',
                'x-timestamp': '1445629170.46005',
                'x-account-access-control': '{"read-only":["seq_upload_rnd","swft_labprod"],"read-write":["svc_svc_seq"]}',  # nopep8
                'x-account-storage-policy-3xreplica-bytes-used': '24993077101523',  # nopep8
                'x-trans-id': 'tx2acc1bc870884a0487dd0-0056a6a993',
                'date': 'Mon, 25 Jan 2016 23:02:43 GMT',
                'x-account-bytes-used': '24993077101523',
                'x-account-container-count': '31',
                'content-type': 'text/plain; charset=utf-8',
                'accept-ranges': 'bytes',
                'x-account-storage-policy-3xreplica-object-count': '20081986'
            },
            'container': None,
            'success': True,
            'action': 'stat_account',
            'items': [
                ('Account', 'AUTH_seq_upload_prod'),
                ('Containers', 31),
                ('Objects', '20081986'),
                ('Bytes', '24993077101523'),
                ('Containers in policy "3xreplica"', '31'),
                ('Objects in policy "3xreplica"', '20081986'),
                ('Bytes in policy "3xreplica"', '24993077101523')
            ],
            'object': None
        })
        swift_p = SwiftPath('swift://tenant/')
        res = swift_p.stat()
        self.assertEquals(res, {
            'Account': 'AUTH_seq_upload_prod',
            'Containers': 31,
            'Objects': '20081986',
            'Bytes': '24993077101523',
            'Containers-in-policy-"3xreplica"': '31',
            'Objects-in-policy-"3xreplica"': '20081986',
            'Bytes-in-policy-"3xreplica"': '24993077101523',
            'Access-Control': {
                'read-only': ['seq_upload_rnd', 'swft_labprod'],
                'read-write': ['svc_svc_seq']
            },
            'headers': {
                'content-length': '0',
                'x-account-storage-policy-3xreplica-container-count': '31',
                'x-account-object-count': '20081986',
                'connection': 'close',
                'x-timestamp': '1445629170.46005',
                'x-account-access-control': '{"read-only":["seq_upload_rnd","swft_labprod"],"read-write":["svc_svc_seq"]}',  # nopep8
                'x-account-storage-policy-3xreplica-bytes-used': '24993077101523',  # nopep8
                'x-trans-id': 'tx2acc1bc870884a0487dd0-0056a6a993',
                'date': 'Mon, 25 Jan 2016 23:02:43 GMT',
                'x-account-bytes-used': '24993077101523',
                'x-account-container-count': '31',
                'content-type': 'text/plain; charset=utf-8',
                'accept-ranges': 'bytes',
                'x-account-storage-policy-3xreplica-object-count': '20081986'
            }
        })

    def test_tenant_no_access_control(self):
        self.mock_swift.stat.return_value = _make_stat_response({
            'headers': {
                'content-length': '0',
                'x-account-storage-policy-3xreplica-container-count': '31',
                'x-account-object-count': '20081986',
                'connection': 'close',
                'x-timestamp': '1445629170.46005',
                'x-account-storage-policy-3xreplica-bytes-used': '24993077101523',  # nopep8
                'x-trans-id': 'tx2acc1bc870884a0487dd0-0056a6a993',
                'date': 'Mon, 25 Jan 2016 23:02:43 GMT',
                'x-account-bytes-used': '24993077101523',
                'x-account-container-count': '31',
                'content-type': 'text/plain; charset=utf-8',
                'accept-ranges': 'bytes',
                'x-account-storage-policy-3xreplica-object-count': '20081986'
            },
            'container': None,
            'success': True,
            'action': 'stat_account',
            'items': [
                ('Account', 'AUTH_seq_upload_prod'),
                ('Containers', 31),
                ('Objects', '20081986'),
                ('Bytes', '24993077101523'),
                ('Containers in policy "3xreplica"', '31'),
                ('Objects in policy "3xreplica"', '20081986'),
                ('Bytes in policy "3xreplica"', '24993077101523')
            ],
            'object': None
        })
        swift_p = SwiftPath('swift://tenant/')
        res = swift_p.stat()
        self.assertEquals(res, {
            'Account': 'AUTH_seq_upload_prod',
            'Containers': 31,
            'Objects': '20081986',
            'Bytes': '24993077101523',
            'Containers-in-policy-"3xreplica"': '31',
            'Objects-in-policy-"3xreplica"': '20081986',
            'Bytes-in-policy-"3xreplica"': '24993077101523',
            'Access-Control': {},
            'headers': {
                'content-length': '0',
                'x-account-storage-policy-3xreplica-container-count': '31',
                'x-account-object-count': '20081986',
                'connection': 'close',
                'x-timestamp': '1445629170.46005',
                'x-account-storage-policy-3xreplica-bytes-used': '24993077101523',  # nopep8
                'x-trans-id': 'tx2acc1bc870884a0487dd0-0056a6a993',
                'date': 'Mon, 25 Jan 2016 23:02:43 GMT',
                'x-account-bytes-used': '24993077101523',
                'x-account-container-count': '31',
                'content-type': 'text/plain; charset=utf-8',
                'accept-ranges': 'bytes',
                'x-account-storage-policy-3xreplica-object-count': '20081986'
            }
        })

    def test_container(self):
        self.mock_swift.stat.return_value = _make_stat_response({
            'headers': {
                'content-length': '0',
                'x-container-object-count': '43868',
                'accept-ranges': 'bytes',
                'x-storage-policy': '3xReplica',
                'date': 'Mon, 25 Jan 2016 23:10:45 GMT',
                'connection': 'close',
                'x-timestamp': '1452627422.60776',
                'x-trans-id': 'tx441a691b0e514782b51be-0056a6ab75',
                'x-container-bytes-used': '55841489571',
                'content-type': 'text/plain; charset=utf-8'
            },
            'container': '2016-01',
            'success': True,
            'action': 'stat_container',
            'items': [
                ('Account', 'AUTH_seq_upload_prod'),
                ('Container', '2016-01'),
                ('Objects', '43868'),
                ('Bytes', '55841489571'),
                ('Read ACL', ''),
                ('Write ACL', ''),
                ('Sync To', ''),
                ('Sync Key', '')
            ]
        })
        swift_p = SwiftPath('swift://tenant/container')
        res = swift_p.stat()
        self.assertEquals(res, {
            'Account': 'AUTH_seq_upload_prod',
            'Container': '2016-01',
            'Objects': '43868',
            'Bytes': '55841489571',
            'Read-ACL': '',
            'Write-ACL': '',
            'Sync-To': '',
            'Sync-Key': '',
            'headers': {
                'content-length': '0',
                'x-container-object-count': '43868',
                'accept-ranges': 'bytes',
                'x-storage-policy': '3xReplica',
                'date': 'Mon, 25 Jan 2016 23:10:45 GMT',
                'connection': 'close',
                'x-timestamp': '1452627422.60776',
                'x-trans-id': 'tx441a691b0e514782b51be-0056a6ab75',
                'x-container-bytes-used': '55841489571',
                'content-type': 'text/plain; charset=utf-8'
            }
        })

    def test_object(self):
        self.mock_swift.stat.return_value = _make_stat_response({
            'headers': {
                'content-length': '112',
                'x-object-meta-x-agi-ctime': '2016-01-15T05:22:00.0Z',
                'x-object-meta-x-agi-mode': '436',
                'accept-ranges': 'bytes',
                'last-modified': 'Fri, 15 Jan 2016 05:22:46 GMT',
                'connection': 'close',
                'x-object-meta-x-agi-gid': '0',
                'x-timestamp': '1452835365.34322',
                'etag': '87f0b7f04557315e6d1e6db21742d31c',
                'x-trans-id': 'tx805b2e7ce56343a6b2ea3-0056a6ac39',
                'date': 'Mon, 25 Jan 2016 23:14:01 GMT',
                'content-type': 'application/octet-stream',
                'x-object-meta-x-agi-uid': '0',
                'x-object-meta-x-agi-mtime': 'Fri, 15 Jan 2016 05:22:01 PST'
            },
            'container': '2016-01',
            'success': True, 'action':
            'stat_object',
            'items': [
                ('Account', u'AUTH_seq_upload_prod'),
                ('Container', '2016-01'),
                ('Object', 'object.txt'),
                ('Content Type', u'application/octet-stream'),
                ('Content Length', u'112'),
                ('Last Modified', u'Fri, 15 Jan 2016 05:22:46 GMT'),
                ('ETag', u'87f0b7f04557315e6d1e6db21742d31c'),
                ('Manifest', None)
            ],
            'object': 'object.txt'
        })
        swift_p = SwiftPath('swift://tenant/container')
        res = swift_p.stat()
        self.assertEquals(res, {
            'Account': 'AUTH_seq_upload_prod',
            'Container': '2016-01',
            'Object': 'object.txt',
            'Content-Type': 'application/octet-stream',
            'Content-Length': '112',
            'Last-Modified': 'Fri, 15 Jan 2016 05:22:46 GMT',
            'ETag': '87f0b7f04557315e6d1e6db21742d31c',
            'Manifest': None,
            'headers': {
                'content-length': '112',
                'x-object-meta-x-agi-ctime': '2016-01-15T05:22:00.0Z',
                'x-object-meta-x-agi-mode': '436',
                'accept-ranges': 'bytes',
                'last-modified': 'Fri, 15 Jan 2016 05:22:46 GMT',
                'connection': 'close',
                'x-object-meta-x-agi-gid': '0',
                'x-timestamp': '1452835365.34322',
                'etag': '87f0b7f04557315e6d1e6db21742d31c',
                'x-trans-id': 'tx805b2e7ce56343a6b2ea3-0056a6ac39',
                'date': 'Mon, 25 Jan 2016 23:14:01 GMT',
                'content-type': 'application/octet-stream',
                'x-object-meta-x-agi-uid': '0',
                'x-object-meta-x-agi-mtime': 'Fri, 15 Jan 2016 05:22:01 PST'
            }
        })

    def test_getsize_content_length(self):
        self.mock_swift.stat.return_value = _make_stat_response(
            {'items': [('Content-Length', '21')]})
        self.assertEqual(SwiftPath('swift://A/C/d.txt').getsize(), 21)

    def test_getsize_no_content_length(self):
        self.mock_swift.stat.return_value = _make_stat_response({'items': []})
        self.assertEqual(SwiftPath('swift://A/C/d.txt').getsize(), 0)


class TestRemove(SwiftTestCase):
    def test_invalid_remove(self):
        # Remove()s must happen on a resource of a container
        swift_p = SwiftPath('swift://tenant/container')
        with self.assertRaises(ValueError):
            swift_p.remove()

    def test_w_swift_error(self):
        self.mock_swift.delete.return_value = {
            'error': SwiftError('error')
        }
        swift_p = SwiftPath('swift://tenant/container/r')
        with self.assertRaises(swift.SwiftError):
            swift_p.remove()

        self.mock_swift.delete.assert_called_once_with('container', ['r'])

    def test_success(self):
        self.mock_swift.delete.return_value = {}
        swift_p = SwiftPath('swift://tenant/container/r')
        swift_p.remove()

        self.mock_swift.delete.assert_called_once_with('container', ['r'])


class TestRmtree(SwiftTestCase):
    def setUp(self):
        super(TestRmtree, self).setUp()

        # set up list to return no results by default. This ensures that the
        # check at the end of rmtree for no results passes
        mock_list = self.mock_swift_conn.get_container
        mock_list.return_value = ({}, [])

    def test_w_only_tenant(self):
        self.mock_swift.delete.return_value = {}
        swift_p = SwiftPath('swift://tenant')
        with self.assertRaisesRegexp(ValueError, 'include container'):
            swift_p.rmtree()

    @mock.patch('time.sleep', autospec=True)
    def test_rmtree_confict(self, mock_sleep):
        self.mock_swift.delete.side_effect = ClientException('conflict',
                                                             http_status=409)

        swift_p = SwiftPath('swift://tenant/container/path')
        with self.assertRaises(swift.ConflictError):
            swift_p.rmtree(num_retries=5)

        self.assertEquals(len(mock_sleep.call_args_list), 5)

    def test_not_found(self):
        self.mock_swift.delete.side_effect = ClientException('not_found',
                                                             http_status=404)

        swift_p = SwiftPath('swift://tenant/container/path')
        results = swift_p.rmtree()
        self.assertEquals([], results)

    def test_w_only_container_and_threads(self):
        self.mock_swift.delete.return_value = {}
        swift_p = SwiftPath('swift://tenant/container')
        with settings.use({'swift:delete': {'object_threads': 20}}):
            swift_p.rmtree()

        self.assertEquals(self.mock_swift.delete.call_args_list,
                          [mock.call('container'),
                           mock.call('container_segments'),
                           mock.call('.segments_container')])

    @mock.patch('time.sleep', autospec=True)
    def test_w_list_validation_failing_first_time(self, mock_sleep):
        self.mock_swift.delete.return_value = {}
        mock_list = self.mock_swift_conn.get_container
        mock_list.side_effect = [
            ({}, [{
                'name': 'path/to/resource1'
            }, {
                'name': 'path/to/resource2'
            }]),
            ({}, [])
        ]
        swift_p = SwiftPath('swift://tenant/container')
        swift_p.rmtree(num_retries=1)

        self.assertEquals(self.mock_swift.delete.call_args_list,
                          [mock.call('container'),
                           mock.call('container_segments'),
                           mock.call('.segments_container'),
                           mock.call('container'),
                           mock.call('container_segments'),
                           mock.call('.segments_container')])
        self.assertTrue(len(mock_list.call_args_list), 2)
        self.assertTrue(len(mock_sleep.call_args_list), 1)

    def test_thread_options_passed_through(self):
        self.disable_get_swift_service_mock()
        swift_p = SwiftPath('swift://tenant/container')
        with settings.use({'swift:delete': {'object_threads': 20}}):
            swift_p.rmtree()

        options_passed = self.mock_swift_service.call_args[0][0]
        self.assertEquals(options_passed['object_dd_threads'], 20)

    def test_w_only_segment_container(self):
        self.mock_swift.delete.return_value = {}
        swift_p = SwiftPath('swift://tenant/container_segments')
        swift_p.rmtree()

        self.assertEquals(self.mock_swift.delete.call_args_list,
                          [mock.call('container_segments')])

    def test_w_only_container_no_segment_container(self):
        self.mock_swift.delete.side_effect = [{},
                                              swift.NotFoundError('not found'),
                                              swift.NotFoundError('not found')]
        swift_p = SwiftPath('swift://tenant/container')
        swift_p.rmtree()

        self.assertEquals(self.mock_swift.delete.call_args_list,
                          [mock.call('container'),
                           mock.call('container_segments'),
                           mock.call('.segments_container')])

    def test_w_container_and_resource(self):
        self.mock_swift.delete.return_value = {}
        mock_list = self.mock_swift_conn.get_container
        mock_list.side_effect = [
            ({}, [{
                'name': 'dir/r1'
            }, {
                'name': 'dir/r2'
            }]),
            ({}, [])
        ]

        swift_p = SwiftPath('swift://tenant/container/dir')
        swift_p.rmtree()

        self.assertTrue(len(self.mock_swift.delete.call_args_list), 1)
        self.assertEquals(self.mock_swift.delete.call_args_list[0][0][0], 'container')
        self.assertEquals(set(self.mock_swift.delete.call_args_list[0][0][1]),
                          set(['dir/r1', 'dir/r2']))

        self.assertEquals(mock_list.call_args_list, [
            mock.call(u'container', full_listing=True, limit=None, prefix='dir/'),
            mock.call(u'container', full_listing=True, limit=None, prefix='dir/')
        ])


class TestPost(SwiftTestCase):
    def test_only_tenant(self):
        self.mock_swift.post.return_value = {}
        swift_p = SwiftPath('swift://tenant')
        swift_p.post()
        self.mock_swift.post.assert_called_once_with(container=None,
                                                     objects=None,
                                                     options=None)

    def test_w_resource(self):
        self.mock_swift.post.return_value = {}
        swift_p = SwiftPath('swift://tenant/container/r1')
        swift_p.post(options={'header': ['X-Delete-After:30']})
        self.mock_swift.post.assert_called_once_with(container='container',
                                                     objects=['r1'],
                                                     options={
                                                         'header': ['X-Delete-After:30']
                                                     })

    def test_only_container(self):
        self.mock_swift.post.return_value = {}

        swift_p = SwiftPath('swift://tenant/container')
        swift_p.post()

        self.mock_swift.post.assert_called_once_with(container='container',
                                                     objects=None,
                                                     options=None)


class TestCompatHelpers(SwiftTestCase):
    def test_noops(self):
        self.assertEqual(SwiftPath('swift://tenant').expanduser(),
                         SwiftPath('swift://tenant'))
        self.assertEqual(SwiftPath('swift://tenant').abspath(),
                         SwiftPath('swift://tenant'))

    @mock.patch.dict(os.environ, {'somevar': 'blah'}, clear=True)
    def test_expand(self):
        original = SwiftPath('swift://tenant/container/$somevar//another/../a/')
        self.assertEqual(original.expand(),
                         SwiftPath('swift://tenant/container/blah/a'))
        self.assertEqual(SwiftPath('swift://tenant/container//a/b').expand(),
                         SwiftPath('swift://tenant/container/a/b'))

    def test_expandvars(self):
        original = SwiftPath('swift://tenant/container/$somevar/another')
        other = SwiftPath('swift://tenant/container/somevar/another')
        with mock.patch.dict(os.environ, {'somevar': 'blah'}, clear=True):
            expanded = original.expandvars()
            expanded2 = other.expandvars()
        self.assertEqual(expanded,
                         SwiftPath('swift://tenant/container/blah/another'))
        self.assertEqual(expanded2, other)

    def test_normpath(self):
        original = SwiftPath('swift://tenant/container/another/../b')
        self.assertEqual(original.normpath(),
                         SwiftPath('swift://tenant/container/b'))
        self.assertEqual(SwiftPath("swift://tenant/..").normpath(),
                         SwiftPath("swift://"))
        self.assertEqual(SwiftPath("swift://tenant/container/..").normpath(),
                         SwiftPath("swift://tenant"))


class TestAuthCacheRetrying(SwiftTestCase):
    @mock.patch('stor.swift._clear_cached_auth_credentials', spec_set=True)
    def test_refresh_cache_once_on_auth_err(self, mock_clear_cached_auth_credentials):
        self.mock_swift.download.side_effect = swift.AuthenticationError('auth err')

        with self.assertRaises(swift.AuthenticationError):
            SwiftPath('swift://tenant/container/dir').download('.')

        mock_clear_cached_auth_credentials.assert_called_once_with()


class TestSwiftAuthCaching(SwiftTestCase):
    def setUp(self):
        super(TestSwiftAuthCaching, self).setUp()
        self.disable_get_swift_service_mock()

        test_settings = settings.get()['swift']
        self.auth_url = test_settings['auth_url']
        self.username = test_settings['username']
        self.password = test_settings['password']

        def different_auth_per_tenant(auth_url, username, password, opts):
            return opts['tenant_name'] + 'url', opts['tenant_name'] + 'token'

        self.mock_swift_get_auth_keystone.side_effect = different_auth_per_tenant

    def test_simple_auth_caching(self):
        Path('swift://AUTH_seq_upload_prod')._get_swift_service()
        call_seq = mock.call(self.auth_url, self.username, self.password,
                             {'tenant_name': 'AUTH_seq_upload_prod'})
        self.assertEqual(self.mock_swift_get_auth_keystone.call_args_list, [call_seq])
        Path('swift://AUTH_seq_upload_prod')._get_swift_service()
        self.assertEqual(self.mock_swift_get_auth_keystone.call_args_list, [call_seq])

    def test_swift_auth_caching_multiple_tenants(self):
        Path('swift://AUTH_seq_upload_prod')._get_swift_service()
        call_seq = mock.call(self.auth_url, self.username, self.password,
                             {'tenant_name': 'AUTH_seq_upload_prod'})
        self.assertEqual(self.mock_swift_get_auth_keystone.call_args_list,
                         [call_seq])
        self.assertIn('AUTH_seq_upload_prod', swift._cached_auth_token_map)
        Path('swift://AUTH_seq_upload_prod')._get_swift_service()
        self.assertEqual(self.mock_swift_get_auth_keystone.call_args_list,
                         [call_seq])

        Path('swift://AUTH_final_analysis_prod')._get_swift_service()
        call_final_prod = mock.call(self.auth_url, self.username, self.password,
                                    {'tenant_name': 'AUTH_final_analysis_prod'})
        self.assertEqual(self.mock_swift_get_auth_keystone.call_args_list,
                         [call_seq, call_final_prod])
        Path('swift://AUTH_final_analysis_prod')._get_swift_service()
        self.assertEqual(self.mock_swift_get_auth_keystone.call_args_list,
                         [call_seq, call_final_prod])

    def test_auth_caching_connection(self):
        Path('swift://AUTH_seq_upload_prod')._get_swift_connection()
        call_seq = mock.call(self.auth_url, self.username, self.password,
                             {'tenant_name': 'AUTH_seq_upload_prod'})
        self.assertEqual(self.mock_swift_get_auth_keystone.call_args_list,
                         [call_seq])
        self.assertIn('AUTH_seq_upload_prod', swift._cached_auth_token_map)
        Path('swift://AUTH_seq_upload_prod')._get_swift_connection()
        self.assertEqual(self.mock_swift_get_auth_keystone.call_args_list,
                         [call_seq])

        Path('swift://AUTH_final_analysis_prod')._get_swift_connection()
        call_final_prod = mock.call(self.auth_url, self.username, self.password,
                                    {'tenant_name': 'AUTH_final_analysis_prod'})
        self.assertEqual(self.mock_swift_get_auth_keystone.call_args_list,
                         [call_seq, call_final_prod])
        Path('swift://AUTH_final_analysis_prod')._get_swift_connection()
        self.assertEqual(self.mock_swift_get_auth_keystone.call_args_list,
                         [call_seq, call_final_prod])

    def test_auth_caching_updated_settings(self):
        Path('swift://AUTH_seq_upload_prod')._get_swift_service()
        call_seq = mock.call(self.auth_url, self.username, self.password,
                             {'tenant_name': 'AUTH_seq_upload_prod'})
        self.assertIn('AUTH_seq_upload_prod', swift._cached_auth_token_map)
        self.assertEqual(self.mock_swift_get_auth_keystone.call_args_list, [call_seq])
        Path('swift://AUTH_final_analysis_prod')._get_swift_service()
        self.assertIn('AUTH_final_analysis_prod', swift._cached_auth_token_map)

        settings.update({'swift': {'username': 'new_user'}})
        Path('swift://AUTH_seq_upload_prod')._get_swift_service()

        self.assertNotIn('AUTH_final_analysis_prod', swift._cached_auth_token_map)


class TestIsMethods(SwiftTestCase):
    def setUp(self):
        super(TestIsMethods, self).setUp()
        self.mock_list = self.mock_swift_conn.get_container

    def test_isabs_always_true(self):
        self.assertEqual(SwiftPath('swift://A/B/C').isabs(), True)

    def test_ismount_always_true(self):
        self.assertEqual(SwiftPath('swift://A/B/C').ismount(), True)

    def test_islink_always_false(self):
        self.assertEqual(SwiftPath('swift://A/B/C').islink(), False)

    def test_isfile_existing_file(self):
        self.mock_swift.stat.return_value = _make_stat_response(
            {'items': [('Content-Type', 'text/html')]})
        self.assertTrue(SwiftPath('swift://A/B/C.html').isfile())
        self.mock_swift.stat.assert_called_with(container='B',
                                                objects=['C.html'])

    def test_isfile_no_content_type(self):
        self.mock_swift.stat.return_value = _make_stat_response(
            {'items': []})
        self.assertTrue(SwiftPath('swift://A/B/C.html').isfile())

    def test_isfile_nonexistent_file(self):
        self.mock_swift.stat.side_effect = _service_404_exception()
        self.assertFalse(SwiftPath('swift://A/B/C.html').isfile())

    def test_isfile_directory_sentinel(self):
        self.mock_swift.stat.return_value = _make_stat_response(
            {'items': [('Content Type', 'application/directory')]})
        self.assertFalse(stor.isfile('swift://A/B/C'))
        self.mock_swift.stat.assert_called_with(container='B',
                                                objects=['C'])

    def test_isfile_tenants_and_containers(self):
        self.assertFalse(SwiftPath('swift://AUTH_final').isfile())
        self.assertFalse(SwiftPath('swift://AUTH_final/container').isfile())
        self.assertEqual(self.mock_swift.stat.call_args_list, [])

    def test_isdir_directory_sentinel_empty(self):
        self.mock_swift.stat.return_value = _make_stat_response(
            {'items': [('Content Type', 'application/directory')]})
        self.mock_list.return_value = ({}, [])
        self.assertTrue(SwiftPath('swift://A/B/C').isdir())

    def test_isdir_no_sentinel_subfile(self):
        self.mock_swift.stat.side_effect = _service_404_exception()
        self.mock_list.return_value = ({}, [{'name': 'C/blah'}])
        self.assertTrue(SwiftPath('swift://A/B/C').isdir())

    def test_isdir_no_files(self):
        self.mock_swift.stat.side_effect = _service_404_exception()
        self.mock_list.return_value = ({}, [])
        self.assertFalse(SwiftPath('swift://A/B/C').isdir())

    def test_isdir_404_on_container(self):
        self.mock_swift.stat.side_effect = _service_404_exception()
        self.mock_list.side_effect = _service_404_exception()
        self.assertFalse(SwiftPath('swift://A/B/C').isdir())
