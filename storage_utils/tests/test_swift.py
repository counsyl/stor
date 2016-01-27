import cStringIO
import os
from tempfile import NamedTemporaryFile
import unittest

import mock
from swiftclient.exceptions import ClientException
from swiftclient.service import SwiftError

from storage_utils import path
from storage_utils import swift
from storage_utils.swift import make_condition
from storage_utils.swift import SwiftPath
from storage_utils.test import SwiftTestCase


class TestBasicPathMethods(unittest.TestCase):
    def test_name(self):
        p = path('swift://tenant/container/path/to/resource')
        self.assertEquals(p.name, 'resource')

    def test_parent(self):
        p = path('swift://tenant/container/path/to/resource')
        self.assertEquals(p.parent, 'swift://tenant/container/path/to')

    def test_dirname(self):
        p = path('swift://tenant/container/path/to/resource')
        self.assertEquals(p.dirname(), 'swift://tenant/container/path/to')

    def test_basename(self):
        p = path('swift://tenant/container/path/to/resource')
        self.assertEquals(p.basename(), 'resource')


class TestCondition(unittest.TestCase):
    def test_invalid_condition(self):
        with self.assertRaises(ValueError):
            make_condition('bad_cond', 3)

    def test_eq_cond(self):
        eq3 = make_condition('==', 3)
        self.assertIsNone(eq3.assert_is_met_by(3, 'var'))
        with self.assertRaises(swift.ConditionNotMetError):
            eq3.assert_is_met_by(4, 'var')

    def test_ne_cond(self):
        ne3 = make_condition('!=', 3)
        self.assertIsNone(ne3.assert_is_met_by(4, 'var'))
        with self.assertRaises(swift.ConditionNotMetError):
            ne3.assert_is_met_by(3, 'var')

    def test_gt_cond(self):
        gt3 = make_condition('>', 3)
        self.assertIsNone(gt3.assert_is_met_by(4, 'var'))
        with self.assertRaises(swift.ConditionNotMetError):
            gt3.assert_is_met_by(3, 'var')

    def test_ge_cond(self):
        ge3 = make_condition('>=', 3)
        self.assertIsNone(ge3.assert_is_met_by(4, 'var'))
        self.assertIsNone(ge3.assert_is_met_by(3, 'var'))
        with self.assertRaises(swift.ConditionNotMetError):
            ge3.assert_is_met_by(2, 'var')

    def test_lt_cond(self):
        lt3 = make_condition('<', 3)
        self.assertIsNone(lt3.assert_is_met_by(2, 'var'))
        with self.assertRaises(swift.ConditionNotMetError):
            lt3.assert_is_met_by(3, 'var')

    def test_le_cond(self):
        le3 = make_condition('<=', 3)
        self.assertIsNone(le3.assert_is_met_by(2, 'var'))
        self.assertIsNone(le3.assert_is_met_by(3, 'var'))
        with self.assertRaises(swift.ConditionNotMetError):
            le3.assert_is_met_by(4, 'var')

    def test_repr(self):
        cond = make_condition('<=', 3)
        # Import private _Condition class in order to eval it for test
        from storage_utils.swift import _Condition  # flake8: noqa
        evaled_repr = eval(repr(cond))
        self.assertEquals(cond.operator, evaled_repr.operator)
        self.assertEquals(cond.right_operand, evaled_repr.right_operand)


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
        swift_p = swift_p + 'b' + path('c')
        self.assertTrue(isinstance(swift_p, SwiftPath))
        self.assertEquals(swift_p, 'swift://abc')

    def test_div(self):
        swift_p = SwiftPath('swift://t')
        swift_p = swift_p / 'c' / path('p')
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
    # Note that all of these tests manipulate a mocked env, not the actual
    # env. Mock env setup is performed in SwiftTestCase
    def test_wo_username(self):
        os.environ.pop('OS_USERNAME')
        swift_p = SwiftPath('swift://tenant/')
        with self.assertRaises(swift.ConfigurationError):
            swift_p._get_swift_connection_options()

    def test_wo_password(self):
        os.environ.pop('OS_PASSWORD')
        swift_p = SwiftPath('swift://tenant/')
        with self.assertRaises(swift.ConfigurationError):
            swift_p._get_swift_connection_options()

    def test_w_os_auth_url_env_var(self):
        os.environ['OS_AUTH_URL'] = 'env_auth_url'
        swift_p = SwiftPath('swift://tenant/')
        options = swift_p._get_swift_connection_options()
        self.assertEquals(options['os_auth_url'], 'env_auth_url')
        self.assertEquals(options['os_tenant_name'], 'tenant')

    def test_w_default_setting(self):
        os.environ.pop('OS_AUTH_URL')
        swift_p = SwiftPath('swift://tenant/')
        options = swift_p._get_swift_connection_options()
        self.assertEquals(options['os_auth_url'],
                          swift.DEFAULT_AUTH_URL)
        self.assertEquals(options['os_tenant_name'], 'tenant')

    @mock.patch('storage_utils.swift.auth_url', 'module_auth_url')
    @mock.patch('storage_utils.swift.password', 'module_password')
    @mock.patch('storage_utils.swift.username', 'module_username')
    def test_w_module_settings(self):
        swift_p = SwiftPath('swift://tenant/')
        options = swift_p._get_swift_connection_options()
        self.assertEquals(options['os_auth_url'], 'module_auth_url')
        self.assertEquals(options['os_username'], 'module_username')
        self.assertEquals(options['os_password'], 'module_password')
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


@mock.patch('storage_utils.swift.num_retries', 5)
class TestSwiftFile(SwiftTestCase):
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
        class MyFile(object):
            closed = False
            _buffer = cStringIO.StringIO()
            invalid = swift._delegate_to_buffer('invalid')

        with self.assertRaisesRegexp(AttributeError, 'no attribute'):
            MyFile().invalid()

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
        obj = swift_p.open(mode='r', use_slo=False)
        with self.assertRaisesRegexp(TypeError, 'mode.*write'):
            obj.write('hello')

    @mock.patch('time.sleep', autospec=True)
    @mock.patch.object(SwiftPath, 'upload', autospec=True)
    def test_write_multiple_and_close(self, mock_upload, mock_sleep):
        with NamedTemporaryFile(delete=False) as fp:
            with mock.patch('tempfile.NamedTemporaryFile',
                            autospec=True) as ntf_mock:
                ntf_mock.side_effect = [fp]
                swift_p = SwiftPath('swift://tenant/container/obj')
                obj = swift_p.open(mode='wb', use_slo=False)
                obj.write('hello')
                obj.write(' world')
                obj.close()
            upload, = mock_upload.call_args_list
            upload[0][1][0].source == fp.name
            upload[0][1][0].object_name == swift_p.resource
            self.assertEqual(open(fp.name).read(), 'hello world')

    @mock.patch('time.sleep', autospec=True)
    @mock.patch.object(SwiftPath, 'upload', autospec=True)
    def test_write_multiple_w_context_manager(self, mock_upload, mock_sleep):
        swift_p = SwiftPath('swift://tenant/container/obj')
        with swift_p.open(mode='wb', use_slo=False) as obj:
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
                with swift_p.open(mode='wb', use_slo=False) as obj:
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
        obj = swift_p.open(mode='wb', use_slo=False)
        obj.close()

        self.assertFalse(mock_upload.called)


@mock.patch('storage_utils.swift.num_retries', 5)
class TestList(SwiftTestCase):
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
        with self.assertRaises(swift.ConditionNotMetError):
            swift_p.list(num_objs_cond=make_condition('==', 3))

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
        self.assertEquals(results, [
            'swift://tenant/container/path/to/resource1',
            'swift://tenant/container/path/to/resource2'
        ])

        # Verify that list was retried one time
        self.assertEquals(len(mock_list.call_args_list), 2)

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
        with self.assertRaises(swift.ConditionNotMetError):
            swift_p.list(
                num_objs_cond=make_condition('==', 3),
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
        results = swift_p.list(num_objs_cond=make_condition('>', 1))
        self.assertEquals(results, [
            'swift://tenant/container/path/to/resource1',
            'swift://tenant/container/path/to/resource2'
        ])

        # Verify that list was retried once
        self.assertEquals(len(mock_list.call_args_list), 2)

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
        self.assertEquals(results, [
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
        self.assertEquals(results, [
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
        self.assertEquals(results, [
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
        self.assertEquals(results, [
            'swift://tenant/container/r1',
            'swift://tenant/container/r2'
        ])
        mock_list.assert_called_once_with('container',
                                          prefix='prefix',
                                          limit=None,
                                          full_listing=True)


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
            swift_p.glob('*invalid_pattern*', num_objs_cond=None)

    def test_invalid_glob_pattern(self, mock_list):
        swift_p = SwiftPath('swift://tenant/container')
        with self.assertRaises(ValueError):
            swift_p.glob('invalid_*pattern', num_objs_cond=None)

    @mock.patch('time.sleep', autospec=True)
    def test_cond_not_met(self, mock_list, mock_sleep):
        mock_list.return_value = [
            SwiftPath('swift://tenant/container1'),
            SwiftPath('swift://tenant/container2')
        ]
        swift_p = SwiftPath('swift://tenant/container')
        with self.assertRaises(swift.ConditionNotMetError):
            swift_p.glob('pattern*',
                         num_objs_cond=swift.make_condition('>', 2),
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
                             num_objs_cond=swift.make_condition('==', 2))
        self.assertEquals(paths, [
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
        mock_list = self.mock_swift_conn.get_container
        mock_list.return_value = ({}, [])

        swift_p = SwiftPath('swift://tenant/container')
        result = swift_p.exists()
        self.assertFalse(result)
        mock_list.assert_called_once_with('container', full_listing=False,
                                          limit=1, prefix=None)

    def test_false_404(self):
        mock_list = self.mock_swift_conn.get_container
        mock_list.side_effect = ClientException('not found', http_status=404)

        swift_p = SwiftPath('swift://tenant/container')
        result = swift_p.exists()
        self.assertFalse(result)
        mock_list.assert_called_once_with('container', full_listing=False,
                                          limit=1, prefix=None)

    def test_raises_on_non_404_error(self):
        mock_list = self.mock_swift_conn.get_container
        mock_list.side_effect = ClientException('fail', http_status=504)

        swift_p = SwiftPath('swift://tenant/container')
        with self.assertRaises(swift.SwiftError):
            swift_p.exists()
        mock_list.assert_called_once_with('container', full_listing=False,
                                          limit=1, prefix=None)

    def test_true(self):
        mock_list = self.mock_swift_conn.get_container
        mock_list.return_value = ({}, [{
            'name': 'path1'
        }, {
            'name': 'path2'
        }, {
            'name': 'path3'
        }, {
            'name': 'path4'
        }])

        swift_p = SwiftPath('swift://tenant/container/path')
        result = swift_p.exists()
        self.assertTrue(result)
        mock_list.assert_called_once_with('container', full_listing=False,
                                          limit=1, prefix='path')


@mock.patch('storage_utils.swift.num_retries', 5)
class TestDownload(SwiftTestCase):
    def test_download(self):
        self.mock_swift.download.return_value = []

        swift_p = SwiftPath('swift://tenant/container')
        swift_p.download(output_dir='output_dir')
        self.mock_swift.download.assert_called_once_with(
            'container',
            options={
                'prefix': None,
                'out_directory': 'output_dir',
                'remove_prefix': False,
                'skip_identical': True
            })

    def test_download_w_identical(self):
        # Raise a 304 to simulate the download being identical
        self.mock_swift.download.return_value = [{
            'error': ClientException('', http_status=304)
        }]

        swift_p = SwiftPath('swift://tenant/container')
        swift_p.download(output_dir='output_dir')
        self.mock_swift.download.assert_called_once_with(
            'container',
            options={
                'prefix': None,
                'out_directory': 'output_dir',
                'remove_prefix': False,
                'skip_identical': True
            })

    @mock.patch('time.sleep', autospec=True)
    def test_download_w_condition(self, mock_sleep):
        # Simulate the condition not being met the first call
        self.mock_swift.download.side_effect = [
            [{}, {}],
            [{}, {}, {}]
        ]

        swift_p = SwiftPath('swift://tenant/container')
        swift_p.download(output_dir='output_dir',
                         num_objs_cond=make_condition('==', 3))
        self.assertEquals(len(self.mock_swift.download.call_args_list), 2)

    def test_download_correct_thread_options(self):
        self.disable_get_swift_service_mock()

        swift_p = SwiftPath('swift://tenant/container/path')
        swift_p.download(output_dir='output_dir',
                         object_threads=20,
                         container_threads=30)

        options_passed = self.mock_swift_service.call_args[0][0]
        self.assertEquals(options_passed['object_dd_threads'], 20)
        self.assertEquals(options_passed['container_threads'], 30)


@mock.patch('storage_utils.utils.walk_files_and_dirs', autospec=True)
class TestUpload(SwiftTestCase):
    def test_upload(self, mock_walk_files_and_dirs):
        mock_walk_files_and_dirs.return_value = ['file1', 'file2']
        self.mock_swift.upload.return_value = []

        swift_p = SwiftPath('swift://tenant/container/path')
        swift_p.upload(['upload'],
                       segment_size=1000,
                       use_slo=True,
                       segment_container=True,
                       leave_segments=True,
                       changed=True,
                       object_name='obj_name')
        self.mock_swift.upload.assert_called_once_with(
            'container',
            ['file1', 'file2'],
            options={
                'segment_container': True,
                'use_slo': True,
                'leave_segments': True,
                'segment_size': 1000,
                'changed': True,
                'object_name': 'obj_name'
            })

    def test_upload_thread_options_correct(self, mock_walk_files_and_dirs):
        self.disable_get_swift_service_mock()

        swift_p = SwiftPath('swift://tenant/container/path')
        swift_p.upload([],
                       segment_size=1000,
                       use_slo=True,
                       segment_container=True,
                       leave_segments=True,
                       changed=True,
                       object_name='obj_name',
                       object_threads=20,
                       segment_threads=30)

        options_passed = self.mock_swift_service.call_args[0][0]
        self.assertEquals(options_passed['object_uu_threads'], 20)
        self.assertEquals(options_passed['segment_threads'], 30)


class TestCopy(SwiftTestCase):
    @mock.patch.object(swift.SwiftPath, 'download', autospec=True)
    def test_copy_posix_destination(self, mock_download):
        p = SwiftPath('swift://tenant/container')
        p.copy('path', num_retries=1, object_threads=100)
        mock_download.assert_called_once_with(
            p,
            object_threads=100,
            output_dir=path(u'path'),
            remove_prefix=True)

    def test_copy_swift_destination(self):
        p = SwiftPath('swift://tenant/container')
        with self.assertRaises(ValueError):
            p.copy('swift://swift/path')


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


@mock.patch.object(SwiftPath, 'list', autospec=True)
class TestRmtree(SwiftTestCase):
    def test_w_only_container(self, mock_list):
        self.mock_swift.delete.return_value = {}
        swift_p = SwiftPath('swift://tenant/container')
        swift_p.rmtree()

        self.mock_swift.delete.assert_called_once_with('container')
        self.assertFalse(mock_list.called)

    def test_w_container_and_resource(self, mock_list):
        self.mock_swift.delete.return_value = {}
        mock_list.return_value = [
            SwiftPath('swift://tenant/container/r1'),
            SwiftPath('swift://tenant/container/r2')
        ]

        swift_p = SwiftPath('swift://tenant/container/r')
        swift_p.rmtree()

        self.mock_swift.delete.assert_called_once_with('container',
                                                       ['r1', 'r2'])
        mock_list.assert_called_once_with(mock.ANY)


class TestPost(SwiftTestCase):
    def test_path_error_only_tenant(self):
        # Post() only works on a container path
        swift_p = SwiftPath('swift://tenant')
        with self.assertRaises(ValueError):
            swift_p.post()

    def test_path_error_w_resource(self):
        # Post() does not work with resource paths
        swift_p = SwiftPath('swift://tenant/container/r1')
        with self.assertRaises(ValueError):
            swift_p.post()

    def test_success(self):
        self.mock_swift.post.return_value = {}

        swift_p = SwiftPath('swift://tenant/container')
        swift_p.post()

        self.mock_swift.post.assert_called_once_with(container='container',
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
