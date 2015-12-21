from storage_utils.swift_path import SwiftClientError
from storage_utils.swift_path import SwiftCondition
from storage_utils.swift_path import SwiftConditionError
from storage_utils.swift_path import SwiftConfigurationError
from storage_utils.swift_path import SwiftNotFoundError
from storage_utils.swift_path import SwiftPath
from storage_utils.test import SwiftTestCase
import mock
import os
from path import Path
from swiftclient.exceptions import ClientException
from swiftclient.service import SwiftError
import unittest


class TestSwiftCondition(unittest.TestCase):
    def test_invalid_condition(self):
        with self.assertRaises(ValueError):
            SwiftCondition('bad_cond', 3)

    def test_eq_cond(self):
        eq3 = SwiftCondition('==', 3)
        self.assertTrue(eq3.is_met_by(3))
        self.assertFalse(eq3.is_met_by(4))

    def test_ne_cond(self):
        ne3 = SwiftCondition('!=', 3)
        self.assertFalse(ne3.is_met_by(3))
        self.assertTrue(ne3.is_met_by(4))

    def test_gt_cond(self):
        gt3 = SwiftCondition('>', 3)
        self.assertTrue(gt3.is_met_by(4))
        self.assertFalse(gt3.is_met_by(3))

    def test_ge_cond(self):
        ge3 = SwiftCondition('>=', 3)
        self.assertTrue(ge3.is_met_by(4))
        self.assertTrue(ge3.is_met_by(3))
        self.assertFalse(ge3.is_met_by(2))

    def test_lt_cond(self):
        lt3 = SwiftCondition('<', 3)
        self.assertTrue(lt3.is_met_by(2))
        self.assertFalse(lt3.is_met_by(3))

    def test_le_cond(self):
        le3 = SwiftCondition('<=', 3)
        self.assertTrue(le3.is_met_by(2))
        self.assertTrue(le3.is_met_by(3))
        self.assertFalse(le3.is_met_by(4))

    def test_repr(self):
        cond = SwiftCondition('<=', 3)
        evaled_repr = eval(repr(cond))
        self.assertEquals(cond.operator, evaled_repr.operator)
        self.assertEquals(cond.right_operand, evaled_repr.right_operand)


class TestNew(SwiftTestCase):
    def test_failed_new(self):
        with self.assertRaises(ValueError):
            SwiftPath('/bad/swift/path')

    def test_successful_new(self):
        swift_path = SwiftPath('swift://tenant/container/path')
        self.assertEquals(swift_path, 'swift://tenant/container/path')


class TestRepr(SwiftTestCase):
    def test_repr(self):
        swift_path = SwiftPath('swift://t/c/p')
        self.assertEquals(eval(repr(swift_path)), swift_path)


class TestPathManipulations(SwiftTestCase):
    def test_add(self):
        swift_path = SwiftPath('swift://a')
        swift_path = swift_path + 'b' + Path('c')
        self.assertTrue(isinstance(swift_path, SwiftPath))
        self.assertEquals(swift_path, 'swift://abc')

    def test_div(self):
        swift_path = SwiftPath('swift://t')
        swift_path = swift_path / 'c' / Path('p')
        self.assertTrue(isinstance(swift_path, SwiftPath))
        self.assertEquals(swift_path, 'swift://t/c/p')


class TestTenant(SwiftTestCase):
    def test_tenant_none(self):
        swift_path = SwiftPath('swift://')
        self.assertIsNone(swift_path.tenant)

    def test_tenant_wo_container(self):
        swift_path = SwiftPath('swift://tenant')
        self.assertEquals(swift_path.tenant, 'tenant')

    def test_tenant_w_container(self):
        swift_path = SwiftPath('swift://tenant/container')
        self.assertEquals(swift_path.tenant, 'tenant')


class TestContainer(SwiftTestCase):
    def test_container_none(self):
        swift_path = SwiftPath('swift://tenant/')
        self.assertIsNone(swift_path.container)

    def test_container_wo_resource(self):
        swift_path = SwiftPath('swift://tenant/container')
        self.assertEquals(swift_path.container, 'container')

    def test_container_w_resource(self):
        swift_path = SwiftPath('swift://tenant/container/resource/path')
        self.assertEquals(swift_path.container, 'container')


class TestResource(SwiftTestCase):
    def test_no_container(self):
        swift_path = SwiftPath('swift://tenant/')
        self.assertIsNone(swift_path.resource)

    def test_w_container_no_resource(self):
        swift_path = SwiftPath('swift://tenant/container/')
        self.assertIsNone(swift_path.resource)

    def test_resource_single_object(self):
        swift_path = SwiftPath('swift://tenant/container/obj')
        self.assertEquals(swift_path.resource, 'obj')

    def test_resource_single_dir_w_slash(self):
        swift_path = SwiftPath('swift://tenant/container/dir/')
        self.assertEquals(swift_path.resource, 'dir/')

    def test_resource_nested_dir_wo_slash(self):
        swift_path = SwiftPath('swift://tenant/container/nested/dir')
        self.assertEquals(swift_path.resource, 'nested/dir')


class TestGetSwiftConnectionOptions(SwiftTestCase):
    # Note that all of these tests manipulate a mocked env, not the actual
    # env. Mock env setup is performed in SwiftTestCase
    def test_wo_username(self):
        os.environ.pop('OS_USERNAME')
        swift_path = SwiftPath('swift://tenant/')
        with self.assertRaises(SwiftConfigurationError):
            swift_path._get_swift_connection_options()

    def test_wo_password(self):
        os.environ.pop('OS_PASSWORD')
        swift_path = SwiftPath('swift://tenant/')
        with self.assertRaises(SwiftConfigurationError):
            swift_path._get_swift_connection_options()

    def test_w_os_auth_url_env_var(self):
        os.environ['OS_AUTH_URL'] = 'env_auth_url'
        swift_path = SwiftPath('swift://tenant/')
        options = swift_path._get_swift_connection_options()
        self.assertEquals(options['os_auth_url'], 'env_auth_url')
        self.assertEquals(options['os_tenant_name'], 'tenant')

    def test_w_default_setting(self):
        os.environ.pop('OS_AUTH_URL')
        swift_path = SwiftPath('swift://tenant/')
        options = swift_path._get_swift_connection_options()
        self.assertEquals(options['os_auth_url'],
                          SwiftPath.default_auth_url)
        self.assertEquals(options['os_tenant_name'], 'tenant')


@mock.patch.object(SwiftPath, '_get_swift_connection_options',
                   autospec=True)
class TestGetSwiftService(SwiftTestCase):
    def test_get_swift_service(self, mock_get_swift_connection_options):
        self.disable_get_swift_service_mock()

        mock_get_swift_connection_options.return_value = {'option': 'value'}
        swift_path = SwiftPath('swift://tenant/')
        swift_path._get_swift_service()
        self.mock_swift_service.assert_called_once_with({'option': 'value'})


@mock.patch.object(SwiftPath, '_get_swift_connection_options',
                   autospec=True)
class TestGetSwiftConnection(SwiftTestCase):
    def test_get_swift_connection(self, mock_get_swift_connection_options):
        mock_get_swift_connection_options.return_value = {'option': 'value'}
        swift_path = SwiftPath('swift://tenant/')
        swift_path._get_swift_connection()
        self.mock_swift_get_conn.assert_called_once_with({'option': 'value'})


class TestOpen(SwiftTestCase):
    def test_open_success(self):
        self.mock_swift_conn.get_object.return_value = ('header', 'data')

        swift_path = SwiftPath('swift://tenant/container')
        self.assertEquals(swift_path.open().read(), 'data')

    @mock.patch('time.sleep', autospec=True)
    def test_open_success_on_second_try(self, mock_sleep):
        self.mock_swift_conn.get_object.side_effect = [
            ClientException('dummy', 'dummy'),
            ('header', 'data')
        ]
        swift_path = SwiftPath('swift://tenant/container')
        print 'opening'
        # print 'results', swift_path.open()
        obj = swift_path.open()
        print 'opened obj', obj
        self.assertEquals(obj.read(), 'data')
        self.assertEquals(len(mock_sleep.call_args_list), 1)

    def test_open_invalid_mode(self):
        swift_path = SwiftPath('swift://tenant/container')
        with self.assertRaises(ValueError):
            swift_path.open('w')


class TestList(SwiftTestCase):
    def test_error(self):
        mock_list = self.mock_swift_conn.get_container
        mock_list.side_effect = ValueError

        swift_path = SwiftPath('swift://tenant/container')
        with self.assertRaises(ValueError):
            list(swift_path.list())
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

        swift_path = SwiftPath('swift://tenant/container/path')
        with self.assertRaises(SwiftConditionError):
            swift_path.list(num_objs_cond=SwiftCondition('==', 3))

        # Verify that list was retried at least once
        self.assertTrue(len(mock_list.call_args_list) > 1)

    @mock.patch('time.sleep', autospec=True)
    def test_list_condition_not_met_custom_retry_logic(self, mock_sleep):
        mock_list = self.mock_swift_conn.get_container
        mock_list.return_value = ({}, [{
            'name': 'path/to/resource1'
        }, {
            'name': 'path/to/resource2'
        }])

        swift_path = SwiftPath('swift://tenant/container/path')
        with self.assertRaises(SwiftConditionError):
            swift_path.list(
                num_objs_cond=SwiftCondition('==', 3),
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

        swift_path = SwiftPath('swift://tenant/container/path')
        results = list(swift_path.list(num_objs_cond=SwiftCondition('>', 1)))
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

        swift_path = SwiftPath('swift://tenant/container/path')
        results = list(swift_path.list())
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

        swift_path = SwiftPath('swift://tenant/container/path')
        results = list(swift_path.list(limit=1))
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
        swift_path = SwiftPath('swift://tenant')
        results = list(swift_path.list())
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
        swift_path = SwiftPath('swift://tenant/container/r')
        results = list(swift_path.list(starts_with='prefix'))
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
        swift_path = SwiftPath('swift://tenant/container')
        results = list(swift_path.list(starts_with='prefix'))
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
        swift_path = SwiftPath('swift://tenant/container')
        swift_path.glob('pattern*')
        mock_list.assert_called_once_with(mock.ANY, starts_with='pattern',
                                          num_objs_cond=None)

    def test_valid_pattern_wo_wildcard(self, mock_list):
        swift_path = SwiftPath('swift://tenant/container')
        swift_path.glob('pattern')
        mock_list.assert_called_once_with(mock.ANY, starts_with='pattern',
                                          num_objs_cond=None)

    def test_multi_glob_pattern(self, mock_list):
        swift_path = SwiftPath('swift://tenant/container')
        with self.assertRaises(ValueError):
            swift_path.glob('*invalid_pattern*', num_objs_cond=None)

    def test_invalid_glob_pattern(self, mock_list):
        swift_path = SwiftPath('swift://tenant/container')
        with self.assertRaises(ValueError):
            swift_path.glob('invalid_*pattern', num_objs_cond=None)


@mock.patch.object(SwiftPath, 'list', autospec=True)
class TestFirst(SwiftTestCase):
    def test_none(self, mock_list):
        mock_list.return_value = []

        swift_path = SwiftPath('swift://tenant/container')
        result = swift_path.first()
        self.assertIsNone(result)

    def test_w_results(self, mock_list):
        mock_list.return_value = [
            SwiftPath('swift://tenant/container/path1'),
            SwiftPath('swift://tenant/container/path2'),
            SwiftPath('swift://tenant/container/path3'),
            SwiftPath('swift://tenant/container/path4'),
        ]

        swift_path = SwiftPath('swift://tenant/container/path')
        result = swift_path.first()
        self.assertEquals(result, 'swift://tenant/container/path1')


@mock.patch.object(SwiftPath, 'list', autospec=True)
class TestExists(SwiftTestCase):
    def test_false(self, mock_list):
        mock_list.return_value = []

        swift_path = SwiftPath('swift://tenant/container')
        result = swift_path.exists()
        self.assertFalse(result)
        mock_list.assert_called_once_with(mock.ANY, limit=1)

    def test_false_404(self, mock_list):
        mock_list.side_effect = SwiftNotFoundError('not found')

        swift_path = SwiftPath('swift://tenant/container')
        result = swift_path.exists()
        self.assertFalse(result)
        mock_list.assert_called_once_with(mock.ANY, limit=1)

    def test_raises_on_non_404_error(self, mock_list):
        mock_list.side_effect = ClientException('fail', http_status=504)

        swift_path = SwiftPath('swift://tenant/container')
        with self.assertRaises(ClientException):
            swift_path.exists()
        mock_list.assert_called_once_with(mock.ANY, limit=1)

    def test_true(self, mock_list):
        mock_list.return_value = [
            SwiftPath('swift://tenant/container/path1'),
            SwiftPath('swift://tenant/container/path2'),
            SwiftPath('swift://tenant/container/path3'),
            SwiftPath('swift://tenant/container/path4'),
        ]

        swift_path = SwiftPath('swift://tenant/container/path')
        result = swift_path.exists()
        self.assertTrue(result)
        mock_list.assert_called_once_with(mock.ANY, limit=1)


class TestDownload(SwiftTestCase):
    def test_download(self):
        self.mock_swift.download.return_value = []

        swift_path = SwiftPath('swift://tenant/container')
        swift_path.download(output_dir='output_dir')
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

        swift_path = SwiftPath('swift://tenant/container')
        swift_path.download(output_dir='output_dir')
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

        swift_path = SwiftPath('swift://tenant/container')
        swift_path.download(output_dir='output_dir',
                            num_objs_cond=SwiftCondition('==', 3))
        self.assertEquals(len(self.mock_swift.download.call_args_list), 2)

    def test_download_correct_thread_options(self):
        self.disable_get_swift_service_mock()

        swift_path = SwiftPath('swift://tenant/container/path')
        swift_path.download(output_dir='output_dir',
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

        swift_path = SwiftPath('swift://tenant/container/path')
        swift_path.upload(['upload'],
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

        swift_path = SwiftPath('swift://tenant/container/path')
        swift_path.upload([],
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


class TestRemove(SwiftTestCase):
    def test_invalid_remove(self):
        # Remove()s must happen on a resource of a container
        swift_path = SwiftPath('swift://tenant/container')
        with self.assertRaises(ValueError):
            swift_path.remove()

    def test_w_swift_error(self):
        self.mock_swift.delete.return_value = {
            'error': SwiftError('error')
        }
        swift_path = SwiftPath('swift://tenant/container/r')
        with self.assertRaises(SwiftClientError):
            swift_path.remove()

        self.mock_swift.delete.assert_called_once_with('container', ['r'])

    def test_success(self):
        self.mock_swift.delete.return_value = {}
        swift_path = SwiftPath('swift://tenant/container/r')
        swift_path.remove()

        self.mock_swift.delete.assert_called_once_with('container', ['r'])


@mock.patch.object(SwiftPath, 'list', autospec=True)
class TestRmtree(SwiftTestCase):
    def test_w_only_container(self, mock_list):
        self.mock_swift.delete.return_value = {}
        swift_path = SwiftPath('swift://tenant/container')
        swift_path.rmtree()

        self.mock_swift.delete.assert_called_once_with('container')
        self.assertFalse(mock_list.called)

    def test_w_container_and_resource(self, mock_list):
        self.mock_swift.delete.return_value = {}
        mock_list.return_value = [
            SwiftPath('swift://tenant/container/r1'),
            SwiftPath('swift://tenant/container/r2')
        ]

        swift_path = SwiftPath('swift://tenant/container/r')
        swift_path.rmtree()

        self.mock_swift.delete.assert_called_once_with('container',
                                                       ['r1', 'r2'])
        mock_list.assert_called_once_with(mock.ANY)


class TestPost(SwiftTestCase):
    def test_path_error_only_tenant(self):
        # Post() only works on a container path
        swift_path = SwiftPath('swift://tenant')
        with self.assertRaises(ValueError):
            swift_path.post()

    def test_path_error_w_resource(self):
        # Post() does not work with resource paths
        swift_path = SwiftPath('swift://tenant/container/r1')
        with self.assertRaises(ValueError):
            swift_path.post()

    def test_success(self):
        self.mock_swift.post.return_value = {}

        swift_path = SwiftPath('swift://tenant/container')
        swift_path.post()

        self.mock_swift.post.assert_called_once_with(container='container',
                                                     options=None)
