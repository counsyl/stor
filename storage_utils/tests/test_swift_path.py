from storage_utils.swift_path import SwiftConfigurationError
from storage_utils.swift_path import SwiftPath
from storage_utils.test import SwiftTestCase
import mock
import os
from swiftclient.service import SwiftError


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
        swift_path = SwiftPath('swift://t')
        swift_path = swift_path + 'added'
        self.assertTrue(isinstance(swift_path, SwiftPath))
        self.assertEquals(swift_path, 'swift://tadded')

    def test_div(self):
        swift_path = SwiftPath('swift://t')
        swift_path = swift_path / 'c' / 'p'
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

    def test_open_invalid_mode(self):
        swift_path = SwiftPath('swift://tenant/container')
        with self.assertRaises(ValueError):
            swift_path.open('w')


class TestList(SwiftTestCase):
    def test_error(self):
        self.mock_swift.list.return_value = [{
            'error': ValueError
        }]

        swift_path = SwiftPath('swift://tenant/container')
        with self.assertRaises(ValueError):
            list(swift_path.list())
        self.mock_swift.list.assert_called_once_with(container='container',
                                                     options={
                                                         'prefix': None
                                                     })

    def test_list_resources_multiple_batches(self):
        self.mock_swift.list.return_value = [{
            'container': 'container',
            'listing': [{
                'name': 'path/to/resource1'
            }, {
                'name': 'path/to/resource2'
            }]
        }, {
            'container': 'container',
            'listing': [{
                'name': 'path/to/resource3'
            }, {
                'name': 'path/to/resource4'
            }]
        }]

        swift_path = SwiftPath('swift://tenant/container/path')
        results = list(swift_path.list())
        self.assertEquals(results, [
            'swift://tenant/container/path/to/resource1',
            'swift://tenant/container/path/to/resource2',
            'swift://tenant/container/path/to/resource3',
            'swift://tenant/container/path/to/resource4'
        ])
        self.mock_swift.list.assert_called_once_with(container='container',
                                                     options={
                                                         'prefix': 'path'
                                                     })

    def test_list_containers(self):
        self.mock_swift.list.return_value = [{
            'container': None,
            'listing': [{
                'name': 'container1'
            }, {
                'name': 'container2'
            }]
        }]
        swift_path = SwiftPath('swift://tenant')
        results = list(swift_path.list())
        self.assertEquals(results, [
            'swift://tenant/container1',
            'swift://tenant/container2'
        ])
        self.mock_swift.list.assert_called_once_with(container=None,
                                                     options={
                                                         'prefix': None
                                                     })

    def test_list_starts_with(self):
        self.mock_swift.list.return_value = [{
            'container': 'container',
            'listing': [{
                'name': 'r1'
            }, {
                'name': 'r2'
            }]
        }]
        swift_path = SwiftPath('swift://tenant/container/r')
        results = list(swift_path.list(starts_with='prefix'))
        self.assertEquals(results, [
            'swift://tenant/container/r1',
            'swift://tenant/container/r2'
        ])
        self.mock_swift.list.assert_called_once_with(container='container',
                                                     options={
                                                         'prefix': 'r/prefix'
                                                     })


@mock.patch.object(SwiftPath, 'list', autospec=True)
class TestGlob(SwiftTestCase):
    def test_valid_pattern(self, mock_list):
        swift_path = SwiftPath('swift://tenant/container')
        swift_path.glob('pattern*')
        mock_list.assert_called_once_with(mock.ANY, starts_with='pattern')

    def test_valid_pattern_wo_wildcard(self, mock_list):
        swift_path = SwiftPath('swift://tenant/container')
        swift_path.glob('pattern')
        mock_list.assert_called_once_with(mock.ANY, starts_with='pattern')

    def test_multi_glob_pattern(self, mock_list):
        swift_path = SwiftPath('swift://tenant/container')
        with self.assertRaises(ValueError):
            swift_path.glob('*invalid_pattern*')

    def test_invalid_glob_pattern(self, mock_list):
        swift_path = SwiftPath('swift://tenant/container')
        with self.assertRaises(ValueError):
            swift_path.glob('invalid_*pattern')


@mock.patch.object(SwiftPath, 'list', autospec=True)
class TestFirst(SwiftTestCase):
    def test_none(self, mock_list):
        mock_list.return_value = iter([])

        swift_path = SwiftPath('swift://tenant/container')
        result = swift_path.first()
        self.assertIsNone(result)

    def test_w_results(self, mock_list):
        mock_list.return_value = iter([
            SwiftPath('swift://tenant/container/path1'),
            SwiftPath('swift://tenant/container/path2'),
            SwiftPath('swift://tenant/container/path3'),
            SwiftPath('swift://tenant/container/path4'),
        ])

        swift_path = SwiftPath('swift://tenant/container/path')
        result = swift_path.first()
        self.assertEquals(result, 'swift://tenant/container/path1')


@mock.patch.object(SwiftPath, 'list', autospec=True)
class TestExists(SwiftTestCase):
    def test_false(self, mock_list):
        mock_list.return_value = iter([])

        swift_path = SwiftPath('swift://tenant/container')
        result = swift_path.exists()
        self.assertFalse(result)

    def test_false_404(self, mock_list):
        mock_list.side_effect = SwiftError('error',
                                           exc=mock.Mock(http_status=404))

        swift_path = SwiftPath('swift://tenant/container')
        result = swift_path.exists()
        self.assertFalse(result)

    def test_raises_on_non_404_error(self, mock_list):
        mock_list.side_effect = SwiftError('error',
                                           exc=mock.Mock(http_status=504))

        swift_path = SwiftPath('swift://tenant/container')
        with self.assertRaises(SwiftError):
            swift_path.exists()

    def test_true(self, mock_list):
        mock_list.return_value = iter([
            SwiftPath('swift://tenant/container/path1'),
            SwiftPath('swift://tenant/container/path2'),
            SwiftPath('swift://tenant/container/path3'),
            SwiftPath('swift://tenant/container/path4'),
        ])

        swift_path = SwiftPath('swift://tenant/container/path')
        result = swift_path.exists()
        self.assertTrue(result)


class TestDownload(SwiftTestCase):
    def test_false(self):
        self.mock_swift.download.return_value = []

        swift_path = SwiftPath('swift://tenant/container')
        swift_path.download(output_dir='output_dir')
        self.mock_swift.download.assert_called_once_with(
            'container',
            options={
                'prefix': None,
                'out_directory': 'output_dir',
                'remove_prefix': False
            })


@mock.patch('storage_utils.utils.walk_files_and_dirs', autospec=True)
class TestUpload(SwiftTestCase):
    def test_false(self, mock_walk_files_and_dirs):
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
        with self.assertRaises(SwiftError):
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
