from counsyl_io import settings
from counsyl_io.os_path import OSPath
from counsyl_io.swift_path import SwiftPath
from counsyl_io import utils
import mock
from swiftclient.service import SwiftError
from test import test_support
import unittest


class TestNew(unittest.TestCase):
    def test_failed_new(self):
        with self.assertRaises(ValueError):
            SwiftPath('/bad/swift/path')

    def test_successful_new(self):
        swift_path = SwiftPath('swift://tenant/container/path')
        self.assertEquals(swift_path, 'swift://tenant/container/path')


class TestRepr(unittest.TestCase):
    def test_repr(self):
        swift_path = SwiftPath('swift://t/c/p')
        self.assertEquals(repr(swift_path), 'SwiftPath("swift://t/c/p")')
        self.assertEquals(swift_path, 'swift://t/c/p')


class TestTenant(unittest.TestCase):
    def test_tenant_none(self):
        swift_path = SwiftPath('swift://')
        self.assertIsNone(swift_path.tenant)

    def test_tenant_wo_container(self):
        swift_path = SwiftPath('swift://tenant')
        self.assertEquals(swift_path.tenant, 'tenant')

    def test_tenant_w_container(self):
        swift_path = SwiftPath('swift://tenant/container')
        self.assertEquals(swift_path.tenant, 'tenant')


class TestContainer(unittest.TestCase):
    def test_container_none(self):
        swift_path = SwiftPath('swift://tenant/')
        self.assertIsNone(swift_path.container)

    def test_container_wo_resource(self):
        swift_path = SwiftPath('swift://tenant/container')
        self.assertEquals(swift_path.container, 'container')

    def test_container_w_resource(self):
        swift_path = SwiftPath('swift://tenant/container/resource/path')
        self.assertEquals(swift_path.container, 'container')


class TestResource(unittest.TestCase):
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


class TestGetSwiftConnectionOptions(unittest.TestCase):
    def test_w_os_auth_url_env_var(self):
        env = test_support.EnvironmentVarGuard()
        env.set('OS_AUTH_URL', 'env_auth_url')
        swift_path = SwiftPath('swift://tenant/')
        with env:
            options = swift_path._get_swift_connection_options()
            self.assertEquals(options['os_auth_url'], 'env_auth_url')
            self.assertEquals(options['os_tenant_name'], 'tenant')

    def test_w_default_setting(self):
        env = test_support.EnvironmentVarGuard()
        env.unset('OS_AUTH_URL')
        swift_path = SwiftPath('swift://tenant/')
        with env:
            options = swift_path._get_swift_connection_options()
            self.assertEquals(options['os_auth_url'],
                              settings.swift_default_auth_url)
            self.assertEquals(options['os_tenant_name'], 'tenant')


@mock.patch('swiftclient.service.SwiftService', autospec=True)
@mock.patch.object(SwiftPath, '_get_swift_connection_options',
                   autospec=True)
class TestGetSwiftService(unittest.TestCase):
    def test_get_swift_service(self, mock_get_swift_connection_options,
                               mock_service):
        mock_get_swift_connection_options.return_value = {'option': 'value'}
        env = test_support.EnvironmentVarGuard()
        env.set('OS_AUTH_URL', 'env_auth_url')
        swift_path = SwiftPath('swift://tenant/')
        swift_path._get_swift_service()
        mock_service.assert_called_once_with({'option': 'value'})


@mock.patch('swiftclient.service.get_conn', autospec=True)
@mock.patch.object(SwiftPath, '_get_swift_connection_options',
                   autospec=True)
class TestGetSwiftConnection(unittest.TestCase):
    def test_get_swift_service(self, mock_get_swift_connection_options,
                               mock_connection):
        mock_get_swift_connection_options.return_value = {'option': 'value'}
        env = test_support.EnvironmentVarGuard()
        env.set('OS_AUTH_URL', 'env_auth_url')
        swift_path = SwiftPath('swift://tenant/')
        swift_path._get_swift_connection()
        mock_connection.assert_called_once_with({'option': 'value'})


@mock.patch.object(SwiftPath, '_get_swift_connection', autospec=True)
class TestOpen(unittest.TestCase):
    def test_open_success(self, mock_connection):
        swift_connection = mock.Mock()
        swift_connection.get_object.return_value = ('header', 'data')
        mock_connection.return_value = swift_connection

        swift_path = SwiftPath('swift://tenant/container')
        self.assertEquals(swift_path.open().read(), 'data')

    def test_open_invalid_mode(self, mock_connection):
        swift_path = SwiftPath('swift://tenant/container')
        with self.assertRaises(ValueError):
            swift_path.open('w')


@mock.patch.object(SwiftPath, '_get_swift_service', autospec=True)
class TestList(unittest.TestCase):
    def test_error(self, mock_service):
        swift_service = mock.Mock()
        swift_service.list.return_value = [{
            'error': ValueError
        }]
        mock_service.return_value = swift_service

        swift_path = SwiftPath('swift://tenant/container')
        with self.assertRaises(ValueError):
            list(swift_path.list())
        swift_service.list.assert_called_once_with(container='container',
                                                   options={
                                                       'prefix': None
                                                   })

    def test_list_resources_multiple_batches(self, mock_service):
        swift_service = mock.Mock()
        swift_service.list.return_value = [{
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
        mock_service.return_value = swift_service

        swift_path = SwiftPath('swift://tenant/container/path')
        results = list(swift_path.list())
        self.assertEquals(results, [
            'swift://tenant/container/path/to/resource1',
            'swift://tenant/container/path/to/resource2',
            'swift://tenant/container/path/to/resource3',
            'swift://tenant/container/path/to/resource4'
        ])
        swift_service.list.assert_called_once_with(container='container',
                                                   options={
                                                       'prefix': 'path'
                                                   })

    def test_list_containers(self, mock_service):
        swift_service = mock.Mock()
        swift_service.list.return_value = [{
            'container': None,
            'listing': [{
                'name': 'container1'
            }, {
                'name': 'container2'
            }]
        }]
        mock_service.return_value = swift_service
        swift_path = SwiftPath('swift://tenant')
        results = list(swift_path.list())
        self.assertEquals(results, [
            'swift://tenant/container1',
            'swift://tenant/container2'
        ])
        swift_service.list.assert_called_once_with(container=None,
                                                   options={
                                                       'prefix': None
                                                   })

    def test_list_starts_with(self, mock_service):
        swift_service = mock.Mock()
        swift_service.list.return_value = [{
            'container': 'container',
            'listing': [{
                'name': 'r1'
            }, {
                'name': 'r2'
            }]
        }]
        mock_service.return_value = swift_service
        swift_path = SwiftPath('swift://tenant/container/r')
        results = list(swift_path.list(starts_with='prefix'))
        self.assertEquals(results, [
            'swift://tenant/container/r1',
            'swift://tenant/container/r2'
        ])
        swift_service.list.assert_called_once_with(container='container',
                                                   options={
                                                       'prefix': 'r/prefix'
                                                   })


@mock.patch.object(SwiftPath, 'list', autospec=True)
class TestGlob(unittest.TestCase):
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
class TestFirst(unittest.TestCase):
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
class TestExists(unittest.TestCase):
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


@mock.patch.object(SwiftPath, '_get_swift_service', autospec=True)
class TestDownload(unittest.TestCase):
    def test_false(self, mock_service):
        swift_service = mock.Mock()
        swift_service.download.return_value = []
        mock_service.return_value = swift_service

        swift_path = SwiftPath('swift://tenant/container')
        swift_path.download(output_dir='output_dir')
        swift_service.download.assert_called_once_with(
            'container',
            options={
                'prefix': None,
                'out_directory': 'output_dir',
                'remove_prefix': False
            })


class TestWalkUploadNames(unittest.TestCase):
    def test_w_dir(self):
        # Create an empty directory for this test in ./swift_upload. This
        # is because git doesnt allow a truly empty directory to be checked
        # in
        swift_path = SwiftPath('swift://tenant')
        swift_dir = OSPath(__file__).absexpand().parent / 'swift_upload'
        with utils.NamedTemporaryDirectory(dir=swift_dir) as tmp_dir:
            uploads = swift_path._walk_upload_names([swift_dir])
            self.assertEquals(set(uploads), set([
                swift_dir / 'file1',
                tmp_dir,
                swift_dir / 'data_dir' / 'file2',
            ]))

    def test_w_file(self):
        name = OSPath(__file__).absexpand().parent / 'swift_upload' / 'file1'

        swift_path = SwiftPath('swift://tenant')
        uploads = swift_path._walk_upload_names([name])
        self.assertEquals(set(uploads), set([name]))

    def test_w_invalid_file(self):
        name = OSPath(__file__).absexpand().parent / 'swift_upload' / 'invalid'

        swift_path = SwiftPath('swift://tenant')
        with self.assertRaises(ValueError):
            swift_path._walk_upload_names([name])


@mock.patch.object(SwiftPath, '_get_swift_service', autospec=True)
@mock.patch.object(SwiftPath, '_walk_upload_names', autospec=True)
class TestUpload(unittest.TestCase):
    def test_false(self, mock_walk_upload_names, mock_service):
        mock_walk_upload_names.return_value = ['file1', 'file2']
        swift_service = mock.Mock()
        swift_service.upload.return_value = []
        mock_service.return_value = swift_service

        swift_path = SwiftPath('swift://tenant/container/path')
        swift_path.upload(['upload'],
                          segment_size=1000,
                          use_slo=True,
                          segment_container=True,
                          leave_segments=True,
                          changed=True,
                          object_name='obj_name')
        swift_service.upload.assert_called_once_with(
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


@mock.patch.object(SwiftPath, '_get_swift_service', autospec=True)
class TestRemove(unittest.TestCase):
    def test_invalid_remove(self, mock_service):
        # Remove()s must happen on a resource of a container
        swift_path = SwiftPath('swift://tenant/container')
        with self.assertRaises(ValueError):
            swift_path.remove()

    def test_w_swift_error(self, mock_service):
        swift_service = mock.Mock()
        swift_service.delete.return_value = {
            'error': SwiftError('error')
        }
        mock_service.return_value = swift_service
        swift_path = SwiftPath('swift://tenant/container/r')
        with self.assertRaises(SwiftError):
            swift_path.remove()

        swift_service.delete.assert_called_once_with('container', ['r'])

    def test_success(self, mock_service):
        swift_service = mock.Mock()
        swift_service.delete.return_value = {}
        mock_service.return_value = swift_service
        swift_path = SwiftPath('swift://tenant/container/r')
        swift_path.remove()

        swift_service.delete.assert_called_once_with('container', ['r'])


@mock.patch.object(SwiftPath, '_get_swift_service', autospec=True)
@mock.patch.object(SwiftPath, 'list', autospec=True)
class TestRmtree(unittest.TestCase):
    def test_w_only_container(self, mock_list, mock_service):
        swift_service = mock.Mock()
        swift_service.delete.return_value = {}
        mock_service.return_value = swift_service
        swift_path = SwiftPath('swift://tenant/container')
        swift_path.rmtree()

        swift_service.delete.assert_called_once_with('container')
        self.assertFalse(mock_list.called)

    def test_w_container_and_resource(self, mock_list, mock_service):
        swift_service = mock.Mock()
        swift_service.delete.return_value = {}
        mock_service.return_value = swift_service
        mock_list.return_value = [
            SwiftPath('swift://tenant/container/r1'),
            SwiftPath('swift://tenant/container/r2')
        ]

        swift_path = SwiftPath('swift://tenant/container/r')
        swift_path.rmtree()

        swift_service.delete.assert_called_once_with('container', ['r1', 'r2'])
        mock_list.assert_called_once_with(mock.ANY)


@mock.patch.object(SwiftPath, '_get_swift_service', autospec=True)
class TestPost(unittest.TestCase):
    def test_path_error_only_tenant(self, mock_service):
        # Post() only works on a container path
        swift_path = SwiftPath('swift://tenant')
        with self.assertRaises(ValueError):
            swift_path.post()

    def test_path_error_w_resource(self, mock_service):
        # Post() does not work with resource paths
        swift_path = SwiftPath('swift://tenant/container/r1')
        with self.assertRaises(ValueError):
            swift_path.post()

    def test_success(self, mock_service):
        swift_service = mock.Mock()
        swift_service.post.return_value = {}
        mock_service.return_value = swift_service

        swift_path = SwiftPath('swift://tenant/container')
        swift_path.post()

        swift_service.post.assert_called_once_with(container='container',
                                                   options=None)
