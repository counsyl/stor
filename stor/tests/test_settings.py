import copy
import mock
import os
from stor import settings
import threading
import time
import unittest

test_settings = {
    'swift:download': {
        'container_threads': 10,
        'object_threads': 10,
        'shuffle': True,
        'skip_identical': True
    },
    'swift:upload': {
        'changed': False,
        'checksum': True,
        'leave_segments': True,
        'object_threads': 10,
        'segment_size': 1073741824,
        'segment_threads': 10,
        'skip_identical': False,
        'use_slo': True
    }
}


@mock.patch.dict('stor.settings._global_settings', copy.deepcopy(test_settings),
                 clear=True)
class TestSettings(unittest.TestCase):
    @mock.patch.dict(os.environ, {}, clear=True)
    def test_initialize_default(self):
        expected_settings = {
            'stor': {},
            's3': {},
            's3:upload': {
                'segment_size': 8388608,
                'object_threads': 10,
                'segment_threads': 10
            },
            's3:download': {
                'segment_size': 8388608,
                'object_threads': 10,
                'segment_threads': 10
            },
            'swift': {
                'username': '',
                'password': '',
                'auth_url': '',
                'temp_url_key': '',
                'num_retries': 0
            },
            'swift:delete': {
                'object_threads': 10
            },
            'swift:download': {
                'container_threads': 10,
                'object_threads': 10,
                'shuffle': True,
                'skip_identical': True
            },
            'swift:upload': {
                'changed': False,
                'checksum': True,
                'leave_segments': True,
                'object_threads': 10,
                'segment_size': 1073741824,
                'segment_threads': 10,
                'skip_identical': False,
                'use_slo': True
            }
        }
        settings._initialize()
        self.assertEquals(settings._global_settings, expected_settings)

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_initialize_w_user_file(self):
        expected_settings = {
            'stor': {},
            's3': {},
            's3:upload': {
                'segment_size': 8388608,
                'object_threads': 10,
                'segment_threads': 10
            },
            's3:download': {
                'segment_size': 8388608,
                'object_threads': 10,
                'segment_threads': 10
            },
            'swift': {
                'username': 'fake_user',
                'password': 'fake_password',
                'auth_url': '',
                'temp_url_key': '',
                'num_retries': 0
            },
            'swift:delete': {
                'object_threads': 10
            },
            'swift:download': {
                'container_threads': 10,
                'object_threads': 10,
                'shuffle': True,
                'skip_identical': True
            },
            'swift:upload': {
                'changed': False,
                'checksum': True,
                'leave_segments': True,
                'object_threads': 10,
                'segment_size': 1073741824,
                'segment_threads': 10,
                'skip_identical': False,
                'use_slo': True
            }
        }
        filename = os.path.join(os.path.dirname(__file__), 'file_data', 'test.cfg')
        with mock.patch('stor.settings.USER_CONFIG_FILE', filename):
            settings._initialize()
        self.assertEquals(settings._global_settings, expected_settings)

    @mock.patch.dict(os.environ, {'OS_USERNAME': 'test_username'})
    @mock.patch.dict(os.environ, {'OS_PASSWORD': 'test_password'})
    @mock.patch.dict(os.environ, {'OS_NUM_RETRIES': '2'})
    @mock.patch.dict(os.environ, {'OS_AUTH_URL': 'http://test_auth_url.com'})
    def test_env_vars_loaded(self):
        print os.environ
        print settings.get()
        settings._initialize()
        print settings.get()
        print os.environ
        initial_settings = settings.get()['swift']
        self.assertEquals(initial_settings['username'], 'test_username')
        self.assertEquals(initial_settings['password'], 'test_password')
        self.assertEquals(initial_settings['num_retries'], 2)
        self.assertEquals(initial_settings['auth_url'], 'http://test_auth_url.com')

    def test_get(self):
        self.assertEquals(settings.get(), test_settings)

    @mock.patch.dict('stor.settings._global_settings',
                     copy.deepcopy(test_settings), clear=True)
    def test_update_w_settings(self):
        update_settings = {
            'swift:upload': {
                'skip_identical': True,
                'object_threads': 30
            },
            'swift:download': {
                'object_threads': 20
            }
        }
        expected_settings = {
            'swift:download': {
                'container_threads': 10,
                'object_threads': 20,
                'shuffle': True,
                'skip_identical': True
            },
            'swift:upload': {
                'changed': False,
                'checksum': True,
                'leave_segments': True,
                'object_threads': 30,
                'segment_size': 1073741824,
                'segment_threads': 10,
                'skip_identical': True,
                'use_slo': True
            }
        }
        settings.update(settings=update_settings)
        self.assertEquals(settings._global_settings, expected_settings)

    def test_update_wo_settings(self):
        settings.update()
        self.assertEquals(settings._global_settings, test_settings)

    @mock.patch.dict('stor.settings._global_settings',
                     {'foo': 1}, clear=True)
    def test_update_validation_error(self):
        with self.assertRaisesRegexp(ValueError, 'not a valid setting'):
            settings.update({'foo': {'bar': 3}})
        with self.assertRaisesRegexp(ValueError, 'not a valid setting'):
            settings.update({'bar': 4})

    def test_use_w_settings(self):
        update_settings = {
            'swift:upload': {
                'skip_identical': True,
                'object_threads': 30
            },
            'swift:download': {
                'object_threads': 20
            }
        }
        expected_settings = {
            'swift:download': {
                'container_threads': 10,
                'object_threads': 20,
                'shuffle': True,
                'skip_identical': True
            },
            'swift:upload': {
                'changed': False,
                'checksum': True,
                'leave_segments': True,
                'object_threads': 30,
                'segment_size': 1073741824,
                'segment_threads': 10,
                'skip_identical': True,
                'use_slo': True
            }
        }

        self.assertEquals(settings._global_settings, test_settings)
        with settings.use(update_settings):
            self.assertEquals(settings._global_settings, test_settings)
            self.assertEquals(settings.get(), expected_settings)
        self.assertEquals(settings._global_settings, test_settings)
        self.assertEquals(settings.get(), test_settings)

    def test_use_wo_settings(self):
        self.assertEquals(settings._global_settings, test_settings)
        with settings.use():
            self.assertEquals(settings._global_settings, test_settings)
        self.assertEquals(settings._global_settings, test_settings)

    @mock.patch.dict('stor.settings._global_settings', {'foo': ''}, clear=True)
    def test_use_nested_w_update(self):
        settings.update({'foo': 0})
        self.assertEquals(settings.get(), {'foo': 0})
        with settings.use({'foo': 1}):
            self.assertEquals(settings.get(), {'foo': 1})
            self.assertEquals(settings._global_settings, {'foo': 0})
            with settings.use({'foo': 2}):
                self.assertEquals(settings.get(), {'foo': 2})
                self.assertEquals(settings._global_settings, {'foo': 0})
            self.assertEquals(settings.get(), {'foo': 1})
            self.assertEquals(settings._global_settings, {'foo': 0})
        self.assertEquals(settings.get(), {'foo': 0})
        self.assertFalse(hasattr(settings.thread_local, 'settings'))
        settings.update({'foo': 3})
        self.assertEquals(settings.get(), {'foo': 3})

    @mock.patch.dict('stor.settings._global_settings', {'foo': ''}, clear=True)
    def test_use_update_w_error(self):
        with settings.use({'foo': 1}):
            with self.assertRaises(RuntimeError):
                settings.update({'foo': 3})

    def _use_contextmanager(self, value):
        with settings.use({'foo': value}):
            time.sleep(.01)
            self.assertEquals(settings.get(), {'foo': value})
            time.sleep(.01)

    @mock.patch.dict('stor.settings._global_settings', {'foo': ''}, clear=True)
    def test_use_multithreaded(self):
        threads = []
        for i in range(30):
            thread = threading.Thread(target=self._use_contextmanager, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()
