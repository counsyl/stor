import mock
import os
import storage_utils
from storage_utils import settings
import unittest

test_settings = {
    'swift': {
        'download': {
            'container_threads': 10,
            'object_threads': 10,
            'shuffle': True,
            'skip_identical': True
        },
        'upload': {
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
}


@mock.patch('storage_utils.global_settings', test_settings)
class TestSettings(unittest.TestCase):
    def test_initialize_default(self):
        expected_settings = {
            'swift': {
                'download': {
                    'container_threads': 10,
                    'object_threads': 10,
                    'shuffle': True,
                    'skip_identical': True
                },
                'upload': {
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
        }
        self.assertEquals(settings.initialize(), expected_settings)

    def test_initialize_file(self):
        expected_settings = {
            'str_val': 'this is a string',
            'something': {
                'just': 'another value'
            },
            'swift': {
                'num_retries': 5,
                'fake_secret_key': '7jsdf0983j""SP{}?//',
                'download': {
                    'container_threads': 10,
                    'object_threads': 10,
                    'shuffle': True,
                    'skip_identical': True
                },
                'upload': {
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
        }
        filename = os.path.join(os.path.dirname(__file__), 'file_data', 'test.cfg')
        self.assertEquals(settings.initialize(filename), expected_settings)

    def test_get(self):
        self.assertEquals(settings.get(), test_settings)

    def test_update_w_settings(self):
        update_settings = {
            'swift': {
                'upload': {
                    'skip_identical': True,
                    'object_threads': 30
                },
                'download': {
                    'object_threads': 20
                }
            }
        }
        expected_settings = {
            'swift': {
                'download': {
                    'container_threads': 10,
                    'object_threads': 20,
                    'shuffle': True,
                    'skip_identical': True
                },
                'upload': {
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
        }
        settings.update(settings=update_settings)
        self.assertEquals(storage_utils.global_settings, expected_settings)

    def test_update_wo_settings(self):
        settings.update()
        self.assertEquals(storage_utils.global_settings, test_settings)

    def test_use_w_settings(self):
        update_settings = {
            'swift': {
                'upload': {
                    'skip_identical': True,
                    'object_threads': 30
                },
                'download': {
                    'object_threads': 20
                }
            }
        }
        expected_settings = {
            'swift': {
                'download': {
                    'container_threads': 10,
                    'object_threads': 20,
                    'shuffle': True,
                    'skip_identical': True
                },
                'upload': {
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
        }

        self.assertEquals(storage_utils.global_settings, test_settings)
        with settings.use(update_settings) as my_settings:
            self.assertEquals(storage_utils.global_settings, test_settings)
            self.assertEquals(my_settings, test_settings)
        self.assertEquals(storage_utils.global_settings, expected_settings)

    def test_use_wo_settings(self):
        self.assertEquals(storage_utils.global_settings, test_settings)
        with settings.use() as my_settings:
            self.assertEquals(storage_utils.global_settings, test_settings)
            self.assertEquals(my_settings, test_settings)
        self.assertEquals(storage_utils.global_settings, test_settings)
