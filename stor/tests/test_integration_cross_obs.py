from stor.tests import integration
from stor.tests.test_integration_swift import set_up_integration_swift
from stor.tests.test_integration_s3 import set_up_integration_s3


class TestSwiftToS3(integration.SourceDestTestCase):
    def setUp(self):
        set_up_integration_swift(self)
        self.src_dir = self.test_dir
        # sets test_dir to s3
        set_up_integration_s3(self)


class TestS3ToSwift(integration.SourceDestTestCase):
    def setUp(self):
        set_up_integration_s3(self)
        self.src_dir = self.test_dir
        # sets test_dir to swift
        set_up_integration_swift(self)
