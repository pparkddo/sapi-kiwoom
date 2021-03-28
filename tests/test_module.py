import sys
import unittest

from PyQt5.Qt import QApplication

from sapi_kiwoom.module import KiwoomModule
from sapi_kiwoom.messenger import Messenger


class ModuleTest(unittest.TestCase):

    def setUp(self):
        self.test_broker_url = "amqp://localhost:5672"

    def test_module_initialize(self):
        # QtWidget 을 상속받는 KiwoomModule 보다 QApplication 이 먼저 실행되어야 함
        _ = QApplication(sys.argv)
        module = KiwoomModule(Messenger(self.test_broker_url))
        self.assertIsInstance(module, KiwoomModule)
