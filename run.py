import sys

from PyQt5.Qt import QApplication

from module import KiwoomModule
from utils import start_consume_process


def callback(channel, method, properties, body):
    print(channel)
    print(method)
    print(properties)
    print(body)


if __name__ == "__main__":
    start_consume_process("tasks", callback)

    app = QApplication(sys.argv)

    kiwoom_module = KiwoomModule()
    kiwoom_module.connect()

    app.exec()
