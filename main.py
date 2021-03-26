import sys

from PyQt5.Qt import QApplication

from messenger import Messenger
from module import KiwoomModule


if __name__ == "__main__":
    app = QApplication(sys.argv)

    kiwoom_module = KiwoomModule(Messenger())
    kiwoom_module.connect()

    app.exec()
