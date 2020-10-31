import sys

from PyQt5.Qt import QApplication

from module import KiwoomModule


if __name__ == "__main__":
    app = QApplication(sys.argv)

    kiwoom_module = KiwoomModule()
    kiwoom_module.connect()

    app.exec()
