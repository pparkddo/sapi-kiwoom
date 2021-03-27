import sys
import argparse
import ctypes

from PyQt5.Qt import QApplication

from .messenger import Messenger
from .module import KiwoomModule


class KiwoomPrivilegeError(Exception):
    pass


def parse_args():
    parser = argparse.ArgumentParser(description="Run Kiwoom Securities API Module")
    parser.add_argument(
        "broker_url",
        help="Type your message queue url (ex: amqp://localhost:5672)"
    )
    parsed_args, unparsed_args = parser.parse_known_args()
    return parsed_args, unparsed_args


def check_run_as_admin():
    return ctypes.windll.shell32.IsUserAnAdmin() == 1


def main():
    if not check_run_as_admin():
        raise KiwoomPrivilegeError("키움 OpenAPI 모듈은 관리자권한에서만 정상실행 됩니다")

    parsed_args, unparsed_args = parse_args()

    # QApplication expects the first argument to be the program name
    qt_args = sys.argv[:1] + unparsed_args
    app = QApplication(qt_args)

    broker_url = parsed_args.broker_url
    kiwoom_module = KiwoomModule(Messenger(broker_url))
    kiwoom_module.connect()

    app.exec()
